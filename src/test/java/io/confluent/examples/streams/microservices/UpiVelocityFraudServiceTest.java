package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.beans.UpiAlertBean;
import io.confluent.examples.streams.microservices.domain.beans.UpiTransactionBean;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Drives {@link UpiVelocityFraudService}'s topology with {@code TopologyTestDriver}, so the
 * four UPI fraud patterns are asserted on without a broker, a schema registry, or Docker.
 *
 * <p>Each test pipes transactions in at controlled timestamps and reads the alerts that come
 * out of {@code upi-fraud-alerts}. What is being pinned down is not "an alert appeared" but
 * the properties an on-call analyst depends on: an alert fires at the threshold and not
 * before, it fires <em>once</em> per window rather than on every record past the threshold,
 * a window boundary resets the count, and a malformed record cannot take the pipeline down.
 */
public class UpiVelocityFraudServiceTest {

  /** Minute-aligned so a one-minute window starts exactly here. */
  private static final Instant BASE = Instant.parse("2026-01-01T00:00:00Z");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> transactions;
  private TestOutputTopic<String, String> alerts;
  private final List<UpiAlertBean> collected = new ArrayList<>();

  @Before
  public void setup() {
    final Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "upi-fraud-test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    // Mirrors the production setting in UpiVelocityFraudService#start. Note that this is
    // documentation rather than coverage: TopologyTestDriver flushes after every piped
    // record, so it forwards each state-store update whatever this is set to. The reason
    // production needs caching off - a cached store forwards only the last value per key
    // per flush, so counts 1..15 can surface as a single 15 and the exact threshold
    // crossing is never seen - cannot be reproduced here, and only a broker-backed
    // integration test would pin it down.
    config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

    testDriver = new TopologyTestDriver(UpiVelocityFraudService.buildTopology(), config);
    transactions = testDriver.createInputTopic(
        UpiTransactionService.TOPIC_ALL, new StringSerializer(), new StringSerializer());
    alerts = testDriver.createOutputTopic(
        UpiVelocityFraudService.TOPIC_FRAUD_ALERTS,
        new StringDeserializer(), new StringDeserializer());
  }

  @After
  public void tearDown() {
    if (testDriver != null) {
      testDriver.close();
    }
  }

  // ── Pattern 1: velocity ────────────────────────────────────────────────────

  @Test
  public void shouldNotAlertBelowTheVelocityThreshold() {
    sendSuccesses("rahul@oksbi", "Mumbai", UpiVelocityFraudService.VELOCITY_THRESHOLD - 1, 0);

    assertTrue("nine transactions in a minute is ordinary use", alerts.isEmpty());
  }

  @Test
  public void shouldRaiseOneVelocityAlertWhenTheThresholdIsCrossed() {
    sendSuccesses("script@ybl", "Pune", UpiVelocityFraudService.VELOCITY_THRESHOLD, 0);

    final List<UpiAlertBean> raised = alertsOfType("VELOCITY_FRAUD");
    assertEquals(1, raised.size());
    final UpiAlertBean alert = raised.get(0);
    assertEquals("script@ybl", alert.getSenderVpa());
    assertEquals("CRITICAL", alert.getSeverity());
    assertEquals(UpiVelocityFraudService.VELOCITY_THRESHOLD, alert.getTxnCount());
    assertEquals("Yes Bank (PhonePe)", alert.getSenderBank());
  }

  @Test
  public void shouldRaiseTheVelocityAlertOnlyOnceNoMatterHowFarPastTheThreshold() {
    // A fraud script does not stop at exactly ten. Every record past the threshold used to
    // raise another alert, which is what turns an alert queue into an unusable firehose.
    sendSuccesses("script@ybl", "Pune", 40, 0);

    assertEquals(1, alertsOfType("VELOCITY_FRAUD").size());
  }

  @Test
  public void shouldNotCarryVelocityCountsAcrossAWindowBoundary() {
    // Nine in the first minute, nine in the second: eighteen transactions, no fraud.
    sendSuccesses("commuter@okaxis", "Delhi", 9, 0);
    sendSuccesses("commuter@okaxis", "Delhi", 9, 60);

    assertTrue(alertsOfType("VELOCITY_FRAUD").isEmpty());
  }

  @Test
  public void shouldNotCountFailedTransactionsTowardsVelocity() {
    // Ten failures are a failed burst, not a velocity burst - two different investigations.
    for (int i = 0; i < 10; i++) {
      send(txn("prober@okicici", "Kolkata", 120.0, "FAILED"), i);
    }

    assertTrue(alertsOfType("VELOCITY_FRAUD").isEmpty());
    assertEquals(1, alertsOfType("FAILED_BURST").size());
  }

  // ── Pattern 2: failed burst ───────────────────────────────────────────────

  @Test
  public void shouldRaiseFailedBurstWhenPinGuessingIsSuspected() {
    for (int i = 0; i < UpiVelocityFraudService.FAILED_BURST_THRESHOLD; i++) {
      send(txn("victim@okhdfcbank", "Chennai", 500.0, "FAILED"), i * 10);
    }

    final List<UpiAlertBean> raised = alertsOfType("FAILED_BURST");
    assertEquals(1, raised.size());
    assertEquals("HIGH", raised.get(0).getSeverity());
    assertEquals("HDFC", raised.get(0).getSenderBank());
    assertEquals(UpiVelocityFraudService.FAILED_BURST_THRESHOLD, raised.get(0).getTxnCount());
  }

  @Test
  public void shouldNotRaiseFailedBurstBelowTheThreshold() {
    for (int i = 0; i < UpiVelocityFraudService.FAILED_BURST_THRESHOLD - 1; i++) {
      send(txn("clumsy@paytm", "Jaipur", 500.0, "FAILED"), i * 10);
    }

    assertTrue(alerts.isEmpty());
  }

  // ── Pattern 3: high value ─────────────────────────────────────────────────

  @Test
  public void shouldRaiseMediumAlertForAHighValueTransaction() {
    send(txn("buyer@okaxis", "Mumbai", UpiVelocityFraudService.HIGH_VALUE_THRESHOLD, "SUCCESS"), 0);

    final List<UpiAlertBean> raised = alertsOfType("HIGH_VALUE_SPIKE");
    assertEquals(1, raised.size());
    assertEquals("MEDIUM", raised.get(0).getSeverity());
    assertEquals(UpiVelocityFraudService.HIGH_VALUE_THRESHOLD,
        raised.get(0).getTotalAmountInr(), 0.001);
  }

  @Test
  public void shouldEscalateToCriticalAtTheOneLakhReviewThreshold() {
    send(txn("mule@paytm", "Surat", UpiVelocityFraudService.HIGH_VALUE_CRITICAL, "SUCCESS"), 0);

    final List<UpiAlertBean> raised = alertsOfType("HIGH_VALUE_SPIKE");
    assertEquals(1, raised.size());
    assertEquals("CRITICAL", raised.get(0).getSeverity());
    assertEquals("Paytm Payments Bank", raised.get(0).getSenderBank());
  }

  @Test
  public void shouldNotRaiseHighValueForATransactionThatNeverSucceeded() {
    send(txn("buyer@okaxis", "Mumbai", 250_000.0, "FAILED"), 0);

    assertTrue(alertsOfType("HIGH_VALUE_SPIKE").isEmpty());
  }

  @Test
  public void shouldNotRaiseHighValueJustBelowTheThreshold() {
    send(txn("buyer@okaxis", "Mumbai", UpiVelocityFraudService.HIGH_VALUE_THRESHOLD - 1, "SUCCESS"), 0);

    assertTrue(alerts.isEmpty());
  }

  // ── Pattern 4: geo-velocity ───────────────────────────────────────────────

  @Test
  public void shouldRaiseMultiCityWhenOneVpaTransactsFromTwoCities() {
    send(txn("traveller@oksbi", "Mumbai", 300.0, "SUCCESS"), 0);
    send(txn("traveller@oksbi", "Delhi", 300.0, "SUCCESS"), 60);

    final List<UpiAlertBean> raised = alertsOfType("MULTI_CITY");
    assertEquals(1, raised.size());
    assertEquals("traveller@oksbi", raised.get(0).getSenderVpa());
    assertTrue("the alert should name both cities it correlated",
        raised.get(0).getDescription().contains("Mumbai")
            && raised.get(0).getDescription().contains("Delhi"));
  }

  @Test
  public void shouldNotRaiseMultiCityForRepeatedActivityInOneCity() {
    // The previous implementation counted transactions per (vpa, city) and would fire here.
    for (int i = 0; i < 6; i++) {
      send(txn("local@oksbi", "Mumbai", 300.0, "SUCCESS"), i * 10);
    }

    assertTrue(alertsOfType("MULTI_CITY").isEmpty());
  }

  @Test
  public void shouldRaiseMultiCityOnlyOnceWhileTheSecondCityKeepsTransacting() {
    send(txn("traveller@oksbi", "Mumbai", 300.0, "SUCCESS"), 0);
    send(txn("traveller@oksbi", "Delhi", 300.0, "SUCCESS"), 30);
    send(txn("traveller@oksbi", "Delhi", 300.0, "SUCCESS"), 60);
    send(txn("traveller@oksbi", "Mumbai", 300.0, "SUCCESS"), 90);

    assertEquals(1, alertsOfType("MULTI_CITY").size());
  }

  @Test
  public void shouldNotCorrelateCitiesAcrossDifferentVpas() {
    send(txn("one@oksbi", "Mumbai", 300.0, "SUCCESS"), 0);
    send(txn("two@oksbi", "Delhi", 300.0, "SUCCESS"), 30);

    assertTrue(alertsOfType("MULTI_CITY").isEmpty());
  }

  // ── Robustness ────────────────────────────────────────────────────────────

  @Test
  public void shouldSurviveMalformedRecordsAndKeepDetecting() {
    transactions.pipeInput("k", "this is not json", BASE);
    transactions.pipeInput("k", "{\"txnId\":\"partial\"", BASE);
    // A well-formed record with no sender VPA cannot be attributed to anyone.
    transactions.pipeInput("k", "{\"txnId\":\"t\",\"amountInr\":90000,\"status\":\"SUCCESS\"}", BASE);

    assertTrue(alerts.isEmpty());

    send(txn("buyer@okicici", "Mumbai", 90_000.0, "SUCCESS"), 10);
    assertEquals("the pipeline must still be alive after bad input",
        1, alertsOfType("HIGH_VALUE_SPIKE").size());
  }

  @Test
  public void shouldKeyEveryAlertBySenderVpaSoConsumersCanPartitionByAccount() {
    send(txn("mule@paytm", "Surat", 250_000.0, "SUCCESS"), 0);

    final KeyValue<String, String> record = alerts.readKeyValue();
    assertEquals("mule@paytm", record.key);
    assertNotNull(record.value);
  }

  // ── Pure helpers ──────────────────────────────────────────────────────────

  @Test
  public void shouldResolveTheBankBehindAUpiHandle() {
    assertEquals("SBI", UpiVelocityFraudService.bankOf("someone@oksbi"));
    assertEquals("ICICI", UpiVelocityFraudService.bankOf("someone@okicici"));
    assertEquals("UNKNOWN", UpiVelocityFraudService.bankOf(null));
    assertEquals("Unknown Bank", UpiVelocityFraudService.bankOf("someone@newpsp"));
  }

  @Test
  public void shouldTrackDistinctCitiesAndFlagOnlyTheAdditions() {
    final String first = UpiVelocityFraudService.addCity("", "Mumbai");
    assertTrue(UpiVelocityFraudService.cityAdded(first));
    assertEquals(1, UpiVelocityFraudService.cityCount(first));

    final String second = UpiVelocityFraudService.addCity(first, "Delhi");
    assertTrue(UpiVelocityFraudService.cityAdded(second));
    assertEquals(2, UpiVelocityFraudService.cityCount(second));

    final String repeat = UpiVelocityFraudService.addCity(second, "Delhi");
    assertFalse("a repeat is not a new city", UpiVelocityFraudService.cityAdded(repeat));
    assertEquals(2, UpiVelocityFraudService.cityCount(repeat));
    assertEquals("Mumbai", UpiVelocityFraudService.cities(repeat).get(0));
  }

  // ── Fixtures ──────────────────────────────────────────────────────────────

  private void sendSuccesses(final String vpa, final String city, final int count,
                             final int startSecond) {
    for (int i = 0; i < count; i++) {
      send(txn(vpa, city, 250.0, "SUCCESS"), startSecond + i);
    }
  }

  private void send(final UpiTransactionBean txn, final int atSecond) {
    try {
      transactions.pipeInput(txn.getSenderVpa(), MAPPER.writeValueAsString(txn),
          BASE.plus(Duration.ofSeconds(atSecond)));
    } catch (final Exception e) {
      throw new IllegalStateException("could not serialize the test transaction", e);
    }
  }

  private static UpiTransactionBean txn(final String senderVpa, final String city,
                                        final double amount, final String status) {
    return new UpiTransactionBean(
        "TXN" + System.nanoTime(),
        senderVpa,
        "merchant@okaxis",
        UpiVelocityFraudService.bankOf(senderVpa),
        "Axis",
        amount,
        status,
        "Shopping",
        "Swiggy",
        "MERCH001",
        city,
        "Maharashtra",
        "ANDROID",
        "PHONEPE",
        BASE.toEpochMilli(),
        "FAILED".equals(status) ? "INSUFFICIENT_BALANCE" : null);
  }

  /**
   * Every alert seen so far. Reading a {@code TestOutputTopic} drains it, so records are
   * accumulated here - otherwise a test that asks about two alert types would find the
   * second query empty and pass for the wrong reason.
   */
  private List<UpiAlertBean> allAlerts() {
    for (final KeyValue<String, String> record : alerts.readKeyValuesToList()) {
      try {
        collected.add(MAPPER.readValue(record.value, UpiAlertBean.class));
      } catch (final Exception e) {
        throw new IllegalStateException("alert topic carried unreadable JSON: " + record.value, e);
      }
    }
    return collected;
  }

  private List<UpiAlertBean> alertsOfType(final String alertType) {
    final List<UpiAlertBean> matching = new ArrayList<>();
    for (final UpiAlertBean alert : allAlerts()) {
      if (alertType.equals(alert.getAlertType())) {
        matching.add(alert);
      }
    }
    return matching;
  }
}
