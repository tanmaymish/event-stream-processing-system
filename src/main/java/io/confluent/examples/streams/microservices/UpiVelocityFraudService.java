package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.beans.UpiAlertBean;
import io.confluent.examples.streams.microservices.domain.beans.UpiTransactionBean;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Real-time UPI velocity fraud detection using Kafka Streams windowed aggregations.
 *
 * <p>Detects four fraud patterns common in India's UPI ecosystem. Every pattern emits a
 * {@link UpiAlertBean} as JSON to {@link #TOPIC_FRAUD_ALERTS}, keyed by sender VPA, so that
 * downstream consumers (a case-management queue, NPCI reporting, a dashboard) can act on
 * alerts rather than having to scrape log lines:
 *
 * <ol>
 *   <li>VELOCITY_FRAUD &mdash; at least 10 transactions from one VPA in a 1-minute window.
 *       Typical of automated scripts hitting merchant QR codes.</li>
 *   <li>FAILED_BURST &mdash; at least 5 failed transactions from one VPA in a 2-minute
 *       window. Typical of UPI PIN guessing after an account takeover.</li>
 *   <li>HIGH_VALUE_SPIKE &mdash; a single successful transaction at or above Rs 50,000.
 *       Escalated to CRITICAL at Rs 1,00,000, where RBI guidance expects additional
 *       verification.</li>
 *   <li>MULTI_CITY &mdash; one VPA transacting from two or more distinct cities inside a
 *       5-minute window, which no single human traveller can do. This is a genuine
 *       cross-city correlation, not a per-city transaction count.</li>
 * </ol>
 *
 * <p>Window alerts fire on the <em>first</em> crossing of a threshold (count == threshold)
 * rather than on every record beyond it. A VPA sending 50 transactions a minute therefore
 * raises one alert for that window instead of 41, which is what makes the alert topic
 * usable as a work queue. That exactness depends on record caching being off (see
 * {@link #start}), so every state-store update reaches the filter.
 *
 * <p>The topology is built by the static {@link #buildTopology()} so it can be driven by
 * {@code TopologyTestDriver} without a running broker &mdash; see
 * {@code UpiVelocityFraudServiceTest}.
 */
public class UpiVelocityFraudService implements Service {

  private static final Logger log = LoggerFactory.getLogger(UpiVelocityFraudService.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  public static final String TOPIC_FRAUD_ALERTS = "upi-fraud-alerts";

  // Fraud thresholds tuned for India UPI patterns
  static final int VELOCITY_THRESHOLD = 10;             // txns per minute per VPA
  static final int FAILED_BURST_THRESHOLD = 5;          // failed txns per 2 min per VPA
  static final double HIGH_VALUE_THRESHOLD = 50_000.0;  // Rs 50,000
  static final double HIGH_VALUE_CRITICAL = 100_000.0;  // Rs 1,00,000
  static final int MULTI_CITY_THRESHOLD = 2;            // distinct cities per 5 min per VPA

  static final Duration VELOCITY_WINDOW = Duration.ofMinutes(1);
  static final Duration FAILED_BURST_WINDOW = Duration.ofMinutes(2);
  static final Duration MULTI_CITY_WINDOW = Duration.ofMinutes(5);

  // City names never contain a pipe, so it is safe as the accumulator separator.
  private static final String CITY_SEPARATOR = "|";
  private static final String CITY_SPLIT_PATTERN = "\\|";

  private KafkaStreams streams;

  /**
   * Builds the fraud-detection topology.
   *
   * <p>Every repartition in here carries explicit String serdes. The transaction bean is
   * projected down to the String fields a pattern actually needs <em>before</em> any
   * grouping, so no repartition has to serialize the bean itself with the default serde.
   */
  public static Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, UpiTransactionBean> txnStream = builder
        .stream(UpiTransactionService.TOPIC_ALL, Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues(UpiVelocityFraudService::parseOrNull)
        .filter((key, txn) -> txn != null && txn.getSenderVpa() != null);

    velocityAlerts(txnStream)
        .merge(failedBurstAlerts(txnStream))
        .merge(highValueAlerts(txnStream))
        .merge(multiCityAlerts(txnStream))
        .peek((vpa, alertJson) -> log.warn("[UPI_FRAUD_ALERT] vpa={} alert={}", vpa, alertJson))
        .to(TOPIC_FRAUD_ALERTS, Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

  /** Pattern 1: too many transactions from one VPA inside a minute. */
  private static KStream<String, String> velocityAlerts(
      final KStream<String, UpiTransactionBean> txnStream) {
    return txnStream
        .filter((key, txn) -> "SUCCESS".equals(txn.getStatus()) || "PENDING".equals(txn.getStatus()))
        // Project to (vpa -> txnId) so the repartition carries Strings, not the bean.
        .map((key, txn) -> KeyValue.pair(txn.getSenderVpa(), txn.getTxnId()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(VELOCITY_WINDOW))
        .count(Materialized.as("velocity-counts"))
        .toStream()
        .filter((window, count) -> count != null && count == VELOCITY_THRESHOLD)
        .map((window, count) -> alert(
            window,
            "VELOCITY_FRAUD",
            "CRITICAL",
            count + " transactions in " + VELOCITY_WINDOW.toMinutes()
                + " min from a single VPA - automated transaction script suspected",
            count,
            0.0));
  }

  /** Pattern 2: repeated failures from one VPA - PIN guessing after an account takeover. */
  private static KStream<String, String> failedBurstAlerts(
      final KStream<String, UpiTransactionBean> txnStream) {
    return txnStream
        .filter((key, txn) -> "FAILED".equals(txn.getStatus()))
        .map((key, txn) -> KeyValue.pair(txn.getSenderVpa(), txn.getTxnId()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(FAILED_BURST_WINDOW))
        .count(Materialized.as("failed-burst-counts"))
        .toStream()
        .filter((window, count) -> count != null && count == FAILED_BURST_THRESHOLD)
        .map((window, count) -> alert(
            window,
            "FAILED_BURST",
            "HIGH",
            count + " failed transactions in " + FAILED_BURST_WINDOW.toMinutes()
                + " min - possible account takeover or UPI PIN guessing",
            count,
            0.0));
  }

  /** Pattern 3: a single large transaction. Stateless, so no window applies. */
  private static KStream<String, String> highValueAlerts(
      final KStream<String, UpiTransactionBean> txnStream) {
    return txnStream
        .filter((key, txn) -> "SUCCESS".equals(txn.getStatus())
            && txn.getAmountInr() >= HIGH_VALUE_THRESHOLD)
        .map((key, txn) -> {
          final boolean critical = txn.getAmountInr() >= HIGH_VALUE_CRITICAL;
          final UpiAlertBean bean = new UpiAlertBean(
              UUID.randomUUID().toString(),
              "HIGH_VALUE_SPIKE",
              critical ? "CRITICAL" : "MEDIUM",
              txn.getSenderVpa(),
              bankOf(txn.getSenderVpa()),
              critical
                  ? "Transaction of Rs " + txn.getAmountInr()
                      + " is at or above the Rs 1,00,000 review threshold"
                  : "Transaction of Rs " + txn.getAmountInr() + " is at or above Rs 50,000",
              txn.getTimestamp(),
              txn.getTimestamp(),
              1L,
              txn.getAmountInr(),
              System.currentTimeMillis());
          return KeyValue.pair(txn.getSenderVpa(), toJson(bean));
        })
        .filter((vpa, alertJson) -> alertJson != null);
  }

  /**
   * Pattern 4: one VPA transacting from two or more distinct cities inside five minutes.
   *
   * <p>The previous implementation grouped by {@code vpa + city} and counted, which could
   * only ever report repeated activity in a <em>single</em> city - it never compared cities
   * against each other. This aggregates the distinct city set per VPA and alerts the moment
   * a second city joins it.
   */
  private static KStream<String, String> multiCityAlerts(
      final KStream<String, UpiTransactionBean> txnStream) {
    return txnStream
        .filter((key, txn) -> "SUCCESS".equals(txn.getStatus())
            && txn.getSenderCity() != null && !txn.getSenderCity().isEmpty())
        .map((key, txn) -> KeyValue.pair(txn.getSenderVpa(), txn.getSenderCity()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(MULTI_CITY_WINDOW))
        .aggregate(
            () -> "",
            (vpa, city, acc) -> addCity(acc, city),
            Materialized.with(Serdes.String(), Serdes.String()))
        .toStream()
        // Fire only on the record that actually grew the set to the threshold. Without the
        // "new city" flag every later transaction would re-raise the same alert, because the
        // aggregate keeps reporting a two-city set for the rest of the window.
        .filter((window, acc) -> cityAdded(acc) && cityCount(acc) == MULTI_CITY_THRESHOLD)
        .map((window, acc) -> alert(
            window,
            "MULTI_CITY",
            "HIGH",
            "Same VPA transacted from " + cityCount(acc) + " cities ("
                + String.join(", ", cities(acc)) + ") within "
                + MULTI_CITY_WINDOW.toMinutes() + " min - geo-velocity impossible for one person",
            cityCount(acc),
            0.0));
  }

  private static KeyValue<String, String> alert(final Windowed<String> window,
                                                final String alertType,
                                                final String severity,
                                                final String description,
                                                final long txnCount,
                                                final double totalAmountInr) {
    final String vpa = window.key();
    final UpiAlertBean bean = new UpiAlertBean(
        UUID.randomUUID().toString(),
        alertType,
        severity,
        vpa,
        bankOf(vpa),
        description,
        window.window().start(),
        window.window().end(),
        txnCount,
        totalAmountInr,
        System.currentTimeMillis());
    return KeyValue.pair(vpa, toJson(bean));
  }

  private static UpiTransactionBean parseOrNull(final String json) {
    try {
      return mapper.readValue(json, UpiTransactionBean.class);
    } catch (final Exception e) {
      // A malformed record must not take the stream thread down with it.
      log.warn("[UPI_PARSE_WARN] Could not deserialize transaction: {}", e.getMessage());
      return null;
    }
  }

  private static String toJson(final UpiAlertBean bean) {
    try {
      return mapper.writeValueAsString(bean);
    } catch (final Exception e) {
      log.error("[UPI_ALERT_SERIALIZE_ERROR] Could not serialize alert {}: {}",
          bean, e.getMessage());
      return null;
    }
  }

  /**
   * Accumulator for the multi-city window, encoded as {@code <flag>|<city>|<city>...}.
   *
   * <p>The leading flag is {@code N} when this record introduced a city the window had not
   * seen and {@code D} when it repeated one. Kafka Streams aggregators only receive the new
   * value, so without carrying that flag forward there is no way downstream to tell a set
   * that just grew from one that merely got another transaction in a city already counted.
   */
  static String addCity(final String acc, final String city) {
    final Set<String> seen = new LinkedHashSet<>(cities(acc));
    final boolean added = seen.add(city);
    return (added ? "N" : "D") + CITY_SEPARATOR + String.join(CITY_SEPARATOR, seen);
  }

  /** True when the record producing this accumulator introduced a previously unseen city. */
  static boolean cityAdded(final String acc) {
    return acc != null && acc.startsWith("N" + CITY_SEPARATOR);
  }

  static int cityCount(final String acc) {
    return cities(acc).size();
  }

  /** The distinct cities in an accumulator, in first-seen order, without the leading flag. */
  static List<String> cities(final String acc) {
    if (acc == null || acc.isEmpty()) {
      return Collections.emptyList();
    }
    final List<String> parts = Arrays.asList(acc.split(CITY_SPLIT_PATTERN));
    return parts.size() <= 1 ? Collections.emptyList() : parts.subList(1, parts.size());
  }

  /** Maps a UPI handle to the bank behind it. Handles are assigned by NPCI per PSP. */
  static String bankOf(final String vpa) {
    if (vpa == null) {
      return "UNKNOWN";
    }
    if (vpa.endsWith("@oksbi")) return "SBI";
    if (vpa.endsWith("@okhdfcbank")) return "HDFC";
    if (vpa.endsWith("@okicici")) return "ICICI";
    if (vpa.endsWith("@okaxis")) return "Axis";
    if (vpa.endsWith("@ybl")) return "Yes Bank (PhonePe)";
    if (vpa.endsWith("@paytm")) return "Paytm Payments Bank";
    if (vpa.endsWith("@apl")) return "Amazon Pay (Axis)";
    if (vpa.endsWith("@ibl")) return "IDFC First Bank";
    if (vpa.endsWith("@kotak")) return "Kotak Mahindra Bank";
    if (vpa.endsWith("@pnb")) return "Punjab National Bank";
    return "Unknown Bank";
  }

  @Override
  public void start(final String bootstrapServers, final String stateDir,
                    final Properties defaultConfig) {
    final Properties props = MicroserviceUtils.baseStreamsConfig(
        bootstrapServers, stateDir, "upi-velocity-fraud-detector", defaultConfig);
    // Record caching batches state-store updates and forwards only the last value per key
    // per flush. A threshold crossing would then be invisible downstream: counts 1..15 can
    // arrive as a single 15. Fraud alerting wants every update anyway, so turn it off.
    props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    streams = new KafkaStreams(buildTopology(), props);

    // The deprecated handler only logged: a failed stream thread died and detection went
    // quiet with the process still up and apparently healthy. Shut the client down instead,
    // so an orchestrator restarts it and the outage is visible.
    streams.setUncaughtExceptionHandler(throwable -> {
      log.error("[UPI_FRAUD_STREAM_ERROR] Shutting down after uncaught exception", throwable);
      return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    });

    streams.start();
    log.info("[UPI_FRAUD_START] UPI fraud detection online. in={} out={} | "
            + "velocity={}/{}min, failedBurst={}/{}min, highValue=Rs {}, multiCity={}/{}min",
        UpiTransactionService.TOPIC_ALL, TOPIC_FRAUD_ALERTS,
        VELOCITY_THRESHOLD, VELOCITY_WINDOW.toMinutes(),
        FAILED_BURST_THRESHOLD, FAILED_BURST_WINDOW.toMinutes(),
        HIGH_VALUE_THRESHOLD,
        MULTI_CITY_THRESHOLD, MULTI_CITY_WINDOW.toMinutes());
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers =
        args.length > 0 ? args[0] : MicroserviceUtils.DEFAULT_BOOTSTRAP_SERVERS;
    final UpiVelocityFraudService service = new UpiVelocityFraudService();
    service.start(bootstrapServers, "/tmp/upi-velocity-fraud-state", new Properties());
    MicroserviceUtils.addShutdownHookAndBlock(service);
  }
}
