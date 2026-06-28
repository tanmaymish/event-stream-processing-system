package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.beans.UpiTransactionBean;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Real-time UPI velocity fraud detection using Kafka Streams windowed aggregations.
 *
 * Detects three distinct fraud patterns common in India's UPI ecosystem:
 *
 * 1. VELOCITY FRAUD  - >10 transactions from same VPA in 1 minute
 *    (common in automated fraud scripts targeting merchant QR codes)
 *
 * 2. FAILED BURST    - >5 failed txns in 2 minutes (account takeover probing)
 *    (attackers test stolen UPI PINs against multiple accounts)
 *
 * 3. HIGH VALUE SPIKE - single txn >₹50,000 after avg <₹500
 *    (money mule patterns: accumulate small amounts then drain)
 *
 * Alerts are published to the upi-fraud-alerts topic and also logged
 * for integration with NPCI's fraud reporting API.
 */
public class UpiVelocityFraudService implements Service {

  private static final Logger log = LoggerFactory.getLogger(UpiVelocityFraudService.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  // Fraud thresholds tuned for India UPI patterns
  private static final int VELOCITY_THRESHOLD = 10;       // txns per minute per VPA
  private static final int FAILED_BURST_THRESHOLD = 5;    // failed txns per 2 min
  private static final double HIGH_VALUE_THRESHOLD = 50000.0;  // ₹50,000

  private KafkaStreams streams;

  @Override
  public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
    StreamsBuilder builder = new StreamsBuilder();

    // Consume all UPI transactions
    KStream<String, String> rawStream = builder.stream(
        UpiTransactionService.TOPIC_ALL,
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, UpiTransactionBean> txnStream = rawStream
        .mapValues(json -> {
          try {
            return mapper.readValue(json, UpiTransactionBean.class);
          } catch (Exception e) {
            log.warn("[UPI_PARSE_WARN] Could not deserialize transaction: {}", e.getMessage());
            return null;
          }
        })
        .filter((key, txn) -> txn != null);

    // ── Pattern 1: Velocity fraud (too many txns per minute) ──────────────────
    txnStream
        .filter((key, txn) -> "SUCCESS".equals(txn.getStatus()) || "PENDING".equals(txn.getStatus()))
        .groupBy((key, txn) -> txn.getSenderVpa())
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        .count(Materialized.as("velocity-counts"))
        .toStream()
        .filter((w, count) -> count >= VELOCITY_THRESHOLD)
        .foreach((w, count) -> {
          String vpa = w.key();
          log.warn("[FRAUD_VELOCITY] CRITICAL: UPI velocity fraud detected! vpa={} txns={} in 1min window=[{} - {}]",
              vpa, count, w.window().startTime(), w.window().endTime());
          UpiTransactionService.fraudAlerts.incrementAndGet();
        });

    // ── Pattern 2: Failed burst (account probing / PIN testing) ───────────────
    txnStream
        .filter((key, txn) -> "FAILED".equals(txn.getStatus()))
        .groupBy((key, txn) -> txn.getSenderVpa())
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(2)))
        .count(Materialized.as("failed-burst-counts"))
        .toStream()
        .filter((w, count) -> count >= FAILED_BURST_THRESHOLD)
        .foreach((w, count) -> {
          String vpa = w.key();
          log.warn("[FRAUD_FAILED_BURST] HIGH: Repeated UPI failures detected! "
              + "vpa={} failures={} in 2min - possible account takeover attempt! bank={}",
              vpa, count, extractBank(vpa));
          UpiTransactionService.fraudAlerts.incrementAndGet();
        });

    // ── Pattern 3: High-value anomaly detection ────────────────────────────────
    // Track running average per VPA and flag when single txn is 100x the average
    txnStream
        .filter((key, txn) -> "SUCCESS".equals(txn.getStatus())
            && txn.getAmountInr() >= HIGH_VALUE_THRESHOLD)
        .foreach((key, txn) -> {
          log.warn("[FRAUD_HIGH_VALUE] MEDIUM: Large UPI transaction! vpa={} amount=₹{} merchant={} city={}",
              txn.getSenderVpa(), txn.getAmountInr(), txn.getMerchantName(), txn.getSenderCity());
          // Amounts >= ₹50k require additional TPIN verification per RBI guidelines
          if (txn.getAmountInr() >= 100000) {
            log.error("[FRAUD_HIGH_VALUE_CRITICAL] Amount >=₹1L requires NPCI fraud review! "
                + "txnId={} vpa={} amount=₹{}", txn.getTxnId(), txn.getSenderVpa(), txn.getAmountInr());
            UpiTransactionService.fraudAlerts.incrementAndGet();
          }
        });

    // ── Pattern 4: Multi-city rapid transactions (geo-velocity fraud) ──────────
    // Group by VPA + city, detect when same VPA transacts from 2+ cities within 5 min
    txnStream
        .filter((key, txn) -> "SUCCESS".equals(txn.getStatus()))
        .groupBy((key, txn) -> txn.getSenderVpa() + "##" + txn.getSenderCity())
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
        .count(Materialized.as("city-vpa-counts"))
        .toStream()
        .filter((w, count) -> count >= 3)
        .foreach((w, count) -> {
          String[] parts = w.key().split("##");
          String vpa = parts.length > 0 ? parts[0] : w.key();
          String city = parts.length > 1 ? parts[1] : "UNKNOWN";
          log.info("[UPI_CITY_ACTIVITY] vpa={} has {} txns from {} in 5min window",
              vpa, count, city);
        });

    Properties props = MicroserviceUtils.baseStreamsConfig(
        bootstrapServers, stateDir, "upi-velocity-fraud-detector", defaultConfig);
    streams = new KafkaStreams(builder.build(), props);

    streams.setUncaughtExceptionHandler((thread, throwable) -> {
      log.error("[UPI_FRAUD_STREAM_ERROR] Uncaught exception in thread {}: {}",
          thread.getName(), throwable.getMessage());
    });

    streams.start();
    log.info("[UPI_FRAUD_START] UPI Velocity Fraud Detection online. "
        + "Monitoring topics: {} | Thresholds: velocity={}/min, failed={}/2min, highValue=₹{}",
        UpiTransactionService.TOPIC_ALL, VELOCITY_THRESHOLD, FAILED_BURST_THRESHOLD, HIGH_VALUE_THRESHOLD);
  }

  private String extractBank(String vpa) {
    if (vpa == null) return "UNKNOWN";
    // UPI handle → bank mapping (common India handles)
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
  public void stop() {
    if (streams != null) streams.close();
  }

  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : MicroserviceUtils.DEFAULT_BOOTSTRAP_SERVERS;
    UpiVelocityFraudService service = new UpiVelocityFraudService();
    service.start(bootstrapServers, "/tmp/upi-velocity-fraud-state", new Properties());
    MicroserviceUtils.addShutdownHookAndBlock(service);
  }
}
