package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.beans.UpiTransactionBean;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.startJetty;

/**
 * State-wise and city-wise UPI transaction analytics for India.
 *
 * Tracks 5-minute windowed aggregates across all 28 states + UTs
 * to surface regional payment trends — critical for NPCI load
 * balancing, Aadhaar-based payment routing, and RBI reporting.
 *
 * Key insights exposed:
 *  - Which states drive UPI volume (Maharashtra, Karnataka, Delhi lead)
 *  - UPI adoption in Tier-2/3 cities (Jaipur, Lucknow, Indore, etc.)
 *  - Failure rates by state (proxy for bank downtime / connectivity)
 *  - Average transaction value by state (urban vs rural spending)
 *
 * REST API (port 8093):
 *   GET /regional/state/{state}     -> 5-min stats for a state
 *   GET /regional/city/{city}       -> 5-min stats for a city
 *   GET /regional/top/states        -> top 10 states by volume
 *   GET /regional/failure-hotspots  -> states with high failure rates
 */
@Path("regional")
public class RegionalAnalyticsService implements Service {

  private static final Logger log = LoggerFactory.getLogger(RegionalAnalyticsService.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  // In-memory rolling aggregates updated by stream processing
  private static final Map<String, StateStats> stateStatsMap = new ConcurrentHashMap<>();
  private static final Map<String, CityStats> cityStatsMap = new ConcurrentHashMap<>();

  private KafkaStreams streams;
  private Server jettyServer;
  private final int port;

  public RegionalAnalyticsService(int port) {
    this.port = port;
  }

  public static class StateStats {
    public String state;
    public long successCount;
    public long failedCount;
    public double totalAmountInr;
    public double avgAmountInr;
    public long windowUpdatedMs;

    public StateStats(String state) {
      this.state = state;
      this.windowUpdatedMs = System.currentTimeMillis();
    }

    public void update(UpiTransactionBean txn) {
      if ("SUCCESS".equals(txn.getStatus())) {
        successCount++;
        totalAmountInr += txn.getAmountInr();
        avgAmountInr = totalAmountInr / successCount;
      } else {
        failedCount++;
      }
      windowUpdatedMs = System.currentTimeMillis();
    }

    public double failureRate() {
      long total = successCount + failedCount;
      return total > 0 ? (failedCount * 100.0 / total) : 0;
    }
  }

  public static class CityStats {
    public String city;
    public String state;
    public long successCount;
    public long failedCount;
    public double totalAmountInr;
    public long windowUpdatedMs;

    public CityStats(String city, String state) {
      this.city = city;
      this.state = state;
      this.windowUpdatedMs = System.currentTimeMillis();
    }

    public void update(UpiTransactionBean txn) {
      if ("SUCCESS".equals(txn.getStatus())) {
        successCount++;
        totalAmountInr += txn.getAmountInr();
      } else {
        failedCount++;
      }
      windowUpdatedMs = System.currentTimeMillis();
    }
  }

  @Override
  public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> rawStream = builder.stream(
        UpiTransactionService.TOPIC_ALL,
        Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, UpiTransactionBean> txnStream = rawStream
        .mapValues(json -> {
          try { return mapper.readValue(json, UpiTransactionBean.class); }
          catch (Exception e) { return null; }
        })
        .filter((k, v) -> v != null && v.getSenderState() != null);

    // ── State-level windowed volume (5-min tumbling) ──────────────────────────
    txnStream
        .groupBy((k, txn) -> txn.getSenderState())
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
        .count(Materialized.as("state-txn-counts"))
        .toStream()
        .foreach((w, count) -> {
          String state = w.key();
          log.info("[REGIONAL_STATE] state={} txn_count={} in 5min window=[{} - {}]",
              state, count, w.window().startTime(), w.window().endTime());
        });

    // ── In-memory state aggregation (for REST queries) ────────────────────────
    txnStream.foreach((k, txn) -> {
      String state = txn.getSenderState();
      stateStatsMap.computeIfAbsent(state, StateStats::new).update(txn);

      String city = txn.getSenderCity();
      if (city != null) {
        cityStatsMap.computeIfAbsent(city, c -> new CityStats(c, state)).update(txn);
      }
    });

    // ── Failure hotspot detection ─────────────────────────────────────────────
    txnStream
        .filter((k, txn) -> "FAILED".equals(txn.getStatus()))
        .groupBy((k, txn) -> txn.getSenderState())
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
        .count(Materialized.as("state-failure-counts"))
        .toStream()
        .filter((w, count) -> count >= 100) // >100 failures/5min = potential issue
        .foreach((w, count) -> {
          log.warn("[REGIONAL_FAILURE_HOTSPOT] state={} failures={} in 5min - "
              + "Possible bank downtime or connectivity issue in this region!",
              w.key(), count);
        });

    // ── Top city real-time leaderboard ───────────────────────────────────────
    txnStream
        .filter((k, txn) -> "SUCCESS".equals(txn.getStatus()))
        .groupBy((k, txn) -> txn.getSenderCity() != null ? txn.getSenderCity() : "Unknown")
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
        .count(Materialized.as("city-txn-counts"))
        .toStream()
        .filter((w, count) -> count % 1000 == 0) // log every 1000 txns per city
        .foreach((w, count) -> {
          log.info("[REGIONAL_CITY_MILESTONE] city={} crossed {} UPI transactions in current 5min window",
              w.key(), count);
        });

    Properties props = MicroserviceUtils.baseStreamsConfig(
        bootstrapServers, stateDir, "regional-analytics", defaultConfig);
    streams = new KafkaStreams(builder.build(), props);
    streams.start();

    jettyServer = startJetty(port, this);
    log.info("[REGIONAL_START] Regional Analytics Service online at port={}. "
        + "Monitoring UPI transactions across all Indian states.", port);
  }

  @GET
  @Path("/state/{state}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStateStats(@PathParam("state") String state) {
    StateStats stats = stateStatsMap.get(state);
    if (stats == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("{\"error\": \"No data for state: " + state + "\"}").build();
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("state", stats.state);
    result.put("successTransactions", stats.successCount);
    result.put("failedTransactions", stats.failedCount);
    result.put("totalVolumeInr", Math.round(stats.totalAmountInr));
    result.put("avgTransactionInr", Math.round(stats.avgAmountInr));
    result.put("failureRatePct", Math.round(stats.failureRate() * 100.0) / 100.0);
    result.put("lastUpdatedMs", stats.windowUpdatedMs);
    return Response.ok(result).build();
  }

  @GET
  @Path("/city/{city}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCityStats(@PathParam("city") String city) {
    CityStats stats = cityStatsMap.get(city);
    if (stats == null) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity("{\"error\": \"No data for city: " + city + "\"}").build();
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("city", stats.city);
    result.put("state", stats.state);
    result.put("successTransactions", stats.successCount);
    result.put("failedTransactions", stats.failedCount);
    result.put("totalVolumeInr", Math.round(stats.totalAmountInr));
    result.put("lastUpdatedMs", stats.windowUpdatedMs);
    return Response.ok(result).build();
  }

  @GET
  @Path("/top/states")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTopStates(@QueryParam("limit") @DefaultValue("10") int limit) {
    List<Map<String, Object>> ranked = new ArrayList<>();
    stateStatsMap.values().stream()
        .sorted((a, b) -> Long.compare(b.successCount, a.successCount))
        .limit(limit)
        .forEach(stats -> {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("state", stats.state);
          row.put("successTransactions", stats.successCount);
          row.put("totalVolumeInr", Math.round(stats.totalAmountInr));
          row.put("avgTransactionInr", Math.round(stats.avgAmountInr));
          ranked.add(row);
        });
    return Response.ok(ranked).build();
  }

  @GET
  @Path("/failure-hotspots")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getFailureHotspots() {
    List<Map<String, Object>> hotspots = new ArrayList<>();
    stateStatsMap.values().stream()
        .filter(s -> s.failureRate() > 5.0) // >5% failure rate
        .sorted((a, b) -> Double.compare(b.failureRate(), a.failureRate()))
        .forEach(stats -> {
          Map<String, Object> row = new LinkedHashMap<>();
          row.put("state", stats.state);
          row.put("failureRatePct", Math.round(stats.failureRate() * 100.0) / 100.0);
          row.put("failedTransactions", stats.failedCount);
          row.put("totalTransactions", stats.successCount + stats.failedCount);
          hotspots.add(row);
        });
    log.warn("[REGIONAL_HOTSPOTS_QUERY] {} states with >5% UPI failure rate", hotspots.size());
    return Response.ok(hotspots).build();
  }

  @Override
  public void stop() {
    if (streams != null) streams.close();
    if (jettyServer != null) {
      try { jettyServer.stop(); } catch (Exception e) { log.error("Jetty stop", e); }
    }
  }

  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : MicroserviceUtils.DEFAULT_BOOTSTRAP_SERVERS;
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 8093;
    RegionalAnalyticsService service = new RegionalAnalyticsService(port);
    service.start(bootstrapServers, "/tmp/upi-regional-state", new Properties());
    MicroserviceUtils.addShutdownHookAndBlock(service);
  }
}
