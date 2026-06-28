package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.beans.MerchantStatsBean;
import io.confluent.examples.streams.microservices.domain.beans.UpiTransactionBean;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Properties;

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.startJetty;

/**
 * Real-time merchant analytics pipeline using Kafka Streams KTable aggregations.
 *
 * Consumes from upi-transactions-p2m (merchant payments only) and maintains
 * a stateful KTable of per-merchant revenue stats queryable via REST.
 *
 * Powers dashboards for merchants like Swiggy, Zomato, Amazon, Ola, etc.
 * tracking their real-time UPI payment inflow.
 *
 * REST API (port 8091):
 *   GET /merchant/{merchantId}/stats  -> real-time revenue stats
 *   GET /merchant/top                 -> top merchants by revenue
 *   GET /merchant/category/{cat}      -> stats by category (Food, Shopping, etc.)
 */
@Path("merchant")
public class MerchantAnalyticsService implements Service {

  private static final Logger log = LoggerFactory.getLogger(MerchantAnalyticsService.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String MERCHANT_STATS_STORE = "merchant-stats-store";

  private KafkaStreams streams;
  private Server jettyServer;
  private final int port;

  public MerchantAnalyticsService(int port) {
    this.port = port;
  }

  @Override
  public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> p2mRaw = builder.stream(
        UpiTransactionService.TOPIC_P2M,
        Consumed.with(Serdes.String(), Serdes.String()));

    // Parse transactions and key by merchantId
    KStream<String, UpiTransactionBean> txnStream = p2mRaw
        .mapValues(json -> {
          try {
            return mapper.readValue(json, UpiTransactionBean.class);
          } catch (Exception e) {
            log.warn("[MERCHANT_PARSE_WARN] {}", e.getMessage());
            return null;
          }
        })
        .filter((k, v) -> v != null && v.getMerchantId() != null)
        .selectKey((k, txn) -> txn.getMerchantId());

    // Aggregate into merchant stats KTable (stored in RocksDB)
    txnStream
        .groupByKey(Grouped.with(Serdes.String(), buildUpiSerde()))
        .aggregate(
            () -> new MerchantStatsBean(),
            (merchantId, txn, stats) -> {
              if (stats.getMerchantId() == null) {
                stats.setMerchantId(merchantId);
                stats.setMerchantName(txn.getMerchantName() != null ? txn.getMerchantName() : merchantId);
                stats.setCategory(txn.getCategory() != null ? txn.getCategory() : "General");
              }
              MerchantStatsBean updated = stats.merge(txn);
              log.info("[MERCHANT_AGG] merchant={} category={} totalRevenue=₹{} txns={}",
                  merchantId, updated.getCategory(),
                  Math.round(updated.getTotalRevenueInr()), updated.getSuccessCount());
              return updated;
            },
            Materialized.<String, MerchantStatsBean, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(
                MERCHANT_STATS_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(buildMerchantStatsSerde())
        );

    Properties props = MicroserviceUtils.baseStreamsConfig(
        bootstrapServers, stateDir, "merchant-analytics", defaultConfig);
    streams = new KafkaStreams(builder.build(), props);
    streams.start();

    jettyServer = startJetty(port, this);
    log.info("[MERCHANT_START] Merchant Analytics Service online at port={}. "
        + "Aggregating P2M payments from topic={}",
        port, UpiTransactionService.TOPIC_P2M);
  }

  @GET
  @Path("/{merchantId}/stats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMerchantStats(@PathParam("merchantId") String merchantId) {
    try {
      ReadOnlyKeyValueStore<String, MerchantStatsBean> store = streams.store(
          StoreQueryParameters.fromNameAndType(MERCHANT_STATS_STORE,
              QueryableStoreTypes.keyValueStore()));
      MerchantStatsBean stats = store.get(merchantId);
      if (stats == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("{\"error\": \"Merchant " + merchantId + " not found\"}").build();
      }
      log.info("[MERCHANT_QUERY] merchantId={} revenue=₹{} txns={}",
          merchantId, stats.getTotalRevenueInr(), stats.getSuccessCount());
      return Response.ok(stats).build();
    } catch (Exception e) {
      log.error("[MERCHANT_QUERY_ERROR] {}", e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("/top")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTopMerchants(@QueryParam("limit") @DefaultValue("10") int limit) {
    try {
      ReadOnlyKeyValueStore<String, MerchantStatsBean> store = streams.store(
          StoreQueryParameters.fromNameAndType(MERCHANT_STATS_STORE,
              QueryableStoreTypes.keyValueStore()));

      List<MerchantStatsBean> all = new ArrayList<>();
      store.all().forEachRemaining(kv -> all.add(kv.value));
      all.sort((a, b) -> Double.compare(b.getTotalRevenueInr(), a.getTotalRevenueInr()));

      List<MerchantStatsBean> top = all.subList(0, Math.min(limit, all.size()));
      log.info("[MERCHANT_TOP_QUERY] Returning top {} merchants out of {}", top.size(), all.size());
      return Response.ok(top).build();
    } catch (Exception e) {
      log.error("[MERCHANT_TOP_ERROR] {}", e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("/category/{category}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMerchantsByCategory(@PathParam("category") String category) {
    try {
      ReadOnlyKeyValueStore<String, MerchantStatsBean> store = streams.store(
          StoreQueryParameters.fromNameAndType(MERCHANT_STATS_STORE,
              QueryableStoreTypes.keyValueStore()));

      List<MerchantStatsBean> result = new ArrayList<>();
      store.all().forEachRemaining(kv -> {
        if (category.equalsIgnoreCase(kv.value.getCategory())) {
          result.add(kv.value);
        }
      });
      result.sort((a, b) -> Double.compare(b.getTotalRevenueInr(), a.getTotalRevenueInr()));

      log.info("[MERCHANT_CAT_QUERY] category={} merchants={}", category, result.size());
      return Response.ok(result).build();
    } catch (Exception e) {
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  // Minimal inline Serdes for UpiTransactionBean and MerchantStatsBean using Jackson JSON

  private org.apache.kafka.common.serialization.Serde<UpiTransactionBean> buildUpiSerde() {
    return Serdes.serdeFrom(
        (topic, data) -> {
          try { return mapper.writeValueAsBytes(data); } catch (JsonProcessingException e) { return new byte[0]; }
        },
        (topic, bytes) -> {
          try { return mapper.readValue(bytes, UpiTransactionBean.class); } catch (Exception e) { return null; }
        });
  }

  private org.apache.kafka.common.serialization.Serde<MerchantStatsBean> buildMerchantStatsSerde() {
    return Serdes.serdeFrom(
        (topic, data) -> {
          try { return mapper.writeValueAsBytes(data); } catch (JsonProcessingException e) { return new byte[0]; }
        },
        (topic, bytes) -> {
          try { return mapper.readValue(bytes, MerchantStatsBean.class); } catch (Exception e) { return new MerchantStatsBean(); }
        });
  }

  @Override
  public void stop() {
    if (streams != null) streams.close();
    if (jettyServer != null) {
      try { jettyServer.stop(); } catch (Exception e) { log.error("Jetty stop error", e); }
    }
  }

  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : MicroserviceUtils.DEFAULT_BOOTSTRAP_SERVERS;
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 8091;
    MerchantAnalyticsService service = new MerchantAnalyticsService(port);
    service.start(bootstrapServers, "/tmp/upi-merchant-state", new Properties());
    MicroserviceUtils.addShutdownHookAndBlock(service);
  }
}
