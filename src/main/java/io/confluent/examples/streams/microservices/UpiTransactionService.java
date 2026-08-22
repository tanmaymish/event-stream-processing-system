package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.beans.UpiTransactionBean;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import jakarta.ws.rs.*;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;

/**
 * REST gateway for India UPI payment events.
 *
 * Ingests transactions and routes them to three Kafka topics:
 *   upi-transactions     -> all transactions (source of truth)
 *   upi-transactions-p2p -> peer-to-peer payments only
 *   upi-transactions-p2m -> merchant payments only (for merchant analytics)
 *
 * Realistic throughput: NPCI processes ~400 million UPI txns/day (~4600 TPS peak).
 */
@Path("upi")
public class UpiTransactionService implements Service {

  private static final Logger log = LoggerFactory.getLogger(UpiTransactionService.class);

  public static final String TOPIC_ALL = "upi-transactions";
  public static final String TOPIC_P2P = "upi-transactions-p2p";
  public static final String TOPIC_P2M = "upi-transactions-p2m";

  private final ObjectMapper mapper = new ObjectMapper();
  private KafkaProducer<String, String> producer;
  private Server jettyServer;
  private final int port;

  // Real-time metrics (exposed to monitoring dashboards)
  public static final AtomicLong totalTxns = new AtomicLong(0);
  public static final AtomicLong successTxns = new AtomicLong(0);
  public static final AtomicLong failedTxns = new AtomicLong(0);
  public static final AtomicLong p2pTxns = new AtomicLong(0);
  public static final AtomicLong p2mTxns = new AtomicLong(0);
  // There is deliberately no fraudAlerts counter here. It used to be incremented by
  // UpiVelocityFraudService, which runs in its own JVM, so this process could never observe
  // it and /upi/metrics reported a permanent zero. Alert counts belong to whoever consumes
  // the upi-fraud-alerts topic.

  public UpiTransactionService(int port) {
    this.port = port;
  }

  @POST
  @Path("/transaction")
  @ManagedAsync
  @Consumes(MediaType.APPLICATION_JSON)
  public void submitTransaction(UpiTransactionBean txn, @Suspended final AsyncResponse response) {
    if (txn.getTxnId() == null || txn.getSenderVpa() == null || txn.getReceiverVpa() == null) {
      response.resume(Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\": \"txnId, senderVpa, receiverVpa are required\"}").build());
      return;
    }

    totalTxns.incrementAndGet();
    if ("SUCCESS".equals(txn.getStatus())) successTxns.incrementAndGet();
    else if ("FAILED".equals(txn.getStatus())) failedTxns.incrementAndGet();

    boolean isMerchant = txn.getMerchantId() != null && !txn.getMerchantId().isEmpty();
    if (isMerchant) p2mTxns.incrementAndGet();
    else p2pTxns.incrementAndGet();

    log.info("[UPI_TXN] id={} sender={} receiver={} amount=₹{} status={} city={}",
        txn.getTxnId(), txn.getSenderVpa(), txn.getReceiverVpa(),
        txn.getAmountInr(), txn.getStatus(), txn.getSenderCity());

    try {
      String json = mapper.writeValueAsString(txn);

      // Route to main topic
      producer.send(new ProducerRecord<>(TOPIC_ALL, txn.getSenderVpa(), json), (meta, e) -> {
        if (e != null) {
          log.error("[UPI_TXN_ERROR] Kafka send failed: {}", e.getMessage());
          response.resume(Response.serverError().entity(e.getMessage()).build());
          return;
        }
        log.info("[UPI_TXN_PERSISTED] txn={} topic={} partition={} offset={}",
            txn.getTxnId(), meta.topic(), meta.partition(), meta.offset());
      });

      // Route to P2P or P2M topic for specialized processing
      String routingTopic = isMerchant ? TOPIC_P2M : TOPIC_P2P;
      String routingKey = isMerchant ? txn.getMerchantId() : txn.getSenderVpa();
      producer.send(new ProducerRecord<>(routingTopic, routingKey, json));

      response.resume(Response.status(Response.Status.ACCEPTED)
          .entity("{\"txnId\": \"" + txn.getTxnId() + "\", \"status\": \"QUEUED\"}").build());

    } catch (Exception e) {
      log.error("[UPI_TXN_ERROR] Serialization failed for txn={}: {}", txn.getTxnId(), e.getMessage());
      response.resume(Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build());
    }
  }

  @GET
  @Path("/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> getMetrics() {
    Map<String, Object> m = new HashMap<>();
    m.put("totalTransactions", totalTxns.get());
    m.put("successTransactions", successTxns.get());
    m.put("failedTransactions", failedTxns.get());
    m.put("p2pTransactions", p2pTxns.get());
    m.put("p2mTransactions", p2mTxns.get());
    double successRate = totalTxns.get() > 0
        ? (successTxns.get() * 100.0 / totalTxns.get()) : 0.0;
    m.put("successRatePct", Math.round(successRate * 100.0) / 100.0);
    log.info("[UPI_METRICS] total={} success={} failed={} p2p={} p2m={}",
        totalTxns.get(), successTxns.get(), failedTxns.get(), p2pTxns.get(), p2mTxns.get());
    return m;
  }

  @GET
  @Path("/health")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, String> health() {
    Map<String, String> h = new HashMap<>();
    h.put("service", "UpiTransactionService");
    h.put("status", "UP");
    h.put("network", "NPCI-UPI");
    return h;
  }

  @Override
  public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
    Properties producerConfig = new Properties();
    producerConfig.putAll(defaultConfig);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "upi-transaction-gateway");

    producer = new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer());
    jettyServer = startJetty(port, this);
    log.info("[UPI_SERVICE_START] UPI Transaction Gateway online at port={}. Topics: {}, {}, {}",
        port, TOPIC_ALL, TOPIC_P2P, TOPIC_P2M);
  }

  @Override
  public void stop() {
    if (producer != null) producer.close();
    if (jettyServer != null) {
      try { jettyServer.stop(); } catch (Exception e) { log.error("Jetty stop error", e); }
    }
  }

  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS;
    String schemaRegistryUrl = args.length > 1 ? args[1] : DEFAULT_SCHEMA_REGISTRY_URL;
    int port = args.length > 2 ? Integer.parseInt(args[2]) : 8090;

    Properties config = new Properties();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    UpiTransactionService service = new UpiTransactionService(port);
    service.start(bootstrapServers, "/tmp/upi-txn-state", config);
    addShutdownHookAndBlock(service);
  }
}
