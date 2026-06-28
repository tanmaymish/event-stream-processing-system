package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.beans.BankSettlementBean;
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

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.startJetty;

/**
 * Real-time inter-bank UPI settlement tracking.
 *
 * India's NPCI settles UPI transactions in near real-time. Each transaction
 * creates a debit obligation at the sender's bank and a credit obligation at
 * the receiver's bank. NPCI nets these positions and settles via RBI's RTGS.
 *
 * This service tracks live net positions per bank:
 *   - SBI, HDFC, ICICI, Axis, PNB, Kotak, Yes Bank, Paytm Payments Bank
 *
 * Banks with negative net position (net debtors) must fund NPCI's
 * settlement account before the settlement window closes.
 *
 * REST API (port 8092):
 *   GET /settlement/{bankName}   -> net position for a bank
 *   GET /settlement/all          -> all banks ranked by net position
 *   GET /settlement/debtors      -> banks with negative net position
 */
@Path("settlement")
public class BankSettlementService implements Service {

  private static final Logger log = LoggerFactory.getLogger(BankSettlementService.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String SETTLEMENT_STORE = "bank-settlement-store";

  private KafkaStreams streams;
  private Server jettyServer;
  private final int port;

  public BankSettlementService(int port) {
    this.port = port;
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
        .filter((k, v) -> v != null);

    // Create two streams: one keyed by sender bank, one by receiver bank
    // Sender bank gets debited, receiver bank gets credited
    KStream<String, UpiTransactionBean> debitStream = txnStream
        .filter((k, txn) -> "SUCCESS".equals(txn.getStatus()) && txn.getSenderBank() != null)
        .selectKey((k, txn) -> txn.getSenderBank());

    KStream<String, UpiTransactionBean> creditStream = txnStream
        .filter((k, txn) -> "SUCCESS".equals(txn.getStatus()) && txn.getReceiverBank() != null)
        .selectKey((k, txn) -> txn.getReceiverBank());

    KStream<String, UpiTransactionBean> failedStream = txnStream
        .filter((k, txn) -> "FAILED".equals(txn.getStatus()) && txn.getSenderBank() != null)
        .selectKey((k, txn) -> txn.getSenderBank());

    // Merge and tag each record with type before aggregating
    KStream<String, String> taggedDebits = debitStream
        .mapValues(txn -> "DEBIT:" + txn.getAmountInr());
    KStream<String, String> taggedCredits = creditStream
        .mapValues(txn -> "CREDIT:" + txn.getAmountInr());
    KStream<String, String> taggedFailed = failedStream
        .mapValues(txn -> "FAILED:0");

    // Merge all streams and aggregate per bank
    taggedDebits.merge(taggedCredits).merge(taggedFailed)
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .aggregate(
            () -> new BankSettlementBean(),
            (bankName, tagged, settlement) -> {
              if (settlement.getBankName() == null) {
                settlement.setBankName(bankName);
                settlement.setWindowStartMs(System.currentTimeMillis());
              }
              String[] parts = tagged.split(":", 2);
              String type = parts[0];
              double amount = parts.length > 1 ? parseDouble(parts[1]) : 0;
              switch (type) {
                case "DEBIT":
                  settlement.addDebit(amount);
                  log.info("[SETTLEMENT_DEBIT] bank={} amount=₹{} netPosition=₹{}",
                      bankName, amount, Math.round(settlement.getNetPositionInr()));
                  break;
                case "CREDIT":
                  settlement.addCredit(amount);
                  log.info("[SETTLEMENT_CREDIT] bank={} amount=₹{} netPosition=₹{}",
                      bankName, amount, Math.round(settlement.getNetPositionInr()));
                  break;
                case "FAILED":
                  settlement.addFailed();
                  break;
              }
              if (settlement.getNetPositionInr() < -10000000) { // < -₹1 Crore
                log.warn("[SETTLEMENT_ALERT] bank={} net_position=₹{} - Bank is a significant net debtor! "
                    + "NPCI funding required before settlement window closes.",
                    bankName, Math.round(settlement.getNetPositionInr()));
              }
              settlement.setWindowEndMs(System.currentTimeMillis());
              return settlement;
            },
            Materialized.<String, BankSettlementBean, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(
                SETTLEMENT_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(buildSettlementSerde())
        );

    Properties props = MicroserviceUtils.baseStreamsConfig(
        bootstrapServers, stateDir, "bank-settlement", defaultConfig);
    streams = new KafkaStreams(builder.build(), props);
    streams.start();

    jettyServer = startJetty(port, this);
    log.info("[SETTLEMENT_START] Bank Settlement Service online at port={}. "
        + "Tracking NPCI inter-bank positions in real-time.", port);
  }

  @GET
  @Path("/{bankName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBankSettlement(@PathParam("bankName") String bankName) {
    try {
      ReadOnlyKeyValueStore<String, BankSettlementBean> store = getStore();
      BankSettlementBean settlement = store.get(bankName);
      if (settlement == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity("{\"error\": \"No settlement data for bank: " + bankName + "\"}").build();
      }
      return Response.ok(settlement).build();
    } catch (Exception e) {
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("/all")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllBankPositions() {
    try {
      ReadOnlyKeyValueStore<String, BankSettlementBean> store = getStore();
      List<BankSettlementBean> all = new ArrayList<>();
      store.all().forEachRemaining(kv -> all.add(kv.value));
      all.sort((a, b) -> Double.compare(b.getNetPositionInr(), a.getNetPositionInr()));
      log.info("[SETTLEMENT_ALL_QUERY] Returning positions for {} banks", all.size());
      return Response.ok(all).build();
    } catch (Exception e) {
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  @GET
  @Path("/debtors")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getNetDebtors() {
    try {
      ReadOnlyKeyValueStore<String, BankSettlementBean> store = getStore();
      List<BankSettlementBean> debtors = new ArrayList<>();
      store.all().forEachRemaining(kv -> {
        if (kv.value.getNetPositionInr() < 0) debtors.add(kv.value);
      });
      debtors.sort((a, b) -> Double.compare(a.getNetPositionInr(), b.getNetPositionInr()));
      log.warn("[SETTLEMENT_DEBTORS] {} banks have negative net UPI positions", debtors.size());
      return Response.ok(debtors).build();
    } catch (Exception e) {
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  private ReadOnlyKeyValueStore<String, BankSettlementBean> getStore() {
    return streams.store(StoreQueryParameters.fromNameAndType(
        SETTLEMENT_STORE, QueryableStoreTypes.keyValueStore()));
  }

  private double parseDouble(String s) {
    try { return Double.parseDouble(s); } catch (Exception e) { return 0; }
  }

  private org.apache.kafka.common.serialization.Serde<BankSettlementBean> buildSettlementSerde() {
    return Serdes.serdeFrom(
        (topic, data) -> {
          try { return mapper.writeValueAsBytes(data); } catch (JsonProcessingException e) { return new byte[0]; }
        },
        (topic, bytes) -> {
          try { return mapper.readValue(bytes, BankSettlementBean.class); } catch (Exception e) { return new BankSettlementBean(); }
        });
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
    int port = args.length > 1 ? Integer.parseInt(args[1]) : 8092;
    BankSettlementService service = new BankSettlementService(port);
    service.start(bootstrapServers, "/tmp/upi-settlement-state", new Properties());
    MicroserviceUtils.addShutdownHookAndBlock(service);
  }
}
