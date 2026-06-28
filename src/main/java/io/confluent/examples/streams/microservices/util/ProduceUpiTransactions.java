package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.UpiTransactionService;
import io.confluent.examples.streams.microservices.domain.beans.UpiTransactionBean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Realistic India UPI transaction data generator.
 *
 * Simulates the full diversity of India's UPI ecosystem:
 *  - 300+ million active users across PhonePe, Google Pay, Paytm, BHIM
 *  - ₹10 chai at a roadside stall to ₹2L rent payments
 *  - All major Indian cities and states
 *  - Realistic failure patterns (bank downtime, wrong PIN, insufficient balance)
 *  - Merchant payments to Swiggy, Zomato, Amazon, Ola, BigBasket, etc.
 *  - P2P transfers between family members, friends, landlords
 *
 * Usage:
 *   ProduceUpiTransactions [bootstrapServers] [transactionsPerSecond] [durationSeconds]
 *   ProduceUpiTransactions localhost:9092 100 60
 */
public class ProduceUpiTransactions {

  private static final Logger log = LoggerFactory.getLogger(ProduceUpiTransactions.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final AtomicLong txnCounter = new AtomicLong(0);

  // ── India UPI ecosystem data ───────────────────────────────────────────────

  private static final String[] FIRST_NAMES = {
      "rahul", "priya", "amit", "sunita", "vijay", "kavya", "rohit", "meera",
      "arjun", "deepa", "suresh", "anita", "raj", "pooja", "sanjay", "nisha",
      "arun", "seema", "vinod", "rekha", "manish", "geeta", "prakash", "usha",
      "kiran", "lalitha", "ravi", "savita", "dinesh", "radha", "mohan", "sarla",
      "harish", "parvati", "ganesh", "durga", "shyam", "kamla", "ramesh", "sulochana"
  };

  private static final String[] LAST_NAMES = {
      "sharma", "patel", "singh", "kumar", "gupta", "verma", "joshi", "nair",
      "reddy", "iyer", "mehta", "khan", "mishra", "agarwal", "pandey", "rao",
      "pillai", "menon", "kulkarni", "desai", "jain", "mukherjee", "chatterjee", "bose",
      "das", "ghosh", "sen", "trivedi", "shukla", "dubey", "tiwari", "yadav"
  };

  // UPI handle suffix → bank mapping
  private static final String[][] UPI_HANDLES = {
      {"@oksbi", "SBI"},
      {"@okhdfcbank", "HDFC"},
      {"@okicici", "ICICI"},
      {"@okaxis", "Axis"},
      {"@ybl", "Yes Bank"},          // PhonePe
      {"@paytm", "Paytm Payments Bank"},
      {"@apl", "Axis"},              // Amazon Pay
      {"@ibl", "IDFC First Bank"},
      {"@kotak", "Kotak Mahindra"},
      {"@pnb", "PNB"},
      {"@upi", "NPCI"},              // BHIM UPI
      {"@sbi", "SBI"},
      {"@axisbank", "Axis"},
      {"@hdfcbank", "HDFC"}
  };

  private static final String[] UPI_APPS = {
      "PHONEPE", "PHONEPE", "PHONEPE",       // PhonePe dominates (~47%)
      "GOOGLEPAY", "GOOGLEPAY", "GOOGLEPAY",  // Google Pay (~37%)
      "PAYTM", "PAYTM",                       // Paytm (~10%)
      "BHIM", "AMAZONPAY"                     // Others (~6%)
  };

  private static final Object[][] CITIES_STATES = {
      // {city, state, weight} — weighted by UPI adoption
      {"Mumbai", "Maharashtra", 15},
      {"Delhi", "Delhi NCT", 12},
      {"Bangalore", "Karnataka", 11},
      {"Hyderabad", "Telangana", 8},
      {"Chennai", "Tamil Nadu", 7},
      {"Pune", "Maharashtra", 6},
      {"Kolkata", "West Bengal", 5},
      {"Ahmedabad", "Gujarat", 5},
      {"Jaipur", "Rajasthan", 4},
      {"Lucknow", "Uttar Pradesh", 4},
      {"Surat", "Gujarat", 3},
      {"Kochi", "Kerala", 3},
      {"Indore", "Madhya Pradesh", 3},
      {"Chandigarh", "Punjab", 2},
      {"Nagpur", "Maharashtra", 2},
      {"Bhopal", "Madhya Pradesh", 2},
      {"Visakhapatnam", "Andhra Pradesh", 2},
      {"Patna", "Bihar", 2},
      {"Vadodara", "Gujarat", 2},
      {"Coimbatore", "Tamil Nadu", 2}
  };

  private static final Object[][] MERCHANTS = {
      // {merchantId, merchantName, category, minAmount, maxAmount}
      {"swiggy_ind", "Swiggy", "Food & Dining", 80.0, 800.0},
      {"zomato_ind", "Zomato", "Food & Dining", 100.0, 1200.0},
      {"amazon_ind", "Amazon India", "Shopping", 199.0, 15000.0},
      {"flipkart_ind", "Flipkart", "Shopping", 299.0, 20000.0},
      {"myntra_ind", "Myntra", "Fashion", 499.0, 5000.0},
      {"bigbasket_ind", "BigBasket", "Groceries", 200.0, 2500.0},
      {"blinkit_ind", "Blinkit", "Groceries", 100.0, 1500.0},
      {"ola_ind", "Ola Cabs", "Transport", 50.0, 600.0},
      {"uber_ind", "Uber India", "Transport", 60.0, 700.0},
      {"rapido_ind", "Rapido", "Transport", 30.0, 200.0},
      {"makemytrip_ind", "MakeMyTrip", "Travel", 1500.0, 80000.0},
      {"irctc_ind", "IRCTC", "Travel", 200.0, 5000.0},
      {"bookmyshow_ind", "BookMyShow", "Entertainment", 150.0, 2000.0},
      {"netflix_ind", "Netflix India", "Entertainment", 149.0, 649.0},
      {"hotstar_ind", "Disney+ Hotstar", "Entertainment", 299.0, 1499.0},
      {"airtel_ind", "Airtel", "Utilities", 199.0, 999.0},
      {"jio_ind", "Jio", "Utilities", 179.0, 2999.0},
      {"bsnl_ind", "BSNL", "Utilities", 99.0, 599.0},
      {"bescom_ind", "BESCOM", "Electricity", 500.0, 8000.0},
      {"mahadiscom_ind", "Mahadiscom", "Electricity", 300.0, 10000.0},
      {"lenskart_ind", "Lenskart", "Healthcare", 990.0, 8000.0},
      {"1mg_ind", "1mg", "Healthcare", 100.0, 3000.0},
      {"pharmeasy_ind", "PharmEasy", "Healthcare", 150.0, 2500.0},
      {"byju_ind", "BYJU'S", "Education", 1000.0, 5000.0},
      {"unacademy_ind", "Unacademy", "Education", 500.0, 3000.0}
  };

  private static final String[] FAILURE_REASONS = {
      "INSUFFICIENT_BALANCE",
      "INVALID_VPA",
      "WRONG_PIN",
      "BANK_SERVER_DOWN",
      "TRANSACTION_LIMIT_EXCEEDED",
      "ACCOUNT_BLOCKED",
      "NETWORK_TIMEOUT",
      "DUPLICATE_REQUEST",
      "PAYMENT_GATEWAY_ERROR",
      "RBI_LIMIT_EXCEEDED"
  };

  private static final String[] DEVICE_TYPES = {"ANDROID", "ANDROID", "ANDROID", "IOS", "IOS", "WEB"};

  // ── Generator ──────────────────────────────────────────────────────────────

  public static UpiTransactionBean generateTransaction(Random rng) {
    long id = txnCounter.incrementAndGet();
    String txnId = "UPI" + System.currentTimeMillis() + String.format("%06d", id);

    // Sender
    String senderName = randomName(rng);
    String[] senderHandle = UPI_HANDLES[rng.nextInt(UPI_HANDLES.length)];
    String senderVpa = senderName + senderHandle[0];
    String senderBank = senderHandle[1];

    // Receiver - P2P or P2M
    boolean isMerchant = rng.nextDouble() < 0.65; // 65% merchant payments, 35% P2P
    String receiverVpa, receiverBank, merchantId, merchantName, category;
    double minAmt, maxAmt;

    if (isMerchant) {
      Object[] merchant = MERCHANTS[rng.nextInt(MERCHANTS.length)];
      merchantId = (String) merchant[0];
      merchantName = (String) merchant[1];
      category = (String) merchant[2];
      minAmt = (double) merchant[3];
      maxAmt = (double) merchant[4];
      String[] receiverHandle = UPI_HANDLES[rng.nextInt(UPI_HANDLES.length)];
      receiverVpa = merchantId + receiverHandle[0];
      receiverBank = receiverHandle[1];
    } else {
      merchantId = null;
      merchantName = null;
      category = "P2P Transfer";
      minAmt = 10.0;
      maxAmt = 50000.0;
      String receiverName = randomName(rng);
      String[] receiverHandle = UPI_HANDLES[rng.nextInt(UPI_HANDLES.length)];
      receiverVpa = receiverName + receiverHandle[0];
      receiverBank = receiverHandle[1];
    }

    // Amount with realistic India distribution (most txns are small)
    double amount = generateAmount(rng, minAmt, maxAmt);

    // Location
    Object[] cityState = weightedCityState(rng);
    String city = (String) cityState[0];
    String state = (String) cityState[1];

    // Status — 94% success rate (India UPI average)
    String status, failureReason;
    double roll = rng.nextDouble();
    if (roll < 0.94) {
      status = "SUCCESS";
      failureReason = null;
    } else if (roll < 0.97) {
      status = "FAILED";
      failureReason = FAILURE_REASONS[rng.nextInt(FAILURE_REASONS.length)];
      amount = 0;
    } else {
      status = "PENDING";
      failureReason = "PROCESSING";
    }

    String app = UPI_APPS[rng.nextInt(UPI_APPS.length)];
    String device = DEVICE_TYPES[rng.nextInt(DEVICE_TYPES.length)];

    return new UpiTransactionBean(
        txnId, senderVpa, receiverVpa, senderBank, receiverBank,
        Math.round(amount * 100.0) / 100.0, status, category,
        merchantName, merchantId, city, state, device, app,
        System.currentTimeMillis(), failureReason
    );
  }

  private static String randomName(Random rng) {
    return FIRST_NAMES[rng.nextInt(FIRST_NAMES.length)] + "."
        + LAST_NAMES[rng.nextInt(LAST_NAMES.length)]
        + (rng.nextInt(100));
  }

  private static double generateAmount(Random rng, double min, double max) {
    // Log-normal distribution: most transactions are small, few are large
    double logMin = Math.log(min);
    double logMax = Math.log(max);
    double logAmount = logMin + rng.nextDouble() * (logMax - logMin);
    return Math.exp(logAmount);
  }

  private static Object[] weightedCityState(Random rng) {
    // Compute total weight
    int totalWeight = 0;
    for (Object[] cs : CITIES_STATES) totalWeight += (int) cs[2];
    int pick = rng.nextInt(totalWeight);
    int cumulative = 0;
    for (Object[] cs : CITIES_STATES) {
      cumulative += (int) cs[2];
      if (pick < cumulative) return cs;
    }
    return CITIES_STATES[0];
  }

  // ── Main ───────────────────────────────────────────────────────────────────

  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : MicroserviceUtils.DEFAULT_BOOTSTRAP_SERVERS;
    int tps = args.length > 1 ? Integer.parseInt(args[1]) : 50;
    int durationSeconds = args.length > 2 ? Integer.parseInt(args[2]) : 60;

    log.info("[UPI_PRODUCER_START] Starting India UPI transaction simulator. "
        + "target={} TPS, duration={}s, bootstrapServers={}",
        tps, durationSeconds, bootstrapServers);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "upi-data-simulator");

    Random rng = new Random();
    long successCount = 0, failedCount = 0;
    double totalAmount = 0;

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props,
        new StringSerializer(), new StringSerializer())) {

      long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);
      long intervalMs = 1000L / tps;

      while (System.currentTimeMillis() < endTime) {
        long loopStart = System.currentTimeMillis();

        UpiTransactionBean txn = generateTransaction(rng);
        String json = mapper.writeValueAsString(txn);

        // Route to correct topic
        String topic = UpiTransactionService.TOPIC_ALL;
        String routingKey = txn.getSenderVpa();

        producer.send(new ProducerRecord<>(topic, routingKey, json), (meta, e) -> {
          if (e != null) {
            log.error("[UPI_PRODUCER_ERROR] {}", e.getMessage());
          }
        });

        // Also route to P2P or P2M
        if (txn.getMerchantId() != null) {
          producer.send(new ProducerRecord<>(UpiTransactionService.TOPIC_P2M,
              txn.getMerchantId(), json));
        } else {
          producer.send(new ProducerRecord<>(UpiTransactionService.TOPIC_P2P,
              txn.getSenderVpa(), json));
        }

        if ("SUCCESS".equals(txn.getStatus())) {
          successCount++;
          totalAmount += txn.getAmountInr();
        } else if ("FAILED".equals(txn.getStatus())) {
          failedCount++;
        }

        // Log progress every 1000 transactions
        if (txnCounter.get() % 1000 == 0) {
          log.info("[UPI_PRODUCER_STATS] Sent {} txns | success={} failed={} totalAmount=₹{}",
              txnCounter.get(), successCount, failedCount, Math.round(totalAmount));
        }

        // Rate limiting
        long elapsed = System.currentTimeMillis() - loopStart;
        if (elapsed < intervalMs) {
          Thread.sleep(intervalMs - elapsed);
        }
      }
    }

    log.info("[UPI_PRODUCER_COMPLETE] Done! Total={} txns | Success={} | Failed={} | "
        + "Total Volume=₹{} | Avg=₹{}/txn",
        txnCounter.get(), successCount, failedCount,
        Math.round(totalAmount),
        successCount > 0 ? Math.round(totalAmount / successCount) : 0);
  }
}
