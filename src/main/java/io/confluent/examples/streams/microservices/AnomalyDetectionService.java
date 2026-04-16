package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.beans.EventBean;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.EVENTS;

public class AnomalyDetectionService implements Service {

  private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionService.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private KafkaStreams streams;
  private final int THRESHOLD = 3;

  @Override
  public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> eventStream = builder.stream(EVENTS.name(), 
        Consumed.with(Serdes.String(), Serdes.String()));

    eventStream
        .mapValues(value -> {
          try {
            return mapper.readValue(value, EventBean.class);
          } catch (Exception e) {
            return null;
          }
        })
        .filter((key, event) -> event != null && 
            "FAILED".equals(event.getStatus()) && 
            "LOGIN".equals(event.getAction()))
        .groupBy((key, event) -> event.getUserId())
        .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
        .count()
        .toStream()
        .filter((windowedUser, count) -> count >= THRESHOLD)
        .foreach(this::logAlert);

    Properties props = MicroserviceUtils.baseStreamsConfig(bootstrapServers, stateDir, "anomaly-detector", defaultConfig);
    streams = new KafkaStreams(builder.build(), props);
    streams.start();
    log.info("[ANOMALY_START] Anomaly detection engine online. Monitoring for FAILED LOGIN bursts.");
  }

  private void logAlert(Windowed<String> windowedUser, Long count) {
    String userId = windowedUser.key();
    log.warn("[SECURITY_ALERT] Potential Brute Force Detected! user={} failed_login_count={} window=[{} - {}]", 
        userId, count, windowedUser.window().startTime(), windowedUser.window().endTime());
    EventService.alertCount.incrementAndGet();
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : MicroserviceUtils.DEFAULT_BOOTSTRAP_SERVERS;
    AnomalyDetectionService service = new AnomalyDetectionService();
    service.start(bootstrapServers, "/tmp/kafka-streams-anomaly", new Properties());
    MicroserviceUtils.addShutdownHookAndBlock(service);
  }
}
