package io.confluent.examples.streams.microservices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.beans.EventBean;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import jakarta.ws.rs.*;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.EVENTS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;

@Path("api")
public class EventService implements Service {

  private static final Logger log = LoggerFactory.getLogger(EventService.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private KafkaProducer<String, String> producer;
  private Server jettyServer;
  private final int port;

  // Production-grade metrics tracking
  public static final AtomicLong totalEvents = new AtomicLong(0);
  public static final AtomicLong failedEvents = new AtomicLong(0);
  public static final AtomicLong alertCount = new AtomicLong(0);

  public EventService(int port) {
    this.port = port;
  }

  @POST
  @Path("/events")
  @ManagedAsync
  @Consumes(MediaType.APPLICATION_JSON)
  public void ingestEvent(EventBean event, @Suspended final AsyncResponse response) {
    totalEvents.incrementAndGet();
    log.info("[EVENT_INGEST] Received event: user={}, action={}, status={}", 
        event.getUserId(), event.getAction(), event.getStatus());

    try {
      String json = mapper.writeValueAsString(event);
      producer.send(new ProducerRecord<>(EVENTS.name(), event.getUserId(), json), (recordMetadata, e) -> {
        if (e != null) {
          log.error("[EVENT_INGEST_ERROR] Failed to send event to Kafka: {}", e.getMessage());
          failedEvents.incrementAndGet();
          response.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
        } else {
          log.info("[EVENT_INGEST_SUCCESS] Event persisted to topic={} partition={} offset={}", 
              recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
          response.resume(Response.status(Response.Status.ACCEPTED).build());
        }
      });
    } catch (Exception e) {
      log.error("[EVENT_INGEST_ERROR] Serialization failed: {}", e.getMessage());
      failedEvents.incrementAndGet();
      response.resume(Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build());
    }
  }

  @GET
  @Path("/metrics/events")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Long> getMetrics() {
    Map<String, Long> metrics = new HashMap<>();
    metrics.put("totalEvents", totalEvents.get());
    metrics.put("failedEvents", failedEvents.get());
    metrics.put("alertCount", alertCount.get());
    log.info("[METRICS_READ] Metrics requested: total={}, failed={}, alerts={}", 
        totalEvents.get(), failedEvents.get(), alertCount.get());
    return metrics;
  }

  @Override
  public void start(String bootstrapServers, String stateDir, Properties defaultConfig) {
    jettyServer = startJetty(port, this);
    producer = startProducer(bootstrapServers, EVENTS, defaultConfig);
    log.info("[SERVICE_START] EventService listening on port: {}", port);
  }

  @Override
  public void stop() {
    if (producer != null) producer.close();
    if (jettyServer != null) {
      try {
        jettyServer.stop();
      } catch (Exception e) {
        log.error("Error stopping Jetty", e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : DEFAULT_BOOTSTRAP_SERVERS;
    String schemaRegistryUrl = args.length > 1 ? args[1] : DEFAULT_SCHEMA_REGISTRY_URL;
    int port = args.length > 2 ? Integer.parseInt(args[2]) : 8080;

    Properties config = new Properties();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    
    EventService service = new EventService(port);
    service.start(bootstrapServers, "/tmp/kafka-streams-events", config);
    addShutdownHookAndBlock(service);
  }
}
