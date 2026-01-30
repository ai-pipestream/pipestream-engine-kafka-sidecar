package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.sidecar.v1.DlqMessage;
import com.google.protobuf.Timestamp;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducerRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class DlqProducer {

    private static final Logger LOG = Logger.getLogger(DlqProducer.class);

    @Inject
    Vertx vertx;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "pipestream.dlq.global-topic", defaultValue = "pipestream.global.dlq")
    String globalDlqTopic;

    private KafkaProducer<UUID, DlqMessage> producer;

    void onStart(@Observes StartupEvent ev) {
        LOG.info("Initializing DLQ Producer...");
        createProducer();
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (producer != null) {
            producer.close().await().indefinitely();
        }
    }

    private void createProducer() {
        // Resolve registry URL at runtime
        String registryUrl = ConfigProvider.getConfig()
                .getOptionalValue("apicurio.registry.url", String.class)
                .or(() -> ConfigProvider.getConfig().getOptionalValue("mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class))
                .orElseThrow(() -> new IllegalStateException(
                        "apicurio.registry.url must be set. " +
                        "For manual Kafka producers, the Apicurio DevServices extension doesn't auto-detect the need. " +
                        "In dev mode with Compose DevServices, set apicurio.registry.url in application.properties. " +
                        "In production, set APICURIO_REGISTRY_URL environment variable."));

        LOG.infof("Creating DLQ Kafka Producer with registry: %s", registryUrl);

        Map<String, String> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.apicurio.registry.serde.protobuf.ProtobufKafkaSerializer");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        
        // Apicurio Registry configuration
        config.put("apicurio.registry.url", registryUrl);
        config.put("apicurio.registry.auto-register", "true");
        config.put("apicurio.protobuf.derive.class", "true");
        ConfigProvider.getConfig().getOptionalValue("apicurio.registry.use-id", String.class)
            .ifPresent(value -> config.put("apicurio.registry.use-id", value));

        producer = KafkaProducer.create(vertx, config);
    }

    public Uni<Void> sendToDlq(String originalTopic, UUID key, PipeStream stream, Throwable error, 
                               int retryCount, int partition, long offset) {
        String dlqTopic = determineDlqTopic(originalTopic);
        
        DlqMessage dlqMessage = buildDlqMessage(stream, originalTopic, error, retryCount, partition, offset);
        
        LOG.warnf("Sending failed message to DLQ topic %s (original: %s, error: %s)", 
                dlqTopic, originalTopic, error.getMessage());

        return producer.send(KafkaProducerRecord.create(dlqTopic, key, dlqMessage))
                .onItem().invoke(meta -> LOG.infof("Successfully published to DLQ topic %s (partition: %d, offset: %d)", 
                        dlqTopic, meta.getPartition(), meta.getOffset()))
                .replaceWithVoid()
                .onFailure().invoke(t -> LOG.errorf(t, "Failed to publish to DLQ topic %s", dlqTopic));
    }

    String determineDlqTopic(String originalTopic) {
        // Pattern: pipestream.{cluster}.{node} -> dlq.{cluster}.{node}
        // Pattern: intake.{datasource} -> dlq.intake.{datasource} (or global?)
        if (originalTopic.startsWith("pipestream.")) {
            return originalTopic.replaceFirst("pipestream\\.", "dlq.");
        } else if (originalTopic.startsWith("intake.")) {
            return originalTopic.replaceFirst("intake\\.", "dlq.intake.");
        }
        return globalDlqTopic;
    }

    private DlqMessage buildDlqMessage(PipeStream stream, String originalTopic, Throwable error, 
                                       int retryCount, int partition, long offset) {
        Instant now = Instant.now();
        String errorMessage = error.getMessage();
        if (errorMessage == null) {
            errorMessage = error.getClass().getName();
        }

        return DlqMessage.newBuilder()
                .setStream(stream)
                .setErrorType(error.getClass().getSimpleName())
                .setErrorMessage(errorMessage)
                .setFailedAt(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .setRetryCount(retryCount)
                .setOriginalTopic(originalTopic)
                .setOriginalPartition(partition)
                .setOriginalOffset(offset)
                .build();
    }
}
