package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.sidecar.v1.DlqMessage;
import ai.pipestream.sidecar.service.ConsulLeaseManager.TopicType;
import ai.pipestream.sidecar.util.WireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducerRecord;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class DlqEndToEndTest {

    private static final Logger LOG = Logger.getLogger(DlqEndToEndTest.class);

    @Inject
    ConsumerManager consumerManager;

    @Inject
    Vertx vertx;

    @Inject
    DlqProducer dlqProducer;

    @Test
    void testDlqFlow() throws Exception {
        String inputTopic = "pipestream.test.cluster.node";
        // Dynamically determine DLQ topic using the same logic as DlqProducer
        String dlqTopic = dlqProducer.determineDlqTopic(inputTopic);
        UUID key = UUID.randomUUID();

        // 1. Prepare message
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId("fail-doc-" + key)
                .setAccountId("acct")
                .build();
        PipeStream stream = PipeStream.newBuilder().setDocumentRef(docRef).build();
        RuntimeException error = new RuntimeException("Test failure");

        // 2. Prepare Consumer for DLQ
        KafkaConsumer<UUID, DlqMessage> dlqConsumer = createDlqConsumer(dlqTopic);

        // 3. Setup handler BEFORE subscribing (to avoid missing messages)
        CompletableFuture<DlqMessage> future = new CompletableFuture<>();
        
        dlqConsumer.handler(record -> {
            if (record.key().equals(key)) {
                LOG.info("Received DLQ message in test handler!");
                future.complete(record.value());
            } else {
                LOG.infof("Received unrelated message in test handler: %s", record.key());
            }
        });

        // 4. Subscribe to DLQ topic
        dlqConsumer.subscribe(dlqTopic).await().indefinitely();

        // 5. Call DLQ producer to publish message
        LOG.infof("Sending message to DLQ via DlqProducer (inputTopic: %s, dlqTopic: %s)", inputTopic, dlqTopic);
        dlqProducer.sendToDlq(inputTopic, key, stream, error, 3, 0, 123L)
                .await().indefinitely();

        // 6. Wait for message in DLQ
        DlqMessage received = future.get(30, TimeUnit.SECONDS);
        
        assertNotNull(received, "Should have received a message in DLQ");
        assertEquals(docRef.getDocId(), received.getStream().getDocumentRef().getDocId());
        assertEquals("RuntimeException", received.getErrorType());
        assertEquals("Test failure", received.getErrorMessage());
        assertEquals(123L, received.getOriginalOffset());
        
        // Cleanup
        dlqConsumer.close().await().indefinitely();
    }

    private Map<String, String> getCommonConfig() {
        var config = ConfigProvider.getConfig();
        String bootstrapServers = config.getValue("kafka.bootstrap.servers", String.class);
        String registryUrl = config.getOptionalValue("apicurio.registry.url", String.class)
                .or(() -> config.getOptionalValue("mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class))
                .orElseThrow(() -> new IllegalStateException("Apicurio Registry URL not found in config"));
        
        LOG.infof("Test using Bootstrap: %s, Registry: %s", bootstrapServers, registryUrl);

        Map<String, String> map = new HashMap<>();
        map.put("bootstrap.servers", bootstrapServers);
        map.put("apicurio.registry.url", registryUrl);
        map.put("apicurio.registry.auto-register", "true");
        return map;
    }

    private KafkaConsumer<UUID, DlqMessage> createDlqConsumer(String topic) {
        // Ensure DlqMessage class is loaded before Apicurio tries to load it
        Class<?> dlqMessageClass = DlqMessage.class;
        
        Map<String, String> config = getCommonConfig();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-consumer-" + UUID.randomUUID());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Explicitly tell Apicurio which class to deserialize (consumer doesn't know the type upfront)
        config.put("apicurio.registry.deserializer.value.return-class", DlqMessage.class.getName());
        
        return KafkaConsumer.create(vertx, config);
    }
}
