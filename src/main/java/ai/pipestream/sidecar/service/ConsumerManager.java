package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.sidecar.service.ConsulLeaseManager.TopicType;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages Kafka consumers for dynamically assigned topics.
 * <p>
 * Consumes {@link PipeStream} messages containing {@link DocumentReference},
 * hydrates them via RepoService, and hands off to Engine.
 */
@ApplicationScoped
public class ConsumerManager {

    private static final Logger LOG = Logger.getLogger(ConsumerManager.class);

    @Inject
    Vertx vertx;

    @Inject
    DocumentHydrator documentHydrator;

    @Inject
    RequestValidator requestValidator;

    @Inject
    EngineClient engineClient;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "pipestream.sidecar.consumer-group", defaultValue = "pipestream-sidecar")
    String consumerGroupId;

    @ConfigProperty(name = "pipestream.sidecar.consumer.enabled", defaultValue = "true")
    boolean consumerEnabled;

    @ConfigProperty(name = "pipestream.sidecar.processing.max-retries", defaultValue = "3")
    int processingMaxRetries;

    @ConfigProperty(name = "pipestream.sidecar.processing.backoff.initial-ms", defaultValue = "25")
    long processingBackoffInitialMs;

    @ConfigProperty(name = "pipestream.sidecar.processing.backoff.max-ms", defaultValue = "250")
    long processingBackoffMaxMs;

    /** Single consumer instance that subscribes to multiple topics */
    private KafkaConsumer<UUID, PipeStream> consumer;

    /** Currently subscribed topics with their types */
    private final ConcurrentHashMap<String, TopicType> subscribedTopics = new ConcurrentHashMap<>();
    
    /** Track paused topics */
    private final Set<String> pausedTopics = ConcurrentHashMap.newKeySet();

    void onStart(@Observes StartupEvent ev) {
        LOG.info("Initializing ConsumerManager...");
        
        // Log gRPC configuration for debugging health checks
        var config = ConfigProvider.getConfig();
        boolean useSeparateServer = config.getOptionalValue("quarkus.grpc.server.use-separate-server", Boolean.class).orElse(true);
        boolean healthServiceEnabled = config.getOptionalValue("quarkus.grpc.server.enable-health-service", Boolean.class).orElse(false);
        int httpPort = config.getOptionalValue("quarkus.http.port", Integer.class).orElse(8080);
        LOG.infof("gRPC Configuration - use-separate-server: %s, enable-health-service: %s, HTTP port: %d", 
                useSeparateServer, healthServiceEnabled, httpPort);
        
        if (!consumerEnabled) {
            LOG.info("pipestream.sidecar.consumer.enabled=false; skipping Kafka consumer startup.");
            return;
        }
        createConsumer();
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (consumer != null) {
            LOG.info("Closing Kafka Consumer...");
            consumer.close().await().indefinitely();
        }
    }

    /**
     * Adds a topic to the subscription set.
     *
     * @param topicName The Kafka topic name
     * @param topicType The type of topic (INTAKE or NODE)
     */
    public synchronized void addTopic(String topicName, TopicType topicType) {
        if (subscribedTopics.containsKey(topicName)) {
            LOG.debugf("Topic %s already subscribed", topicName);
            return;
        }

        subscribedTopics.put(topicName, topicType);
        updateSubscription();
        LOG.infof("Added topic %s (%s) to subscriptions", topicName, topicType);
    }

    /**
     * Removes a topic from the subscription set.
     *
     * @param topicName The Kafka topic name to remove
     */
    public synchronized void removeTopic(String topicName) {
        if (subscribedTopics.remove(topicName) != null) {
            pausedTopics.remove(topicName); // Clear paused state if removed
            updateSubscription();
            LOG.infof("Removed topic %s from subscriptions", topicName);
        }
    }

    /**
     * Pauses consumption for a specific topic.
     *
     * @param topicName The topic to pause
     */
    public void pauseTopic(String topicName) {
        if (!subscribedTopics.containsKey(topicName)) {
            LOG.warnf("Cannot pause topic %s: not subscribed", topicName);
            return;
        }
        
        pausedTopics.add(topicName);
        
        if (consumer != null) {
            consumer.assignment()
                .map(partitions -> partitions.stream()
                    .filter(tp -> tp.getTopic().equals(topicName))
                    .collect(Collectors.toSet()))
                .subscribe().with(
                    partitionsToPause -> {
                        if (!partitionsToPause.isEmpty()) {
                            consumer.pause(partitionsToPause)
                                .subscribe().with(
                                    v -> LOG.infof("Paused consumer for topic %s", topicName),
                                    e -> LOG.errorf(e, "Failed to pause topic %s", topicName)
                                );
                        }
                    },
                    e -> LOG.errorf(e, "Failed to get assignment for pausing topic %s", topicName)
                );
        }
    }

    /**
     * Resumes consumption for a specific topic.
     *
     * @param topicName The topic to resume
     */
    public void resumeTopic(String topicName) {
        if (!pausedTopics.remove(topicName)) {
            LOG.debugf("Topic %s was not paused", topicName);
            return;
        }

        if (consumer != null) {
            consumer.assignment()
                .map(partitions -> partitions.stream()
                    .filter(tp -> tp.getTopic().equals(topicName))
                    .collect(Collectors.toSet()))
                .subscribe().with(
                    partitionsToResume -> {
                        if (!partitionsToResume.isEmpty()) {
                            consumer.resume(partitionsToResume)
                                .subscribe().with(
                                    v -> LOG.infof("Resumed consumer for topic %s", topicName),
                                    e -> LOG.errorf(e, "Failed to resume topic %s", topicName)
                                );
                        }
                    },
                    e -> LOG.errorf(e, "Failed to get assignment for resuming topic %s", topicName)
                );
        }
    }

    /**
     * Returns the set of currently subscribed topics.
     *
     * @return Set of topic names
     */
    public Set<String> getSubscribedTopics() {
        return new HashSet<>(subscribedTopics.keySet());
    }
    
    public Map<String, TopicType> getSubscribedTopicsWithTypes() {
        return new HashMap<>(subscribedTopics);
    }
    
    public boolean isTopicPaused(String topic) {
        return pausedTopics.contains(topic);
    }

    /**
     * Updates the Kafka consumer subscription to match subscribedTopics.
     */
    private void updateSubscription() {
        if (consumer == null) {
            LOG.warn("Consumer not initialized, cannot update subscription");
            return;
        }

        Set<String> topics = new HashSet<>(subscribedTopics.keySet());
        if (topics.isEmpty()) {
            consumer.unsubscribe()
                .subscribe().with(
                    v -> LOG.info("Unsubscribed from all topics"),
                    e -> LOG.error("Failed to unsubscribe", e)
                );
        } else {
            consumer.subscribe(topics)
                .subscribe().with(
                    v -> LOG.infof("Subscribed to topics: %s", topics),
                    e -> LOG.errorf("Failed to subscribe to topics: %s", topics, e)
                );
        }
    }

    /**
     * Creates the Kafka consumer configured for PipeStream messages.
     */
    private void createConsumer() {
        // Resolve registry URL at runtime
        // Priority: 1. Explicit config property, 2. Connector-level config, 3. Fail with clear error
        String registryUrl = ConfigProvider.getConfig()
                .getOptionalValue("apicurio.registry.url", String.class)
                .orElseGet(() -> ConfigProvider.getConfig()
                        .getOptionalValue("mp.messaging.connector.smallrye-kafka.apicurio.registry.url", String.class)
                        .orElseThrow(() -> new IllegalStateException(
                                "apicurio.registry.url must be set. " +
                                "For manual Kafka consumers, the Apicurio DevServices extension doesn't auto-detect the need. " +
                                "In dev mode with Compose DevServices, set apicurio.registry.url in application.properties. " +
                                "In production, set APICURIO_REGISTRY_URL environment variable.")));

        Map<String, String> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                   "io.apicurio.registry.serde.protobuf.ProtobufKafkaDeserializer");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Apicurio Registry configuration - minimal required per docs
        config.put("apicurio.registry.url", registryUrl);
        config.put("apicurio.registry.auto-register", "true");
        config.put("apicurio.protobuf.derive.class", "true");

        LOG.infof("Creating Kafka Consumer with group %s, registry: %s", consumerGroupId, registryUrl);

        consumer = KafkaConsumer.create(vertx, config);
        consumer.handler(this::handleRecord)
                .exceptionHandler(t -> LOG.error("Kafka Consumer error", t));
    }

    /**
     * Handles a consumed Kafka record.
     * <p>
     * Flow: Extract DocumentReference → Hydrate → Validate → Engine Handoff → Commit
     *
     * @param record The consumed record containing a PipeStream
     */
    private void handleRecord(KafkaConsumerRecord<UUID, PipeStream> record) {
        String topic = record.topic();
        UUID key = record.key();
        PipeStream pipeStream = record.value();

        LOG.debugf("Received record key=%s from topic=%s partition=%d offset=%d",
                key, topic, record.partition(), record.offset());

        // Determine topic type for routing
        TopicType topicType = subscribedTopics.getOrDefault(topic, TopicType.INTAKE);

        // Extract DocumentReference from PipeStream
        if (!pipeStream.hasDocumentRef()) {
            LOG.warnf("PipeStream from topic %s has no DocumentReference, skipping", topic);
            commitOffset(record);
            return;
        }

        DocumentReference docRef = pipeStream.getDocumentRef();
        LOG.debugf("Processing document reference: docId=%s, sourceNodeId=%s, accountId=%s",
                docRef.getDocId(), docRef.getSourceNodeId(), docRef.getAccountId());

        processPipeStream(pipeStream, topicType, topic)
                .subscribe().with(
                        ignored -> {
                            LOG.debugf("Successfully processed document from topic %s", topic);
                            commitOffset(record);
                        },
                        failure -> {
                            LOG.errorf(failure, "Error processing document %s from topic %s",
                                    docRef.getDocId(), topic);
                            // TODO: DLQ publishing (Issue #3 Phase 2)
                            // For now, commit to avoid infinite retry loop
                            commitOffset(record);
                        }
                );
    }



    /**
     * Process a PipeStream through hydration -> validation -> engine routing, with retry/backoff for transient failures.
     * <p>
     * Split out so we can test the processing + retry policy without requiring a Kafka broker.
     */
    Uni<Void> processPipeStream(PipeStream pipeStream, TopicType topicType, String topic) {
        if (!pipeStream.hasDocumentRef()) {
            return Uni.createFrom().voidItem();
        }

        DocumentReference docRef = pipeStream.getDocumentRef();

        return documentHydrator.hydrateDocument(docRef)
                .onItem().transformToUni(hydratedDoc -> {
                    if (!requestValidator.validate(hydratedDoc)) {
                        LOG.warnf("Document %s validation failed, skipping", hydratedDoc.getDocId());
                        return Uni.createFrom().voidItem();
                    }
                    return routeToEngine(pipeStream, hydratedDoc, topicType, topic);
                })
                .replaceWithVoid()
                // FR7: transient failures should be retried with backoff (DLQ later in the loop).
                .onFailure(this::isRetryableProcessingFailure)
                .retry()
                .withBackOff(Duration.ofMillis(processingBackoffInitialMs), Duration.ofMillis(processingBackoffMaxMs))
                .atMost(processingMaxRetries)
                .onFailure().invoke(err ->
                        LOG.errorf(err, "Processing failed after retries: docId=%s, topic=%s", docRef.getDocId(), topic)
                );
    }

    private boolean isRetryableProcessingFailure(Throwable t) {
        if (t instanceof io.grpc.StatusRuntimeException sre) {
            io.grpc.Status.Code code = sre.getStatus().getCode();
            return code == io.grpc.Status.Code.UNAVAILABLE
                    || code == io.grpc.Status.Code.DEADLINE_EXCEEDED
                    || code == io.grpc.Status.Code.RESOURCE_EXHAUSTED
                    || code == io.grpc.Status.Code.ABORTED;
        }
        return false;
    }
    /**
     * Routes a hydrated document to the appropriate Engine endpoint.
     *
     * @param pipeStream The original PipeStream (for metadata)
     * @param hydratedDoc The hydrated PipeDoc
     * @param topicType The topic type determining the endpoint
     * @param topic The source topic name (for extracting datasource/node info)
     * @return Uni completing when handoff is done
     */
    private Uni<Void> routeToEngine(PipeStream pipeStream, PipeDoc hydratedDoc,
                                     TopicType topicType, String topic) {
        return switch (topicType) {
            case INTAKE -> {
                // Extract datasource_id from topic: intake.{datasource_id}
                String datasourceId = topic.startsWith("intake.")
                    ? topic.substring("intake.".length())
                    : "unknown";
                yield engineClient.intakeHandoff(hydratedDoc, datasourceId, pipeStream)
                    .replaceWithVoid();
            }
            case NODE -> {
                // Extract cluster.node from topic: pipestream.{cluster}.{node_id}
                String targetNodeId = pipeStream.getCurrentNodeId();
                if (targetNodeId.isEmpty()) {
                    // Fallback: parse from topic name
                    targetNodeId = topic.startsWith("pipestream.")
                        ? topic.substring("pipestream.".length())
                        : "unknown";
                }
                yield engineClient.processNode(hydratedDoc, targetNodeId, pipeStream)
                    .replaceWithVoid();
            }
        };
    }

    /**
     * Commits the offset for a successfully processed record.
     *
     * @param record The record to commit
     */
    private void commitOffset(KafkaConsumerRecord<UUID, PipeStream> record) {
        consumer.commit()
            .subscribe().with(
                v -> LOG.debugf("Offset committed for topic=%s partition=%d offset=%d",
                        record.topic(), record.partition(), record.offset()),
                t -> LOG.errorf(t, "Failed to commit offset for topic=%s", record.topic())
            );
    }

}
