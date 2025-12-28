package ai.pipestream.sidecar.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.SessionBehavior;
import io.vertx.ext.consul.SessionOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.consul.ConsulClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Consul-based lease acquisition for Kafka topics.
 * <p>
 * Uses the pull model: sidecars actively compete for locks on topic keys.
 * When a lock is acquired, the sidecar starts consuming from that topic.
 * Locks are automatically released if the sidecar dies (session TTL expiry).
 * <p>
 * Key paths:
 * <ul>
 *   <li>{@code pipestream/intake-topics/{datasource_id}} - Intake topics</li>
 *   <li>{@code pipestream/node-topics/{cluster_id}.{node_id}} - Node topics</li>
 * </ul>
 */
@ApplicationScoped
public class ConsulLeaseManager {

    private static final Logger LOG = Logger.getLogger(ConsulLeaseManager.class);

    @Inject
    ConsulClient consulClient;

    @Inject
    Vertx vertx;

    @Inject
    ConsumerManager consumerManager;

    @ConfigProperty(name = "pipestream.sidecar.intake-topics-path", defaultValue = "pipestream/intake-topics")
    String intakeTopicsPath;

    @ConfigProperty(name = "pipestream.sidecar.node-topics-path", defaultValue = "pipestream/node-topics")
    String nodeTopicsPath;

    @ConfigProperty(name = "pipestream.sidecar.session-ttl-seconds", defaultValue = "30")
    long sessionTtlSeconds;

    @ConfigProperty(name = "pipestream.sidecar.max-leases", defaultValue = "50")
    int maxLeases;

    @ConfigProperty(name = "pipestream.sidecar.poll-interval-ms", defaultValue = "5000")
    long pollIntervalMs;

    @ConfigProperty(name = "pipestream.sidecar.session-renew-interval-ms", defaultValue = "10000")
    long sessionRenewIntervalMs;

    private String sessionId;
    private long pollTimerId = -1;
    private long renewTimerId = -1;

    /** Topics we currently hold locks for: key -> TopicInfo */
    private final ConcurrentHashMap<String, TopicInfo> heldLeases = new ConcurrentHashMap<>();

    /**
     * Information about an acquired topic lease.
     *
     * @param kvKey The Consul KV key path
     * @param topicName The Kafka topic name
     * @param topicType Whether this is an INTAKE or NODE topic
     */
    public record TopicInfo(String kvKey, String topicName, TopicType topicType) {}

    /**
     * Type of topic - determines which Engine endpoint to call.
     */
    public enum TopicType {
        /** Intake topics use IntakeHandoff endpoint */
        INTAKE,
        /** Node topics use ProcessNode endpoint */
        NODE
    }

    void onStart(@Observes StartupEvent ev) {
        LOG.info("Starting Consul Lease Manager...");
        createSession()
            .subscribe().with(
                sid -> {
                    this.sessionId = sid;
                    LOG.infof("Consul session created: %s", sessionId);
                    startPolling();
                    startSessionRenewal();
                },
                error -> LOG.errorf(error, "Failed to create Consul session")
            );
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOG.info("Stopping Consul Lease Manager...");

        // Cancel timers
        if (pollTimerId >= 0) {
            vertx.cancelTimer(pollTimerId);
        }
        if (renewTimerId >= 0) {
            vertx.cancelTimer(renewTimerId);
        }

        // Release all held leases
        for (TopicInfo info : heldLeases.values()) {
            releaseLock(info.kvKey())
                .subscribe().with(
                    released -> LOG.infof("Released lock on %s", info.kvKey()),
                    error -> LOG.warnf("Failed to release lock on %s: %s", info.kvKey(), error.getMessage())
                );
        }

        // Destroy session
        if (sessionId != null) {
            consulClient.destroySession(sessionId)
                .subscribe().with(
                    v -> LOG.info("Consul session destroyed"),
                    error -> LOG.warnf("Failed to destroy session: %s", error.getMessage())
                );
        }
    }

    /**
     * Creates a Consul session for lock acquisition.
     * Session behavior is DELETE - locks are released when session expires.
     *
     * @return Uni containing the session ID
     */
    private Uni<String> createSession() {
        SessionOptions options = new SessionOptions()
            .setBehavior(SessionBehavior.DELETE)
            .setTtl(sessionTtlSeconds)
            .setName("kafka-sidecar-" + System.getenv().getOrDefault("HOSTNAME", "local"));

        return consulClient.createSessionWithOptions(options);
    }

    /**
     * Starts periodic polling for available topics.
     */
    private void startPolling() {
        // Initial poll
        pollForTopics();

        // Schedule periodic polling (Vert.x setPeriodic takes long millis)
        pollTimerId = vertx.setPeriodic(pollIntervalMs, id -> pollForTopics());
    }

    /**
     * Starts periodic session renewal to prevent TTL expiry.
     */
    private void startSessionRenewal() {
        renewTimerId = vertx.setPeriodic(sessionRenewIntervalMs, id -> {
            if (sessionId != null) {
                consulClient.renewSession(sessionId)
                    .subscribe().with(
                        session -> LOG.debugf("Session renewed: %s", sessionId),
                        error -> LOG.errorf(error, "Failed to renew session - locks may be lost!")
                    );
            }
        });
    }

    /**
     * Polls Consul for available topics and attempts to acquire locks.
     */
    private void pollForTopics() {
        if (sessionId == null) {
            LOG.warn("No session - skipping poll");
            return;
        }

        if (heldLeases.size() >= maxLeases) {
            LOG.debugf("At max leases (%d), skipping poll", maxLeases);
            return;
        }

        // Poll both intake and node topic paths
        pollPath(intakeTopicsPath, TopicType.INTAKE);
        pollPath(nodeTopicsPath, TopicType.NODE);
    }

    /**
     * Polls a specific KV path for available topics.
     *
     * @param basePath The base path to poll
     * @param topicType The type of topics at this path
     */
    private void pollPath(String basePath, TopicType topicType) {
        consulClient.getKeys(basePath)
            .subscribe().with(
                keys -> {
                    if (keys == null || keys.isEmpty()) {
                        LOG.debugf("No keys found under %s", basePath);
                        return;
                    }
                    for (String key : keys) {
                        if (!heldLeases.containsKey(key) && heldLeases.size() < maxLeases) {
                            tryAcquireLock(key, topicType);
                        }
                    }
                },
                error -> LOG.debugf("Failed to list keys under %s: %s", basePath, error.getMessage())
            );
    }

    /**
     * Attempts to acquire a lock on a topic key.
     *
     * @param kvKey The Consul KV key to lock
     * @param topicType The type of topic
     */
    private void tryAcquireLock(String kvKey, TopicType topicType) {
        KeyValueOptions options = new KeyValueOptions()
            .setAcquireSession(sessionId);

        // The value stored is just our sidecar identifier for debugging
        String value = "locked-by-" + sessionId;

        consulClient.putValueWithOptions(kvKey, value, options)
            .subscribe().with(
                acquired -> {
                    if (acquired) {
                        String topicName = deriveTopicName(kvKey, topicType);
                        TopicInfo info = new TopicInfo(kvKey, topicName, topicType);
                        heldLeases.put(kvKey, info);
                        LOG.infof("Acquired lock on %s -> topic %s (%s)", kvKey, topicName, topicType);
                        consumerManager.addTopic(topicName, topicType);
                    } else {
                        LOG.debugf("Lock on %s held by another sidecar", kvKey);
                    }
                },
                error -> LOG.warnf("Failed to acquire lock on %s: %s", kvKey, error.getMessage())
            );
    }

    /**
     * Releases a lock on a topic key.
     *
     * @param kvKey The Consul KV key to release
     * @return Uni completing when release is done
     */
    private Uni<Boolean> releaseLock(String kvKey) {
        KeyValueOptions options = new KeyValueOptions()
            .setReleaseSession(sessionId);

        return consulClient.putValueWithOptions(kvKey, "", options)
            .invoke(released -> {
                if (released) {
                    TopicInfo removed = heldLeases.remove(kvKey);
                    if (removed != null) {
                        consumerManager.removeTopic(removed.topicName());
                    }
                }
            });
    }

    /**
     * Derives the Kafka topic name from a Consul KV key.
     * <p>
     * Intake: {@code pipestream/intake-topics/datasource-123} → {@code intake.datasource-123}
     * Node: {@code pipestream/node-topics/prod.chunker} → {@code pipestream.prod.chunker}
     *
     * @param kvKey The Consul KV key
     * @param topicType The topic type
     * @return The Kafka topic name
     */
    private String deriveTopicName(String kvKey, TopicType topicType) {
        String suffix = kvKey.substring(kvKey.lastIndexOf('/') + 1);
        return switch (topicType) {
            case INTAKE -> "intake." + suffix;
            case NODE -> "pipestream." + suffix;
        };
    }

    /**
     * Returns the set of currently held leases.
     *
     * @return Set of TopicInfo for held leases
     */
    public Set<TopicInfo> getHeldLeases() {
        return new HashSet<>(heldLeases.values());
    }

    /**
     * Returns the current session ID.
     *
     * @return The Consul session ID, or null if not connected
     */
    public String getSessionId() {
        return sessionId;
    }
}
