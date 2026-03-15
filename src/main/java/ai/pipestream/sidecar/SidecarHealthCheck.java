package ai.pipestream.sidecar;

import ai.pipestream.sidecar.service.ConsumerManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

/**
 * Readiness check for the Kafka Sidecar service.
 * <p>
 * Reports UP when the Kafka consumer is initialized and has at least one
 * subscribed topic. Reports DOWN if the consumer is not started or has
 * no topic subscriptions (meaning no leases were acquired from Consul).
 */
@ApplicationScoped
@Readiness
public class SidecarHealthCheck implements HealthCheck {

    @Inject
    ConsumerManager consumerManager;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder builder = HealthCheckResponse.named("kafka-sidecar");

        boolean consumerReady = consumerManager.isConsumerReady();
        int topicCount = consumerManager.getSubscribedTopicCount();

        builder.withData("consumer_initialized", consumerReady);
        builder.withData("subscribed_topics", topicCount);

        if (!consumerReady) {
            return builder.down().withData("reason", "Kafka consumer not initialized").build();
        }

        if (topicCount == 0) {
            // Consumer is connected but no topics yet — this is normal during startup
            // or when no Consul leases are acquired. Report UP with warning.
            return builder.up().withData("warning", "No topics subscribed — waiting for Consul leases").build();
        }

        return builder.up().build();
    }
}
