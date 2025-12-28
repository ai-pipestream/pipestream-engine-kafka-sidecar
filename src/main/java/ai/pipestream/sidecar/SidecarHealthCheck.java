package ai.pipestream.sidecar;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;

/**
 * Health check for the Kafka Sidecar service.
 * <p>
 * Liveness: Indicates the service is running.
 * Readiness: Indicates the service can accept work (Kafka connected, leases acquired).
 */
@ApplicationScoped
@Liveness
@Readiness
public class SidecarHealthCheck implements HealthCheck {

    /**
     * Default constructor for SidecarHealthCheck.
     */
    public SidecarHealthCheck() {
    }

    @Override
    public HealthCheckResponse call() {
        // TODO: Add actual health checks for Kafka connection and Consul leases
        return HealthCheckResponse.up("kafka-sidecar");
    }
}
