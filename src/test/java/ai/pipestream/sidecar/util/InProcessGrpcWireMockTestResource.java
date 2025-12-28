package ai.pipestream.sidecar.util;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.GrpcExtensionFactory;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * In-process WireMock gRPC server for tests that need fine-grained stub control (e.g. fail-once-then-success).
 *
 * This avoids Docker/testcontainers and lets tests register scenario-based stubs.
 */
public class InProcessGrpcWireMockTestResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOG = LoggerFactory.getLogger(InProcessGrpcWireMockTestResource.class);

    private WireMockServer wireMockServer;

    @Override
    public Map<String, String> start() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory()));
        wireMockServer.start();

        int port = wireMockServer.port();
        String serviceAddress = "localhost:" + port;
        LOG.info("Started in-process WireMock gRPC server on {}", serviceAddress);

        Map<String, String> cfg = new HashMap<>();
        cfg.put("test.wiremock.grpc.port", Integer.toString(port));

        // Route both engine and repo-service to this in-process mock.
        cfg.put("stork.engine.service-discovery.type", "static");
        cfg.put("stork.engine.service-discovery.address-list", serviceAddress);
        cfg.put("stork.engine.load-balancer.type", "round-robin");

        cfg.put("stork.repo-service.service-discovery.type", "static");
        cfg.put("stork.repo-service.service-discovery.address-list", serviceAddress);
        cfg.put("stork.repo-service.load-balancer.type", "round-robin");

        // Keep these large to avoid message size surprises.
        cfg.put("quarkus.grpc.clients.engine.max-inbound-message-size", "2147483647");
        cfg.put("quarkus.grpc.clients.repo-service.max-inbound-message-size", "2147483647");

        // Disable actual registration
        cfg.put("pipestream.registration.enabled", "false");

        return cfg;
    }

    @Override
    public void stop() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }
}


