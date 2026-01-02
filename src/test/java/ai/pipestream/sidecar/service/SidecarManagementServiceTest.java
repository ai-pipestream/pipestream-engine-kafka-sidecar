package ai.pipestream.sidecar.service;

import ai.pipestream.engine.sidecar.v1.*;
import ai.pipestream.sidecar.service.ConsulLeaseManager.TopicType;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for SidecarManagementService.
 * Verifies that the management API can query and control the ConsumerManager.
 */
@QuarkusTest
class SidecarManagementServiceTest {

    @GrpcClient
    MutinySidecarManagementServiceGrpc.MutinySidecarManagementServiceStub managementClient;

    @Inject
    ConsumerManager consumerManager;

    @Test
    @DisplayName("Should return active leases")
    void testGetLeases() {
        // Add a dummy topic to consumer manager
        consumerManager.addTopic("test-topic-1", TopicType.INTAKE);

        GetLeasesResponse response = managementClient.getLeases(GetLeasesRequest.newBuilder().build())
                .await().atMost(Duration.ofSeconds(5));

        assertNotNull(response);
        assertFalse(response.getLeasesList().isEmpty());
        
        boolean found = response.getLeasesList().stream()
                .anyMatch(l -> l.getTopic().equals("test-topic-1") && l.getType().equals("INTAKE"));
        
        assertTrue(found, "Should find the added test topic in leases");
    }

    @Test
    @DisplayName("Should pause and resume topic consumption")
    void testPauseResume() {
        String topic = "pause-topic-test";
        consumerManager.addTopic(topic, TopicType.NODE);
        
        // Initial state
        assertFalse(consumerManager.isTopicPaused(topic));

        // Pause via gRPC
        PauseConsumptionResponse pauseResponse = managementClient.pauseConsumption(
                PauseConsumptionRequest.newBuilder().setTopic(topic).build())
                .await().atMost(Duration.ofSeconds(5));
        
        assertTrue(pauseResponse.getSuccess());
        assertTrue(consumerManager.isTopicPaused(topic));
        
        // Verify via GetLeases
        GetLeasesResponse leasesResponse = managementClient.getLeases(GetLeasesRequest.newBuilder().build())
                .await().atMost(Duration.ofSeconds(5));
        
        boolean isPaused = leasesResponse.getLeasesList().stream()
                .filter(l -> l.getTopic().equals(topic))
                .map(l -> l.getStatus().equals("PAUSED"))
                .findFirst()
                .orElse(false);
        
        assertTrue(isPaused, "Topic should be reported as PAUSED in GetLeases");

        // Resume via gRPC
        ResumeConsumptionResponse resumeResponse = managementClient.resumeConsumption(
                ResumeConsumptionRequest.newBuilder().setTopic(topic).build())
                .await().atMost(Duration.ofSeconds(5));
        
        assertTrue(resumeResponse.getSuccess());
        assertFalse(consumerManager.isTopicPaused(topic));
    }
}
