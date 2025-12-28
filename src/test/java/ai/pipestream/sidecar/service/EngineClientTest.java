package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.engine.v1.ProcessNodeResponse;
import ai.pipestream.sidecar.util.WireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.eclipse.microprofile.config.ConfigProvider;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for EngineClient using WireMock gRPC container.
 * <p>
 * Tests IntakeHandoff and ProcessNode flows by calling the real gRPC mock server
 * via testcontainers.
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class EngineClientTest {

    @Inject
    EngineClient engineClient;

    // ============================================
    // IntakeHandoff Tests
    // ============================================

    @Test
    @DisplayName("Should successfully handoff intake document")
    void testIntakeHandoffSuccess() {
        // Diagnostic: print the resolved gRPC/Stork configuration used by this test run.
        // This helps confirm whether the injected @GrpcClient("engine") is expected to use WireMock’s mapped port.
        var cfg = ConfigProvider.getConfig();
        System.out.println("DIAG quarkus.grpc.clients.engine.host=" +
                cfg.getOptionalValue("quarkus.grpc.clients.engine.host", String.class).orElse("<unset>"));
        System.out.println("DIAG quarkus.grpc.clients.engine.port=" +
                cfg.getOptionalValue("quarkus.grpc.clients.engine.port", String.class).orElse("<unset>"));
        System.out.println("DIAG stork.engine.service-discovery.address-list=" +
                cfg.getOptionalValue("stork.engine.service-discovery.address-list", String.class).orElse("<unset>"));

        PipeDoc doc = createTestDoc("intake-doc-1", "test-account", "datasource-1");
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("original-stream-id")
                .setDocument(doc)
                .build();

        IntakeHandoffResponse response = engineClient.intakeHandoff(doc, "datasource-1", stream)
                .await().atMost(Duration.ofSeconds(10));

        assertNotNull(response);
        // The wiremock server returns default accepted response
        assertTrue(response.getAccepted());
    }

    @Test
    @DisplayName("Should handle intake handoff with full document")
    void testIntakeHandoffWithFullDocument() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("full-doc-123")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("test-account")
                        .setDatasourceId("test-datasource")
                        .setConnectorId("test-connector")
                        .build())
                .build();

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("full-stream-id")
                .setDocument(doc)
                .build();

        IntakeHandoffResponse response = engineClient.intakeHandoff(doc, "test-datasource", stream)
                .await().atMost(Duration.ofSeconds(10));

        assertNotNull(response);
        assertTrue(response.getAccepted());
    }

    // ============================================
    // ProcessNode Tests
    // ============================================

    @Test
    @DisplayName("Should successfully process node")
    void testProcessNodeSuccess() {
        PipeDoc doc = createTestDoc("process-doc-1", "test-account", "datasource-1");
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("process-stream-id")
                .setCurrentNodeId("node-chunker")
                .setDocument(doc)
                .build();

        ProcessNodeResponse response = engineClient.processNode(doc, "node-chunker", stream)
                .await().atMost(Duration.ofSeconds(10));

        assertNotNull(response);
        // The wiremock server returns default success response
        assertTrue(response.getSuccess());
    }

    @Test
    @DisplayName("Should process node with different target nodes")
    void testProcessNodeWithDifferentTargets() {
        PipeDoc doc = createTestDoc("multi-node-doc", "test-account", "datasource-1");

        // Process at first node
        PipeStream stream1 = PipeStream.newBuilder()
                .setStreamId("stream-1")
                .setCurrentNodeId("tika-parser")
                .setDocument(doc)
                .build();

        ProcessNodeResponse response1 = engineClient.processNode(doc, "tika-parser", stream1)
                .await().atMost(Duration.ofSeconds(10));
        assertTrue(response1.getSuccess());

        // Process at second node
        PipeStream stream2 = PipeStream.newBuilder()
                .setStreamId("stream-2")
                .setCurrentNodeId("text-chunker")
                .setDocument(doc)
                .build();

        ProcessNodeResponse response2 = engineClient.processNode(doc, "text-chunker", stream2)
                .await().atMost(Duration.ofSeconds(10));
        assertTrue(response2.getSuccess());
    }

    // ============================================
    // End-to-End Workflow Tests
    // ============================================

    @Test
    @DisplayName("Should support complete intake-to-processing workflow")
    void testCompleteWorkflow() {
        // 1. Intake handoff
        PipeDoc doc = createTestDoc("workflow-doc", "test-account", "workflow-datasource");
        PipeStream intakeStream = PipeStream.newBuilder()
                .setStreamId("intake-stream")
                .setDocument(doc)
                .build();

        IntakeHandoffResponse intakeResponse = engineClient.intakeHandoff(doc, "workflow-datasource", intakeStream)
                .await().atMost(Duration.ofSeconds(10));

        assertTrue(intakeResponse.getAccepted());

        // 2. Process at first node (simulating what engine would route to)
        PipeStream processStream = PipeStream.newBuilder()
                .setStreamId(intakeResponse.getAssignedStreamId().isEmpty()
                        ? "assigned-stream"
                        : intakeResponse.getAssignedStreamId())
                .setCurrentNodeId("entry-parser")
                .setDocument(doc)
                .build();

        ProcessNodeResponse processResponse = engineClient.processNode(doc, "entry-parser", processStream)
                .await().atMost(Duration.ofSeconds(10));

        assertTrue(processResponse.getSuccess());
    }

    // ============================================
    // Helper Methods
    // ============================================

    private PipeDoc createTestDoc(String docId, String accountId, String datasourceId) {
        return PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(datasourceId)
                        .build())
                .build();
    }
}
