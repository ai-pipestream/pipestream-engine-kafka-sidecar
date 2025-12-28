package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.*;
import ai.pipestream.wiremock.client.EngineV1ServiceMock;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.*;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for EngineClient using WireMock gRPC mocks.
 * <p>
 * Tests IntakeHandoff and ProcessNode flows by mocking the Engine service.
 */
class EngineClientTest {

    private static WireMockServer wireMockServer;
    private static WireMock wireMock;
    private static ManagedChannel channel;
    private static MutinyEngineV1ServiceGrpc.MutinyEngineV1ServiceStub mutinyStub;
    private EngineV1ServiceMock engineMock;

    @BeforeAll
    static void setUp() {
        // Start WireMock server with gRPC extension
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .extensions(new GrpcExtensionFactory())
                .withRootDirectory("build/resources/test/wiremock"));
        wireMockServer.start();

        wireMock = new WireMock(wireMockServer.port());

        // Create gRPC channel
        channel = ManagedChannelBuilder.forAddress("localhost", wireMockServer.port())
                .usePlaintext()
                .build();

        // Create Mutiny stub for reactive calls
        mutinyStub = MutinyEngineV1ServiceGrpc.newMutinyStub(channel);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void resetMocks() {
        engineMock = new EngineV1ServiceMock(wireMock);
    }

    // ============================================
    // IntakeHandoff Tests
    // ============================================

    @Test
    @DisplayName("Should successfully handoff intake document")
    void testIntakeHandoffSuccess() {
        // Arrange
        String assignedStreamId = "stream-assigned-123";
        String entryNodeId = "entry-parser";
        engineMock.mockIntakeHandoffAccepted(assignedStreamId, entryNodeId);

        PipeDoc doc = createTestDoc("intake-doc-1", "account-1", "datasource-1");
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("original-stream-id")
                .setDocument(doc)
                .build();

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(stream)
                .setDatasourceId("datasource-1")
                .setAccountId("account-1")
                .build();

        // Act
        var response = mutinyStub.intakeHandoff(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertNotNull(response);
        assertTrue(response.getAccepted());
        assertEquals(assignedStreamId, response.getAssignedStreamId());
        assertEquals(entryNodeId, response.getEntryNodeId());
        assertEquals("Stream accepted for processing", response.getMessage());
    }

    @Test
    @DisplayName("Should handle rejected intake handoff")
    void testIntakeHandoffRejected() {
        // Arrange
        String rejectMessage = "Datasource not configured";
        engineMock.mockIntakeHandoffRejected(rejectMessage);

        PipeDoc doc = createTestDoc("rejected-doc", "account-1", "unknown-datasource");
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("rejected-stream-id")
                .setDocument(doc)
                .build();

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(stream)
                .setDatasourceId("unknown-datasource")
                .setAccountId("account-1")
                .build();

        // Act
        var response = mutinyStub.intakeHandoff(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertNotNull(response);
        assertFalse(response.getAccepted());
        assertEquals(rejectMessage, response.getMessage());
    }

    @Test
    @DisplayName("Should handle UNAVAILABLE error for intake handoff")
    void testIntakeHandoffUnavailable() {
        // Arrange
        engineMock.mockIntakeHandoffUnavailable();

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("test-stream").build())
                .setDatasourceId("test-datasource")
                .build();

        // Act & Assert
        UniAssertSubscriber<IntakeHandoffResponse> subscriber =
                mutinyStub.intakeHandoff(request)
                        .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure(Duration.ofSeconds(5));
        Throwable failure = subscriber.getFailure();
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals(io.grpc.Status.Code.UNAVAILABLE,
                ((StatusRuntimeException) failure).getStatus().getCode());
    }

    @Test
    @DisplayName("Should handle queue full rejection")
    void testIntakeHandoffQueueFull() {
        // Arrange
        long queueDepth = 10000;
        engineMock.mockIntakeHandoffQueueFull(queueDepth);

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("queued-stream").build())
                .setDatasourceId("busy-datasource")
                .build();

        // Act
        var response = mutinyStub.intakeHandoff(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertFalse(response.getAccepted());
        assertTrue(response.getMessage().contains("Queue full"));
        assertEquals(queueDepth, response.getQueueDepth());
    }

    // ============================================
    // ProcessNode Tests
    // ============================================

    @Test
    @DisplayName("Should successfully process node")
    void testProcessNodeSuccess() {
        // Arrange
        engineMock.mockProcessNodeSuccess();

        PipeDoc doc = createTestDoc("process-doc-1", "account-1", "datasource-1");
        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("process-stream-id")
                .setCurrentNodeId("node-chunker")
                .setDocument(doc)
                .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        // Act
        var response = mutinyStub.processNode(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertNotNull(response);
        assertTrue(response.getSuccess());
        assertEquals("Node processed successfully", response.getMessage());
    }

    @Test
    @DisplayName("Should process node and return updated stream")
    void testProcessNodeWithUpdatedStream() {
        // Arrange
        PipeStream updatedStream = PipeStream.newBuilder()
                .setStreamId("updated-stream-after-processing")
                .setCurrentNodeId("node-embedder")
                .setDocument(PipeDoc.newBuilder()
                        .setDocId("processed-doc")
                        .build())
                .build();

        engineMock.mockProcessNodeSuccess(updatedStream);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder()
                        .setStreamId("original-stream")
                        .setCurrentNodeId("node-chunker")
                        .build())
                .build();

        // Act
        var response = mutinyStub.processNode(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertTrue(response.getSuccess());
        assertTrue(response.hasUpdatedStream());
        assertEquals("updated-stream-after-processing", response.getUpdatedStream().getStreamId());
        assertEquals("node-embedder", response.getUpdatedStream().getCurrentNodeId());
    }

    @Test
    @DisplayName("Should process node and return metrics")
    void testProcessNodeWithMetrics() {
        // Arrange
        PipeStream updatedStream = PipeStream.newBuilder()
                .setStreamId("metrics-stream")
                .build();

        ProcessingMetrics metrics = ProcessingMetrics.newBuilder()
                .setProcessingTimeMs(150)
                .setNodeId("node-parser")
                .setModuleId("tika-module")
                .setCacheHit(false)
                .setHopCount(2)
                .build();

        engineMock.mockProcessNodeSuccessWithMetrics(updatedStream, metrics);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("original").build())
                .build();

        // Act
        var response = mutinyStub.processNode(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertTrue(response.getSuccess());
        assertTrue(response.hasMetrics());
        assertEquals(150, response.getMetrics().getProcessingTimeMs());
        assertEquals("node-parser", response.getMetrics().getNodeId());
        assertEquals("tika-module", response.getMetrics().getModuleId());
        assertFalse(response.getMetrics().getCacheHit());
        assertEquals(2, response.getMetrics().getHopCount());
    }

    @Test
    @DisplayName("Should handle node processing failure")
    void testProcessNodeFailure() {
        // Arrange
        String errorMessage = "Module failed to parse document";
        engineMock.mockProcessNodeFailure(errorMessage);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("failing-stream").build())
                .build();

        // Act
        var response = mutinyStub.processNode(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertFalse(response.getSuccess());
        assertEquals(errorMessage, response.getMessage());
    }

    @Test
    @DisplayName("Should handle UNAVAILABLE error for process node")
    void testProcessNodeUnavailable() {
        // Arrange
        engineMock.mockProcessNodeUnavailable();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("unavailable-stream").build())
                .build();

        // Act & Assert
        UniAssertSubscriber<ProcessNodeResponse> subscriber =
                mutinyStub.processNode(request)
                        .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure(Duration.ofSeconds(5));
        Throwable failure = subscriber.getFailure();
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals(io.grpc.Status.Code.UNAVAILABLE,
                ((StatusRuntimeException) failure).getStatus().getCode());
    }

    @Test
    @DisplayName("Should handle INTERNAL error for process node")
    void testProcessNodeInternalError() {
        // Arrange
        String errorMessage = "Database connection failed";
        engineMock.mockProcessNodeInternalError(errorMessage);

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("error-stream").build())
                .build();

        // Act & Assert
        UniAssertSubscriber<ProcessNodeResponse> subscriber =
                mutinyStub.processNode(request)
                        .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber.awaitFailure(Duration.ofSeconds(5));
        Throwable failure = subscriber.getFailure();
        assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals(io.grpc.Status.Code.INTERNAL,
                ((StatusRuntimeException) failure).getStatus().getCode());
        assertTrue(failure.getMessage().contains(errorMessage));
    }

    // ============================================
    // End-to-End Workflow Tests
    // ============================================

    @Test
    @DisplayName("Should support complete intake-to-processing workflow")
    void testCompleteWorkflow() {
        // 1. Intake handoff
        String assignedStreamId = "workflow-stream";
        String entryNodeId = "entry-parser";
        engineMock.mockIntakeHandoffAccepted(assignedStreamId, entryNodeId);

        PipeDoc doc = createTestDoc("workflow-doc", "account-wf", "datasource-wf");
        PipeStream intakeStream = PipeStream.newBuilder()
                .setStreamId("intake-stream")
                .setDocument(doc)
                .build();

        IntakeHandoffRequest intakeRequest = IntakeHandoffRequest.newBuilder()
                .setStream(intakeStream)
                .setDatasourceId("datasource-wf")
                .setAccountId("account-wf")
                .build();

        var intakeResponse = mutinyStub.intakeHandoff(intakeRequest)
                .await().atMost(Duration.ofSeconds(5));

        assertTrue(intakeResponse.getAccepted());
        assertEquals(assignedStreamId, intakeResponse.getAssignedStreamId());

        // 2. Process at first node
        ProcessingMetrics metrics = ProcessingMetrics.newBuilder()
                .setProcessingTimeMs(100)
                .setNodeId(entryNodeId)
                .setHopCount(1)
                .build();

        PipeStream processedStream = PipeStream.newBuilder()
                .setStreamId(assignedStreamId)
                .setCurrentNodeId("node-chunker")
                .setDocument(doc)
                .build();

        engineMock.mockProcessNodeSuccessWithMetrics(processedStream, metrics);

        ProcessNodeRequest processRequest = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder()
                        .setStreamId(assignedStreamId)
                        .setCurrentNodeId(entryNodeId)
                        .setDocument(doc)
                        .build())
                .build();

        var processResponse = mutinyStub.processNode(processRequest)
                .await().atMost(Duration.ofSeconds(5));

        assertTrue(processResponse.getSuccess());
        assertTrue(processResponse.hasMetrics());
        assertEquals(100, processResponse.getMetrics().getProcessingTimeMs());
    }

    @Test
    @DisplayName("Should support retry after failure")
    void testRetryAfterFailure() {
        // First attempt: UNAVAILABLE
        engineMock.mockProcessNodeUnavailable();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(PipeStream.newBuilder().setStreamId("retry-stream").build())
                .build();

        // First attempt should fail
        UniAssertSubscriber<ProcessNodeResponse> subscriber1 =
                mutinyStub.processNode(request)
                        .subscribe().withSubscriber(UniAssertSubscriber.create());

        subscriber1.awaitFailure(Duration.ofSeconds(5));
        assertInstanceOf(StatusRuntimeException.class, subscriber1.getFailure());

        // Reset and configure for success
        engineMock.reset();
        engineMock.mockProcessNodeSuccess();

        // Second attempt should succeed
        var response = mutinyStub.processNode(request)
                .await().atMost(Duration.ofSeconds(5));

        assertTrue(response.getSuccess());
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
