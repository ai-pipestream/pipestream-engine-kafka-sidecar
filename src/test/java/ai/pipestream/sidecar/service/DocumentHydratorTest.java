package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceResponse;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for DocumentHydrator using mocked gRPC client.
 */
@ExtendWith(MockitoExtension.class)
class DocumentHydratorTest {

    @Mock
    private MutinyPipeDocServiceGrpc.MutinyPipeDocServiceStub repoClient;

    private DocumentHydrator hydrator;

    @BeforeEach
    void setUp() throws Exception {
        hydrator = new DocumentHydrator();
        // Inject mock using reflection
        Field field = DocumentHydrator.class.getDeclaredField("repoClient");
        field.setAccessible(true);
        field.set(hydrator, repoClient);
    }

    @Test
    @DisplayName("Should hydrate document successfully")
    void testHydrateDocumentSuccess() {
        // Arrange
        String docId = "doc-123";
        String accountId = "account-456";
        String nodeId = "node-parser";
        String drive = "default-drive";

        PipeDoc expectedDoc = PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId("datasource-001")
                        .build())
                .build();

        GetPipeDocByReferenceResponse response = GetPipeDocByReferenceResponse.newBuilder()
                .setPipedoc(expectedDoc)
                .setNodeId(nodeId)
                .setDrive(drive)
                .setSizeBytes(expectedDoc.getSerializedSize())
                .build();

        when(repoClient.getPipeDocByReference(any(GetPipeDocByReferenceRequest.class)))
                .thenReturn(Uni.createFrom().item(response));

        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .build();

        // Act
        PipeDoc result = hydrator.hydrateDocument(docRef)
                .await().indefinitely();

        // Assert
        assertNotNull(result);
        assertEquals(docId, result.getDocId());
        assertTrue(result.hasOwnership());
        assertEquals(accountId, result.getOwnership().getAccountId());
    }

    @Test
    @DisplayName("Should propagate error on hydration failure")
    void testHydrateDocumentFailure() {
        // Arrange
        when(repoClient.getPipeDocByReference(any(GetPipeDocByReferenceRequest.class)))
                .thenReturn(Uni.createFrom().failure(new RuntimeException("Connection failed")));

        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId("failing-doc")
                .setAccountId("account-123")
                .build();

        // Act & Assert
        assertThrows(RuntimeException.class, () ->
                hydrator.hydrateDocument(docRef).await().indefinitely()
        );
    }

    @Test
    @DisplayName("Should hydrate document with all reference fields")
    void testHydrateDocumentWithFullReference() {
        // Arrange
        String docId = "doc-full";
        String accountId = "account-full";
        String sourceNodeId = "source-node";
        String nodeId = "storage-node";

        PipeDoc expectedDoc = PipeDoc.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId("datasource-full")
                        .setConnectorId("connector-001")
                        .build())
                .build();

        GetPipeDocByReferenceResponse response = GetPipeDocByReferenceResponse.newBuilder()
                .setPipedoc(expectedDoc)
                .setNodeId(nodeId)
                .setDrive("production-drive")
                .setSizeBytes(1024)
                .build();

        when(repoClient.getPipeDocByReference(any(GetPipeDocByReferenceRequest.class)))
                .thenReturn(Uni.createFrom().item(response));

        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .setSourceNodeId(sourceNodeId)
                .build();

        // Act
        PipeDoc result = hydrator.hydrateDocument(docRef)
                .await().indefinitely();

        // Assert
        assertNotNull(result);
        assertEquals(docId, result.getDocId());
        assertEquals("datasource-full", result.getOwnership().getDatasourceId());
        assertEquals("connector-001", result.getOwnership().getConnectorId());
    }
}
