package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.test.support.SidecarWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DocumentHydrator using WireMock gRPC container.
 * <p>
 * Tests document hydration by calling the real gRPC mock server via testcontainers.
 * The WireMock server has default test documents registered that can be hydrated.
 */
@QuarkusTest
@QuarkusTestResource(SidecarWireMockTestResource.class)
class DocumentHydratorTest {

    @Inject
    DocumentHydrator hydrator;

    @Test
    @DisplayName("Should hydrate document from repository service")
    void testHydrateDocument() {
        // The wiremock server has default test documents registered
        // test-doc-1 and test-doc-2 with account test-account
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId("test-doc-1")
                .setAccountId("test-account")
                .build();

        PipeDoc result = hydrator.hydrateDocument(docRef)
                .await().atMost(Duration.ofSeconds(10));

        assertNotNull(result);
        assertEquals("test-doc-1", result.getDocId());
    }

    @Test
    @DisplayName("Should hydrate document with source node ID")
    void testHydrateDocumentWithSourceNode() {
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId("test-doc-2")
                .setAccountId("test-account")
                .setSourceNodeId("node-parser")
                .build();

        PipeDoc result = hydrator.hydrateDocument(docRef)
                .await().atMost(Duration.ofSeconds(10));

        assertNotNull(result);
        assertEquals("test-doc-2", result.getDocId());
    }

    @Test
    @DisplayName("Should handle hydration of unknown document gracefully")
    void testHydrateUnknownDocument() {
        // Unknown documents may return an empty response or throw
        // depending on how the wiremock server is configured
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId("nonexistent-doc")
                .setAccountId("unknown-account")
                .build();

        // The wiremock server returns an empty response for unknown docs
        // (not NOT_FOUND error), so this should succeed but return empty doc
        PipeDoc result = hydrator.hydrateDocument(docRef)
                .await().atMost(Duration.ofSeconds(10));

        // Empty PipeDoc has empty docId
        assertNotNull(result);
    }
}
