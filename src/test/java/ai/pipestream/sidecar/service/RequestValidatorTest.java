package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RequestValidator.
 */
class RequestValidatorTest {

    private RequestValidator validator;

    @BeforeEach
    void setUp() {
        validator = new RequestValidator();
    }

    @Test
    @DisplayName("Should validate document with complete ownership")
    void testValidDocumentWithOwnership() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-123")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("account-456")
                        .setDatasourceId("datasource-789")
                        .build())
                .build();

        assertTrue(validator.validate(doc));
    }

    @Test
    @DisplayName("Should reject document without ownership")
    void testDocumentWithoutOwnership() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-no-ownership")
                .build();

        assertFalse(validator.validate(doc));
    }

    @Test
    @DisplayName("Should reject document with empty account ID")
    void testDocumentWithEmptyAccountId() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-empty-account")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("")  // Empty
                        .setDatasourceId("datasource-789")
                        .build())
                .build();

        assertFalse(validator.validate(doc));
    }

    @Test
    @DisplayName("Should reject document with empty datasource ID")
    void testDocumentWithEmptyDatasourceId() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-empty-datasource")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("account-456")
                        .setDatasourceId("")  // Empty
                        .build())
                .build();

        assertFalse(validator.validate(doc));
    }

    @Test
    @DisplayName("Should reject document with all empty ownership fields")
    void testDocumentWithAllEmptyOwnershipFields() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-all-empty")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("")
                        .setDatasourceId("")
                        .build())
                .build();

        assertFalse(validator.validate(doc));
    }

    @Test
    @DisplayName("Should validate document with additional ownership fields")
    void testDocumentWithFullOwnership() {
        PipeDoc doc = PipeDoc.newBuilder()
                .setDocId("doc-full-ownership")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("account-456")
                        .setDatasourceId("datasource-789")
                        .setConnectorId("connector-123")
                        .build())
                .build();

        assertTrue(validator.validate(doc));
    }
}
