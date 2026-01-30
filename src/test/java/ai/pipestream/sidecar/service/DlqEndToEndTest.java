package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.sidecar.v1.DlqMessage;
import ai.pipestream.test.support.SidecarWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@QuarkusTestResource(SidecarWireMockTestResource.class)
class DlqEndToEndTest {

    private static final Logger LOG = Logger.getLogger(DlqEndToEndTest.class);

    @Inject
    DlqTestConsumer dlqTestConsumer;

    @Inject
    DlqProducer dlqProducer;

    @Test
    void testDlqFlow() throws Exception {
        String inputTopic = "pipestream.test.cluster.node";
        // Dynamically determine DLQ topic using the same logic as DlqProducer
        String dlqTopic = dlqProducer.determineDlqTopic(inputTopic);
        UUID key = UUID.randomUUID();

        // 1. Prepare message
        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId("fail-doc-" + key)
                .setAccountId("acct")
                .build();
        PipeStream stream = PipeStream.newBuilder().setDocumentRef(docRef).build();
        RuntimeException error = new RuntimeException("Test failure");

        // 2. Prepare consumer expectation BEFORE sending (avoid missing messages)
        CompletableFuture<DlqMessage> future = dlqTestConsumer.await(key);

        // 3. Call DLQ producer to publish message
        LOG.infof("Sending message to DLQ via DlqProducer (inputTopic: %s, dlqTopic: %s)", inputTopic, dlqTopic);
        dlqProducer.sendToDlq(inputTopic, key, stream, error, 3, 0, 123L)
                .await().indefinitely();

        // 4. Wait for message in DLQ
        DlqMessage received = future.get(30, TimeUnit.SECONDS);
        
        assertNotNull(received, "Should have received a message in DLQ");
        assertEquals(docRef.getDocId(), received.getStream().getDocumentRef().getDocId());
        assertEquals("RuntimeException", received.getErrorType());
        assertEquals("Test failure", received.getErrorMessage());
        assertEquals(123L, received.getOriginalOffset());
        
        // No manual consumer cleanup needed - reactive messaging handles it
    }
}
