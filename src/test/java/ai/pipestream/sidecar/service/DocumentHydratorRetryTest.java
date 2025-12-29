package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceResponse;
import ai.pipestream.repository.pipedoc.v1.PipeDocServiceGrpc;
import ai.pipestream.sidecar.util.InProcessGrpcWireMockTestResource;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.util.JsonFormat;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@QuarkusTestResource(InProcessGrpcWireMockTestResource.class)
class DocumentHydratorRetryTest {

    @Inject
    DocumentHydrator hydrator;

    @Test
    @DisplayName("Should retry hydration when repo returns UNAVAILABLE and then succeed")
    void retriesUnavailableThenSucceeds() throws Exception {
        int port = ConfigProvider.getConfig()
                .getValue("test.wiremock.grpc.port", Integer.class);
        WireMock wm = new WireMock("localhost", port);

        // Keep this fast.
        System.setProperty("pipestream.sidecar.hydration.max-retries", "3");
        System.setProperty("pipestream.sidecar.hydration.backoff.initial-ms", "1");
        System.setProperty("pipestream.sidecar.hydration.backoff.max-ms", "5");

        String serviceName = PipeDocServiceGrpc.SERVICE_NAME;
        String path = "/" + serviceName + "/GetPipeDocByReference";

        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId("retry-doc-1")
                .setAccountId("retry-account")
                .build();

        GetPipeDocByReferenceRequest request = GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(docRef)
                .build();

        PipeDoc expectedDoc = PipeDoc.newBuilder()
                .setDocId("retry-doc-1")
                .build();

        GetPipeDocByReferenceResponse okResponse = GetPipeDocByReferenceResponse.newBuilder()
                .setPipedoc(expectedDoc)
                .setNodeId("node-1")
                .setDrive("drive-1")
                .setSizeBytes(expectedDoc.getSerializedSize())
                .build();

        String requestJson = JsonFormat.printer().print(request);
        String responseJson = JsonFormat.printer().print(okResponse);

        // Fail once with HTTP 504, which the gRPC extension maps to gRPC UNAVAILABLE.
        wm.register(
                post(urlPathEqualTo(path))
                        .inScenario("hydration-retry")
                        .whenScenarioStateIs(STARTED)
                        .withRequestBody(equalToJson(requestJson, true, true))
                        .willReturn(aResponse().withStatus(504))
                        .willSetStateTo("SUCCEEDED")
        );

        // Then succeed.
        wm.register(
                post(urlPathEqualTo(path))
                        .inScenario("hydration-retry")
                        .whenScenarioStateIs("SUCCEEDED")
                        .withRequestBody(equalToJson(requestJson, true, true))
                        .willReturn(okJson(responseJson))
        );

        PipeDoc doc = hydrator.hydrateDocument(docRef)
                .await().atMost(Duration.ofSeconds(10));

        assertNotNull(doc);
        assertEquals("retry-doc-1", doc.getDocId());

        // Verify we really hit the stub twice.
        wm.verify(2, postRequestedFor(urlPathEqualTo(path)));
    }
}



