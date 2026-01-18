package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.sidecar.service.ConsulLeaseManager.TopicType;
import ai.pipestream.test.support.SidecarWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(SidecarWireMockTestResource.class)
class ConsumerManagerRetryTest {

    @Inject
    ConsumerManager consumerManager;

    private static URI wireMockBaseUri() {
        var config = ConfigProvider.getConfig();
        String host = config.getValue("quarkus.grpc.clients.repo-service.host", String.class);
        int port = config.getValue("quarkus.grpc.clients.repo-service.port", Integer.class);
        return URI.create("http://" + host + ":" + port);
    }

    private static void postJson(String path, String json) throws Exception {
        URI uri = wireMockBaseUri().resolve(path);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() >= 200 && response.statusCode() < 300,
                "WireMock admin call failed: status=" + response.statusCode() + " body=" + response.body());
    }

    private static String get(String path) throws Exception {
        URI uri = wireMockBaseUri().resolve(path);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() >= 200 && response.statusCode() < 300,
                "WireMock admin GET failed: status=" + response.statusCode() + " body=" + response.body());
        return response.body();
    }

    @Test
    @DisplayName("FR7: should retry hydration when RepoService returns UNAVAILABLE then succeed")
    void shouldRetryRepoHydrationUnavailableThenSucceed() throws Exception {
        // Arrange a specific docRef
        String docId = "retry-doc-1";
        String accountId = "acct-1";
        String datasourceId = "ds-1";
        String sourceNodeId = "source-node";

        DocumentReference docRef = DocumentReference.newBuilder()
                .setDocId(docId)
                .setAccountId(accountId)
                .setSourceNodeId(sourceNodeId)
                .build();

        PipeStream stream = PipeStream.newBuilder()
                .setStreamId("stream-1")
                .setDocumentRef(docRef)
                .build();

        // WireMock gRPC methods are exposed as HTTP POST /{serviceFullName}/{methodName}
        String urlPath = "/ai.pipestream.repository.pipedoc.v1.PipeDocService/GetPipeDocByReference";

        // Match the request body on the nested document_ref fields (ignore extra elements)
        String requestMatchJson = "{\"document_ref\":{\"doc_id\":\"" + docId + "\",\"account_id\":\"" + accountId + "\"}}";

        // 1) First attempt -> UNAVAILABLE
        String firstMapping = """
                {
                  \"priority\": 1,
                  \"scenarioName\": \"cm-repo-retry\",
                  \"requiredScenarioState\": \"Started\",
                  \"newScenarioState\": \"second\",
                  \"request\": {
                    \"method\": \"POST\",
                    \"urlPath\": \"%s\",
                    \"bodyPatterns\": [
                      {
                        \"equalToJson\": \"%s\",
                        \"ignoreArrayOrder\": true,
                        \"ignoreExtraElements\": true
                      }
                    ]
                  },
                  \"response\": {
                    \"status\": 200,
                    \"headers\": {
                      \"grpc-status\": \"14\",
                      \"grpc-message\": \"Repo unavailable (test)\"
                    },
                    \"body\": \"\"
                  }
                }
                """.formatted(urlPath, requestMatchJson.replace("\"", "\\\""));

        // 2) Second attempt -> OK with a valid PipeDoc (ownership required by RequestValidator)
        String okResponseJson = """
                {
                  \"pipedoc\": {
                    \"doc_id\": \"%s\",
                    \"ownership\": {
                      \"account_id\": \"%s\",
                      \"datasource_id\": \"%s\"
                    }
                  },
                  \"node_id\": \"wiremock-node\",
                  \"size_bytes\": 123
                }
                """.formatted(docId, accountId, datasourceId);

        String secondMapping = """
                {
                  \"priority\": 1,
                  \"scenarioName\": \"cm-repo-retry\",
                  \"requiredScenarioState\": \"second\",
                  \"request\": {
                    \"method\": \"POST\",
                    \"urlPath\": \"%s\",
                    \"bodyPatterns\": [
                      {
                        \"equalToJson\": \"%s\",
                        \"ignoreArrayOrder\": true,
                        \"ignoreExtraElements\": true
                      }
                    ]
                  },
                  \"response\": {
                    \"status\": 200,
                    \"headers\": {
                      \"Content-Type\": \"application/json\",
                      \"grpc-status\": \"0\"
                    },
                    \"jsonBody\": %s
                  }
                }
                """.formatted(
                urlPath,
                requestMatchJson.replace("\"", "\\\""),
                okResponseJson
        );

        postJson("/__admin/mappings", firstMapping);
        postJson("/__admin/mappings", secondMapping);

        // Act: run the consumer processing pipeline (Kafka is disabled in %test; we call the method directly)
        consumerManager.processPipeStream(stream, TopicType.INTAKE, "intake." + datasourceId)
                .await().atMost(Duration.ofSeconds(10));

        // Assert: WireMock received at least 2 Repo calls (first failed, second succeeded)
        String requests = get("/__admin/requests");
        int occurrences = requests.split(urlPath, -1).length - 1;
        assertTrue(occurrences >= 2,
                "Expected at least 2 RepoService calls for retry; saw " + occurrences + " requests. Body=" + requests);
    }
}
