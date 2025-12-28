package ai.pipestream.sidecar.util;

import ai.pipestream.engine.v1.EngineV1ServiceGrpc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.data.v1.PipeStream;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sanity test to verify WireMock gRPC container is accessible.
 * Uses direct ManagedChannel to isolate from Quarkus gRPC client issues.
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
public class WireMockSanityTest {

    @Test
    void canCallEngineServiceOnWireMock() {
        // Read from Stork config (clients use stork://engine)
        String addressList = ConfigProvider.getConfig()
                .getOptionalValue("stork.engine.service-discovery.address-list", String.class)
                .orElse(null);

        System.out.println("Stork address list: " + addressList);
        assertNotNull(addressList, "Stork address list should be set");

        // Parse host:port from Stork address list
        String[] parts = addressList.split(":");
        assertTrue(parts.length == 2, "Address should be in format host:port");
        String host = parts[0];
        Integer port = Integer.parseInt(parts[1]);

        System.out.println("Connecting to WireMock Engine at " + host + ":" + port);

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            EngineV1ServiceGrpc.EngineV1ServiceBlockingStub stub =
                    EngineV1ServiceGrpc.newBlockingStub(channel);

            IntakeHandoffResponse response = stub.intakeHandoff(
                    IntakeHandoffRequest.newBuilder()
                            .setDatasourceId("test-datasource")
                            .setAccountId("test-account")
                            .setStream(PipeStream.newBuilder()
                                    .setStreamId("test-stream")
                                    .build())
                            .build());

            System.out.println("Got response: accepted=" + response.getAccepted() +
                    ", streamId=" + response.getAssignedStreamId());

            assertTrue(response.getAccepted());
            assertFalse(response.getAssignedStreamId().isEmpty());

        } finally {
            channel.shutdown();
        }
    }
}
