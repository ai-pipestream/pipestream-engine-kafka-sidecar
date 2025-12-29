package ai.pipestream.sidecar.grpc;

import ai.pipestream.sidecar.util.WireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class SidecarGrpcClientFactoryOverrideTest {

    @Inject
    SidecarGrpcClientFactory factory;

    @Test
    @DisplayName("Should use test override SidecarGrpcClientFactory during @QuarkusTest")
    void usesTestFactoryOverride() {
        assertInstanceOf(TestSidecarGrpcClientFactory.class, factory, "Expected TestSidecarGrpcClientFactory to be selected during tests");
    }
}



