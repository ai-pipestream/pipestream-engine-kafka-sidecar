package ai.pipestream.sidecar.grpc;

import ai.pipestream.engine.v1.MutinyEngineV1ServiceGrpc;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import org.eclipse.microprofile.config.ConfigProvider;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only factory overriding gRPC stub creation to ensure deterministic connectivity to WireMock.
 * <p>
 * Uses the Stork address-list populated by WireMockTestResource to build real ManagedChannels.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class TestSidecarGrpcClientFactory implements SidecarGrpcClientFactory {

    private final AtomicReference<ManagedChannel> engineChannel = new AtomicReference<>();
    private final AtomicReference<ManagedChannel> repoChannel = new AtomicReference<>();

    @Override
    public Uni<MutinyEngineV1ServiceGrpc.MutinyEngineV1ServiceStub> engine() {
        return Uni.createFrom().item(MutinyEngineV1ServiceGrpc.newMutinyStub(getOrCreateEngineChannel()));
    }

    @Override
    public Uni<MutinyPipeDocServiceGrpc.MutinyPipeDocServiceStub> repo() {
        return Uni.createFrom().item(MutinyPipeDocServiceGrpc.newMutinyStub(getOrCreateRepoChannel()));
    }

    private ManagedChannel getOrCreateEngineChannel() {
        ManagedChannel existing = engineChannel.get();
        if (existing != null) return existing;
        ManagedChannel created = buildChannelFromStork("engine");
        engineChannel.compareAndSet(null, created);
        return engineChannel.get();
    }

    private ManagedChannel getOrCreateRepoChannel() {
        ManagedChannel existing = repoChannel.get();
        if (existing != null) return existing;
        ManagedChannel created = buildChannelFromStork("repo-service");
        repoChannel.compareAndSet(null, created);
        return repoChannel.get();
    }

    private ManagedChannel buildChannelFromStork(String storkServiceName) {
        String address = ConfigProvider.getConfig()
                .getOptionalValue("stork." + storkServiceName + ".service-discovery.address-list", String.class)
                .orElseThrow(() -> new IllegalStateException("Missing Stork address-list for " + storkServiceName));

        // address-list is configured as "host:port" for static discovery
        String[] parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalStateException("Expected stork address-list in host:port format, got: " + address);
        }
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }
}



