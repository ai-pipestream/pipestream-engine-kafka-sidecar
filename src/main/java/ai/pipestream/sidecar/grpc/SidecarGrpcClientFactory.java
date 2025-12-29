package ai.pipestream.sidecar.grpc;

import ai.pipestream.engine.v1.MutinyEngineV1ServiceGrpc;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import io.smallrye.mutiny.Uni;

/**
 * Central factory for gRPC stubs used by the sidecar.
 * <p>
 * This gives us a single override point for tests (e.g. to force WireMock host/port or a different channel impl),
 * while production can resolve via DynamicGrpc/Consul/Stork as desired.
 */
public interface SidecarGrpcClientFactory {
    Uni<MutinyEngineV1ServiceGrpc.MutinyEngineV1ServiceStub> engine();

    Uni<MutinyPipeDocServiceGrpc.MutinyPipeDocServiceStub> repo();
}



