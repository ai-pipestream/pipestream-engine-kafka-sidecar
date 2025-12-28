package ai.pipestream.sidecar.grpc;

import ai.pipestream.engine.v1.MutinyEngineV1ServiceGrpc;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class DefaultSidecarGrpcClientFactory implements SidecarGrpcClientFactory {

    static final String ENGINE_SERVICE_NAME = "engine";
    static final String REPO_SERVICE_NAME = "repo-service";

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @Override
    public Uni<MutinyEngineV1ServiceGrpc.MutinyEngineV1ServiceStub> engine() {
        return grpcClientFactory.getClient(ENGINE_SERVICE_NAME, MutinyEngineV1ServiceGrpc::newMutinyStub);
    }

    @Override
    public Uni<MutinyPipeDocServiceGrpc.MutinyPipeDocServiceStub> repo() {
        return grpcClientFactory.getClient(REPO_SERVICE_NAME, MutinyPipeDocServiceGrpc::newMutinyStub);
    }
}


