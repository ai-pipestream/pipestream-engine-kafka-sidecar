package ai.pipestream.sidecar.service;

import ai.pipestream.engine.sidecar.v1.GetLeasesRequest;
import ai.pipestream.engine.sidecar.v1.GetLeasesResponse;
import ai.pipestream.engine.sidecar.v1.LeaseInfo;
import ai.pipestream.engine.sidecar.v1.MutinySidecarManagementServiceGrpc;
import ai.pipestream.engine.sidecar.v1.PauseConsumptionRequest;
import ai.pipestream.engine.sidecar.v1.PauseConsumptionResponse;
import ai.pipestream.engine.sidecar.v1.ResumeConsumptionRequest;
import ai.pipestream.engine.sidecar.v1.ResumeConsumptionResponse;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

@GrpcService
public class SidecarManagementServiceImpl extends MutinySidecarManagementServiceGrpc.SidecarManagementServiceImplBase {

    private static final Logger LOG = Logger.getLogger(SidecarManagementServiceImpl.class);

    @Inject
    ConsumerManager consumerManager;

    @Override
    public Uni<GetLeasesResponse> getLeases(GetLeasesRequest request) {
        return Uni.createFrom().item(() -> {
            List<LeaseInfo> leases = new ArrayList<>();
            var topics = consumerManager.getSubscribedTopicsWithTypes();
            
            topics.forEach((topic, type) -> {
                boolean isPaused = consumerManager.isTopicPaused(topic);
                leases.add(LeaseInfo.newBuilder()
                        .setTopic(topic)
                        .setType(type.name())
                        .setStatus(isPaused ? "PAUSED" : "ACTIVE")
                        .setCurrentOffset(-1) // Not easily retrievable in sync way, skipping for MVP
                        .setRetryCount(0) // Not exposed by ConsumerManager yet
                        .build());
            });

            return GetLeasesResponse.newBuilder()
                    .addAllLeases(leases)
                    .build();
        });
    }

    @Override
    public Uni<PauseConsumptionResponse> pauseConsumption(PauseConsumptionRequest request) {
        return Uni.createFrom().item(() -> {
            String topic = request.getTopic();
            LOG.infof("Received request to pause consumption for topic: %s", topic);
            
            consumerManager.pauseTopic(topic);
            
            return PauseConsumptionResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Pause requested for topic " + topic)
                    .build();
        });
    }

    @Override
    public Uni<ResumeConsumptionResponse> resumeConsumption(ResumeConsumptionRequest request) {
         return Uni.createFrom().item(() -> {
            String topic = request.getTopic();
            LOG.infof("Received request to resume consumption for topic: %s", topic);
            
            consumerManager.resumeTopic(topic);
            
            return ResumeConsumptionResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Resume requested for topic " + topic)
                    .build();
        });
    }
}
