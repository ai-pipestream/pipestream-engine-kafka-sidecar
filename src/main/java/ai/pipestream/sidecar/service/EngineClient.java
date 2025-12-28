package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.engine.v1.MutinyEngineV1ServiceGrpc;
import ai.pipestream.engine.v1.ProcessNodeRequest;
import ai.pipestream.engine.v1.ProcessNodeResponse;
import ai.pipestream.sidecar.grpc.SidecarGrpcClientFactory;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * gRPC client for communicating with the Pipestream Engine.
 * <p>
 * Provides methods for:
 * <ul>
 *   <li>{@link #intakeHandoff} - For intake topic documents (new documents entering the pipeline)</li>
 *   <li>{@link #processNode} - For node topic documents (documents moving between nodes)</li>
 * </ul>
 */
@ApplicationScoped
public class EngineClient {

    private static final Logger LOG = Logger.getLogger(EngineClient.class);

    @Inject
    SidecarGrpcClientFactory clientFactory;

    /**
     * Hands off a document from an intake topic to the Engine.
     * <p>
     * Used for new documents entering the pipeline via intake topics.
     * The Engine will route the document to the appropriate entry node
     * based on the datasource configuration.
     *
     * @param hydratedDoc The fully hydrated PipeDoc
     * @param datasourceId The datasource identifier (extracted from topic name)
     * @param originalStream The original PipeStream from Kafka (preserves stream_id and context)
     * @return A Uni resolving to the IntakeHandoffResponse
     */
    public Uni<IntakeHandoffResponse> intakeHandoff(PipeDoc hydratedDoc, String datasourceId,
                                                     PipeStream originalStream) {
        LOG.debugf("Intake handoff for document %s, datasource=%s, streamId=%s",
                hydratedDoc.getDocId(), datasourceId, originalStream.getStreamId());

        // Update the original stream with the hydrated document (replaces document_ref with inline doc)
        PipeStream stream = originalStream.toBuilder()
            .setDocument(hydratedDoc)
            .build();

        String accountId = hydratedDoc.hasOwnership()
            ? hydratedDoc.getOwnership().getAccountId()
            : "";

        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setStream(stream)
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .build();

        return clientFactory.engine()
            .flatMap(stub -> stub.intakeHandoff(request))
            .invoke(response -> {
                if (response.getAccepted()) {
                    LOG.infof("Intake handoff accepted for doc %s, streamId=%s, entryNode=%s",
                            hydratedDoc.getDocId(),
                            response.getAssignedStreamId(),
                            response.getEntryNodeId());
                } else {
                    LOG.warnf("Intake handoff rejected for doc %s: %s",
                            hydratedDoc.getDocId(), response.getMessage());
                }
            })
            .onFailure().invoke(error ->
                LOG.errorf(error, "Intake handoff failed for doc %s", hydratedDoc.getDocId())
            );
    }

    /**
     * Submits a document to the Engine for processing at a specific node.
     * <p>
     * Used for documents moving between nodes via node topics.
     * The Engine will process the document at the specified target node.
     *
     * @param hydratedDoc The fully hydrated PipeDoc
     * @param targetNodeId The target node identifier
     * @param originalStream The original PipeStream (for preserving execution context)
     * @return A Uni resolving to the ProcessNodeResponse
     */
    public Uni<ProcessNodeResponse> processNode(PipeDoc hydratedDoc, String targetNodeId,
                                                  PipeStream originalStream) {
        LOG.debugf("Process node for document %s, targetNode=%s, streamId=%s",
                hydratedDoc.getDocId(), targetNodeId, originalStream.getStreamId());

        // Update the stream with the hydrated document (inline)
        PipeStream stream = originalStream.toBuilder()
            .setDocument(hydratedDoc) // Hydrated from repo, now inline
            .setCurrentNodeId(targetNodeId)
            .build();

        ProcessNodeRequest request = ProcessNodeRequest.newBuilder()
                .setStream(stream)
                .build();

        return clientFactory.engine()
            .flatMap(stub -> stub.processNode(request))
            .invoke(response -> {
                if (response.getSuccess()) {
                    LOG.infof("ProcessNode succeeded for doc %s at node %s",
                            hydratedDoc.getDocId(), targetNodeId);
                } else {
                    LOG.warnf("ProcessNode failed for doc %s at node %s: %s",
                            hydratedDoc.getDocId(), targetNodeId, response.getMessage());
                }
            })
            .onFailure().invoke(error ->
                LOG.errorf(error, "ProcessNode failed for doc %s at node %s",
                        hydratedDoc.getDocId(), targetNodeId)
            );
    }
}
