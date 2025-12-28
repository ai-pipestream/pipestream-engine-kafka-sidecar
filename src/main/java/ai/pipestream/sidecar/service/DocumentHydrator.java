package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.sidecar.grpc.SidecarGrpcClientFactory;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;

/**
 * Hydrates documents by fetching full content from the Repository Service.
 * <p>
 * Level 1 Hydration: Resolves a {@link DocumentReference} to a full {@link PipeDoc}
 * by calling the Repository Service's GetPipeDocByReference endpoint.
 */
@ApplicationScoped
public class DocumentHydrator {

    private static final Logger LOG = Logger.getLogger(DocumentHydrator.class);

    @Inject
    SidecarGrpcClientFactory clientFactory;

    @ConfigProperty(name = "pipestream.sidecar.hydration.max-retries", defaultValue = "3")
    int maxRetries;

    @ConfigProperty(name = "pipestream.sidecar.hydration.backoff.initial-ms", defaultValue = "25")
    long backoffInitialMs;

    @ConfigProperty(name = "pipestream.sidecar.hydration.backoff.max-ms", defaultValue = "250")
    long backoffMaxMs;

    /**
     * Hydrates a document by fetching its full content from the Repository Service.
     * <p>
     * The DocumentReference contains:
     * <ul>
     *   <li>{@code doc_id} - The logical document identifier</li>
     *   <li>{@code source_node_id} - The node that produced this version</li>
     *   <li>{@code account_id} - The account context for multi-tenant lookup</li>
     * </ul>
     *
     * @param docRef The DocumentReference from the PipeStream
     * @return A Uni resolving to the hydrated PipeDoc
     */
    public Uni<PipeDoc> hydrateDocument(DocumentReference docRef) {
        LOG.debugf("Hydrating document: docId=%s, sourceNodeId=%s, accountId=%s",
                docRef.getDocId(), docRef.getSourceNodeId(), docRef.getAccountId());

        GetPipeDocByReferenceRequest request = GetPipeDocByReferenceRequest.newBuilder()
                .setDocumentRef(docRef)
                .build();

        return clientFactory.repo()
            .flatMap(stub -> stub.getPipeDocByReference(request))
            // FR3/FR7: hydration failures should be retried with backoff (then DLQ later in the loop).
            .onFailure(this::isRetryableHydrationFailure)
            .retry()
            .withBackOff(Duration.ofMillis(backoffInitialMs), Duration.ofMillis(backoffMaxMs))
            .atMost(maxRetries)
            .map(response -> {
                PipeDoc doc = response.getPipedoc();
                LOG.debugf("Hydrated document %s (size=%d bytes, nodeId=%s)",
                        doc.getDocId(), response.getSizeBytes(), response.getNodeId());
                return doc;
            })
            .onFailure().invoke(error ->
                LOG.errorf(error, "Failed to hydrate document %s", docRef.getDocId())
            );
    }

    private boolean isRetryableHydrationFailure(Throwable t) {
        // Keep this conservative: only retry gRPC transient failures.
        if (t instanceof io.grpc.StatusRuntimeException sre) {
            io.grpc.Status.Code code = sre.getStatus().getCode();
            return code == io.grpc.Status.Code.UNAVAILABLE
                    || code == io.grpc.Status.Code.DEADLINE_EXCEEDED
                    || code == io.grpc.Status.Code.RESOURCE_EXHAUSTED;
        }
        return false;
    }
}
