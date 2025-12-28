package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.repository.pipedoc.v1.MutinyPipeDocServiceGrpc;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Hydrates documents by fetching full content from the Repository Service.
 * <p>
 * Level 1 Hydration: Resolves a {@link DocumentReference} to a full {@link PipeDoc}
 * by calling the Repository Service's GetPipeDocByReference endpoint.
 */
@ApplicationScoped
public class DocumentHydrator {

    private static final Logger LOG = Logger.getLogger(DocumentHydrator.class);

    @GrpcClient("repo-service")
    MutinyPipeDocServiceGrpc.MutinyPipeDocServiceStub repoClient;

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

        return repoClient.getPipeDocByReference(
                GetPipeDocByReferenceRequest.newBuilder()
                    .setDocumentRef(docRef)
                    .build())
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
}
