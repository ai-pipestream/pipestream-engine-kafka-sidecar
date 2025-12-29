package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.pipedoc.v1.GetPipeDocByReferenceRequest;
import ai.pipestream.sidecar.grpc.SidecarGrpcClientFactory;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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

    @Inject
    SidecarGrpcClientFactory clientFactory;

    /**
     * Hydrates a document by fetching its full content from the Repository Service.
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
