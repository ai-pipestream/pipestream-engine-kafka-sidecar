package ai.pipestream.sidecar.service;

import ai.pipestream.data.v1.PipeDoc;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
public class RequestValidator {

    private static final Logger LOG = Logger.getLogger(RequestValidator.class);

    /**
     * Validates that the PipeDoc has necessary ownership information.
     *
     * @param pipeDoc The document to validate.
     * @return true if valid, false otherwise.
     */
    public boolean validate(PipeDoc pipeDoc) {
        if (!pipeDoc.hasOwnership()) {
            LOG.warnf("Document %s missing ownership context", pipeDoc.getDocId());
            return false;
        }

        var ownership = pipeDoc.getOwnership();
        if (ownership.getAccountId().isEmpty()) {
            LOG.warnf("Document %s missing account ID", pipeDoc.getDocId());
            return false;
        }
        if (ownership.getDatasourceId().isEmpty()) {
            LOG.warnf("Document %s missing datasource ID", pipeDoc.getDocId());
            return false;
        }

        return true;
    }
}
