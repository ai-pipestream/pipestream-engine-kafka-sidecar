package ai.pipestream.sidecar;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.jboss.logging.Logger;

/**
 * Main entry point for the Kafka Sidecar application.
 * <p>
 * The Kafka Sidecar is responsible for:
 * <ul>
 *   <li>Consuming documents from Kafka topics based on dynamic Consul leases</li>
 *   <li>Hydrating documents by fetching blobs from RepoService</li>
 *   <li>Handing off fully hydrated documents to the Engine for processing</li>
 * </ul>
 * <p>
 * Architecture:
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                      Kafka Sidecar                              │
 * │                                                                 │
 * │  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
 * │  │    Kafka     │───>│   Hydrator   │───>│  Engine Client   │  │
 * │  │   Consumer   │    │  (RepoSvc)   │    │   (gRPC)         │  │
 * │  └──────────────┘    └──────────────┘    └──────────────────┘  │
 * │         ▲                                                       │
 * │         │                                                       │
 * │  ┌──────────────┐                                              │
 * │  │    Consul    │  (Dynamic topic subscription)                │
 * │  │    Leases    │                                              │
 * │  └──────────────┘                                              │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 */
@QuarkusMain
public class KafkaSidecarApplication implements QuarkusApplication {

    private static final Logger LOG = Logger.getLogger(KafkaSidecarApplication.class);

    /**
     * Default constructor for KafkaSidecarApplication.
     */
    public KafkaSidecarApplication() {
    }

    /**
     * Main entry point for the Quarkus application.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        Quarkus.run(KafkaSidecarApplication.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        LOG.info("Starting Pipestream Engine Kafka Sidecar");
        LOG.info("Sidecar responsibilities: Kafka consumption, document hydration, engine handoff");
        Quarkus.waitForExit();
        return 0;
    }
}
