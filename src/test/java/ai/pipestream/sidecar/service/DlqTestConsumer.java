package ai.pipestream.sidecar.service;

import ai.pipestream.engine.sidecar.v1.DlqMessage;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class DlqTestConsumer {

    private static final Logger LOG = Logger.getLogger(DlqTestConsumer.class);

    private final ConcurrentMap<UUID, CompletableFuture<DlqMessage>> waiters = new ConcurrentHashMap<>();

    @Incoming("dlq-test")
    public void consume(Record<UUID, DlqMessage> record) {
        LOG.debugf("Received DLQ record key=%s", record.key());
        CompletableFuture<DlqMessage> waiter = waiters.remove(record.key());
        if (waiter != null) {
            waiter.complete(record.value());
        }
    }

    CompletableFuture<DlqMessage> await(UUID key) {
        CompletableFuture<DlqMessage> future = new CompletableFuture<>();
        waiters.put(key, future);
        return future;
    }
}
