package com.codeanalyser.api.service;

import com.codeanalyser.common.model.JobState;
import com.codeanalyser.common.model.JobStatus;
import com.codeanalyser.worker.state.JobProgressUpdatedEvent;
import com.codeanalyser.worker.state.JobStateService;
import com.codeanalyser.worker.state.JobStatusChangedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages SSE emitters for live job progress. One emitter per active job, stored
 * in a ConcurrentHashMap and driven by Spring application events from the worker.
 *
 * <p>All sends go through a {@code synchronized} helper because {@link SseEmitter#send}
 * is not thread-safe and events arrive from multiple threads.
 */
@Service
public class SseEmitterService {

    private static final Logger log = LoggerFactory.getLogger(SseEmitterService.class);

    private static final long EMITTER_TIMEOUT_MS = 5 * 60 * 1000L;

    private final ConcurrentHashMap<UUID, SseEmitter> emitters = new ConcurrentHashMap<>();

    private final JobStateService jobStateService;
    private final ObjectMapper    objectMapper;

    public SseEmitterService(JobStateService jobStateService, ObjectMapper objectMapper) {
        this.jobStateService = jobStateService;
        this.objectMapper    = objectMapper;
    }

    // ---------------------------------------------------------------------------
    // Client registration
    // ---------------------------------------------------------------------------

    /**
     * Registers an emitter for the given job. Sends the current state immediately
     * so the client catches up, then keeps the connection open for future events.
     * If the job is already terminal, sends a single event and completes immediately.
     */
    public SseEmitter register(UUID jobId) {
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);

        // Remove from map on completion/timeout/error to avoid stale entries.
        Runnable cleanup = () -> {
            emitters.remove(jobId, emitter);
            log.debug("SSE emitter removed for jobId={}", jobId);
        };
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(t -> cleanup.run());

        Optional<JobState> stateOpt = jobStateService.getState(jobId);

        if (stateOpt.isPresent()) {
            JobState state = stateOpt.get();

            if (state.status().isTerminal()) {
                sendStatusEvent(emitter, jobId, state.status(), state.errorMessage());
                emitter.complete();
                return emitter;
            }

            sendStatusEvent(emitter, jobId, state.status(), null);
            if (state.progressTotal() > 0) {
                sendProgressEvent(emitter, jobId, state.progressAnalysed(), state.progressTotal());
            }
        }

        emitters.put(jobId, emitter);
        log.debug("SSE emitter registered for jobId={}", jobId);
        return emitter;
    }

    // ---------------------------------------------------------------------------
    // Spring event listeners
    // ---------------------------------------------------------------------------

    /** Forwards a status transition to the connected client, closing the stream if terminal. */
    @EventListener
    public void onStatusChanged(JobStatusChangedEvent event) {
        SseEmitter emitter = emitters.get(event.jobId());
        if (emitter == null) return;

        sendStatusEvent(emitter, event.jobId(), event.status(), event.errorMessage());

        if (event.status().isTerminal()) {
            emitter.complete();
        }
    }

    /** Forwards a chunk-analysis progress tick to the connected client. */
    @EventListener
    public void onProgressUpdated(JobProgressUpdatedEvent event) {
        SseEmitter emitter = emitters.get(event.jobId());
        if (emitter == null) return;

        sendProgressEvent(emitter, event.jobId(), event.analysed(), event.total());
    }

    // ---------------------------------------------------------------------------
    // SSE send helpers
    // ---------------------------------------------------------------------------

    private void sendStatusEvent(SseEmitter emitter, UUID jobId,
                                  JobStatus status, String errorMessage) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type",   "STATUS");
        payload.put("jobId",  jobId.toString());
        payload.put("status", status.name());
        if (errorMessage != null) payload.put("errorMessage", errorMessage);
        send(emitter, "status", payload);
    }

    private void sendProgressEvent(SseEmitter emitter, UUID jobId, int analysed, int total) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type",     "PROGRESS");
        payload.put("jobId",    jobId.toString());
        payload.put("analysed", analysed);
        payload.put("total",    total);
        payload.put("percent",  total == 0 ? 0 : (int) ((analysed * 100.0) / total));
        send(emitter, "progress", payload);
    }

    // Synchronized because SseEmitter.send is not thread-safe.
    private synchronized void send(SseEmitter emitter, String eventName, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            emitter.send(SseEmitter.event().name(eventName).data(json));
        } catch (JsonProcessingException e) {
            log.error("SSE serialisation failed for event '{}': {}", eventName, e.getMessage());
        } catch (IOException e) {
            log.warn("SSE send failed for '{}' (client disconnected?): {}", eventName, e.getMessage());
            emitter.completeWithError(e);
        }
    }
}
