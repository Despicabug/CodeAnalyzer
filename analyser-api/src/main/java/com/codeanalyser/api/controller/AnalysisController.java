package com.codeanalyser.api.controller;

import com.codeanalyser.api.service.AnalysisService;
import com.codeanalyser.api.service.SseEmitterService;
import com.codeanalyser.common.model.JobResult;
import com.codeanalyser.common.model.JobState;
import com.codeanalyser.common.model.LlmOverride;
import com.codeanalyser.worker.aggregation.JobResultStore;
import com.codeanalyser.worker.state.JobStateService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;

/**
 * REST endpoints for submitting analysis jobs, streaming progress, and fetching results.
 *
 * <pre>
 *   POST  /api/v1/analyse            – submit a repo URL (idempotent)
 *   GET   /api/v1/jobs/{id}/status   – poll current state
 *   GET   /api/v1/jobs/{id}/stream   – SSE live progress
 *   GET   /api/v1/jobs/{id}/result   – fetch completed analysis
 * </pre>
 */
@RestController
@RequestMapping("/api/v1")
public class AnalysisController {

    private final AnalysisService   analysisService;
    private final SseEmitterService sseEmitterService;
    private final JobStateService   jobStateService;
    private final JobResultStore    jobResultStore;

    public AnalysisController(AnalysisService analysisService,
                               SseEmitterService sseEmitterService,
                               JobStateService jobStateService,
                               JobResultStore jobResultStore) {
        this.analysisService   = analysisService;
        this.sseEmitterService = sseEmitterService;
        this.jobStateService   = jobStateService;
        this.jobResultStore    = jobResultStore;
    }

    // ---------------------------------------------------------------------------
    // POST /api/v1/analyse
    // ---------------------------------------------------------------------------

    /** Submits a repo for analysis. Returns 202 for new jobs, 200 for existing ones. */
    @PostMapping("/analyse")
    public ResponseEntity<AnalyseResponse> analyse(@RequestBody AnalyseRequest request) {
        if (request.repoUrl() == null || request.repoUrl().isBlank()) {
            return ResponseEntity.badRequest().build();
        }

        // Build the optional per-job LLM override from the request.
        LlmOverride llmOverride = new LlmOverride(
                request.llmProvider(),
                request.llmApiKey(),
                request.llmModel(),
                request.llmBaseUrl()
        );

        AnalysisService.SubmitResult result = analysisService.submit(
                request.repoUrl(),
                Boolean.TRUE.equals(request.force()),
                llmOverride
        );

        AnalyseResponse body = new AnalyseResponse(
                result.jobId(),
                result.isNew(),
                result.isNew()
                        ? "Analysis submitted — connect to /api/v1/jobs/" + result.jobId() + "/stream for live updates."
                        : "Existing job found — connect to /api/v1/jobs/" + result.jobId() + "/stream for current status."
        );

        HttpStatus status = result.isNew() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(body);
    }

    // ---------------------------------------------------------------------------
    // GET /api/v1/jobs/{id}/status
    // ---------------------------------------------------------------------------

    /** Returns current job state, or 404 if unknown/expired. */
    @GetMapping("/jobs/{id}/status")
    public ResponseEntity<JobState> status(@PathVariable("id") UUID jobId) {
        return jobStateService.getState(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // ---------------------------------------------------------------------------
    // GET /api/v1/jobs/{id}/stream
    // ---------------------------------------------------------------------------

    /** Opens an SSE stream for live progress. Closes when the job reaches a terminal state. */
    @GetMapping(value = "/jobs/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@PathVariable("id") UUID jobId) {
        return sseEmitterService.register(jobId);
    }

    // ---------------------------------------------------------------------------
    // GET /api/v1/jobs/{id}/result
    // ---------------------------------------------------------------------------

    /** Returns the completed analysis result, 202 if still running, 404 if unknown. */
    @GetMapping("/jobs/{id}/result")
    public ResponseEntity<?> result(@PathVariable("id") UUID jobId) {
        return jobResultStore.find(jobId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> {
                    return jobStateService.getState(jobId)
                            .map(state -> ResponseEntity
                                    .status(HttpStatus.ACCEPTED)
                                    .<Object>body(new PendingResponse(
                                            jobId,
                                            state.status().name(),
                                            "Analysis in progress — poll this endpoint or subscribe to /stream"
                                    )))
                            .orElse(ResponseEntity.notFound().build());
                });
    }

    // ---------------------------------------------------------------------------
    // Request / Response DTOs
    // ---------------------------------------------------------------------------

    /**
     * Submission request. {@code llmProvider}, {@code llmApiKey}, {@code llmModel},
     * and {@code llmBaseUrl} are all optional — omit them to use the server's
     * configured default provider.
     */
    public record AnalyseRequest(
            String  repoUrl,
            Boolean force,
            String  llmProvider,
            String  llmApiKey,
            String  llmModel,
            String  llmBaseUrl
    ) {}

    public record AnalyseResponse(UUID jobId, boolean isNew, String message) {}
    public record PendingResponse(UUID jobId, String status, String message) {}
}
