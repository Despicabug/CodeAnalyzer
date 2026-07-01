package com.codeanalyser.api.service;

import com.codeanalyser.common.model.JobState;
import com.codeanalyser.common.model.JobStatus;
import com.codeanalyser.common.model.LlmOverride;
import com.codeanalyser.common.model.RepoJob;
import com.codeanalyser.worker.config.IngestionProperties;
import com.codeanalyser.worker.ingestion.IngestionPipeline;
import com.codeanalyser.worker.state.JobStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Accepts analysis requests, applies an idempotency check, and dispatches the
 * ingestion pipeline asynchronously.
 *
 * <p>Idempotency: {@code job:latest:{repoUrl}} in Redis holds the most recent
 * job ID for a given URL. Re-submitting the same URL returns the existing ID
 * unless {@code force=true} is passed.
 *
 * <p>URL normalisation strips trailing slashes and {@code .git} suffixes so
 * {@code github.com/foo/bar} and {@code github.com/foo/bar.git} map to the same key.
 *
 * <p>When an {@link LlmOverride} is provided, it is stored in the job's Redis
 * state hash so that workers can build a per-job {@link com.codeanalyser.worker.llm.LlmClient}
 * using the caller's own API key.
 */
@Service
public class AnalysisService {

    private static final Logger log = LoggerFactory.getLogger(AnalysisService.class);

    private static final Duration LATEST_JOB_TTL = Duration.ofHours(72);

    private final StringRedisTemplate redisTemplate;
    private final JobStateService     jobStateService;
    private final IngestionPipeline   ingestionPipeline;
    private final IngestionProperties props;
    private final Executor            analysisExecutor;

    public AnalysisService(
            StringRedisTemplate redisTemplate,
            JobStateService jobStateService,
            IngestionPipeline ingestionPipeline,
            IngestionProperties props,
            @Qualifier("analysisExecutor") Executor analysisExecutor) {
        this.redisTemplate     = redisTemplate;
        this.jobStateService   = jobStateService;
        this.ingestionPipeline = ingestionPipeline;
        this.props             = props;
        this.analysisExecutor  = analysisExecutor;
    }

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /** Submits a job using the server's configured LLM provider. */
    public SubmitResult submit(String repoUrl, boolean force) {
        return submit(repoUrl, force, null);
    }

    /**
     * Submits a job, optionally using the caller's own LLM credentials.
     * When {@code llmOverride} is non-null and {@link LlmOverride#isPresent()},
     * the override is stored in Redis and workers will use it instead of the
     * global provider.
     */
    public SubmitResult submit(String repoUrl, boolean force, LlmOverride llmOverride) {
        String normalizedUrl = normalizeUrl(repoUrl);

        if (!force) {
            String latestKey     = JobState.latestJobKey(normalizedUrl);
            String existingIdStr = redisTemplate.opsForValue().get(latestKey);
            if (existingIdStr != null) {
                UUID existingJobId = UUID.fromString(existingIdStr);
                log.info("Idempotency hit for '{}' → existing jobId={}", normalizedUrl, existingJobId);
                return new SubmitResult(existingJobId, false);
            }
        }

        RepoJob job = RepoJob.builder(normalizedUrl)
                .maxRetries(props.maxRetries())
                .build();

        // Persist initial state, including any per-job LLM override.
        jobStateService.createJob(job.jobId(), normalizedUrl,
                (llmOverride != null && llmOverride.isPresent()) ? llmOverride : null);

        redisTemplate.opsForValue().set(
                JobState.latestJobKey(normalizedUrl),
                job.jobId().toString(),
                LATEST_JOB_TTL
        );

        log.info("Submitting new job {} for '{}' (llmOverride={})",
                job.jobId(), normalizedUrl,
                llmOverride != null && llmOverride.isPresent()
                        ? llmOverride.provider() : "server-default");

        CompletableFuture.runAsync(() -> runIngestion(job), analysisExecutor);

        return new SubmitResult(job.jobId(), true);
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    /** Runs the ingestion pipeline and persists any terminal failure status to Redis. */
    private void runIngestion(RepoJob job) {
        try {
            ingestionPipeline.ingest(job);

            JobStatus finalStatus = job.status();
            if (finalStatus == JobStatus.FAILED || finalStatus == JobStatus.DEAD_LETTERED) {
                jobStateService.updateStatus(job.jobId(), finalStatus, job.errorMessage());
                log.warn("[{}] Pipeline ended in terminal failure: {} — {}",
                        job.jobId(), finalStatus, job.errorMessage());
            }

        } catch (Exception e) {
            log.error("[{}] Uncaught exception in analysis executor: {}",
                    job.jobId(), e.getMessage(), e);
            jobStateService.updateStatus(job.jobId(), JobStatus.FAILED, e.getMessage());
        }
    }

    /** Strips trailing slashes and .git suffix for consistent Redis key derivation. */
    static String normalizeUrl(String repoUrl) {
        String url = repoUrl.trim();
        if (url.endsWith("/"))    url = url.substring(0, url.length() - 1);
        if (url.endsWith(".git")) url = url.substring(0, url.length() - 4);
        return url;
    }

    // ---------------------------------------------------------------------------
    // Result type
    // ---------------------------------------------------------------------------

    public record SubmitResult(UUID jobId, boolean isNew) {}
}
