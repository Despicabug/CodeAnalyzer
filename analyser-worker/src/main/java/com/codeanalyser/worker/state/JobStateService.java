package com.codeanalyser.worker.state;

import com.codeanalyser.common.model.JobState;
import com.codeanalyser.common.model.JobStatus;
import com.codeanalyser.common.model.LlmOverride;
import com.codeanalyser.worker.llm.LlmOverrideStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Persists job state to a Redis Hash ({@code job:state:{jobId}}) and fires
 * Spring application events so the SSE layer can push live updates to the browser.
 */
@Service
public class JobStateService {

    private static final Logger log = LoggerFactory.getLogger(JobStateService.class);

    /** State keys survive long enough to cover any reasonable analysis run. */
    static final Duration STATE_TTL = Duration.ofHours(48);

    // Hash field names — typed constants to prevent typos.
    private static final String F_STATUS    = "status";
    private static final String F_REPO_URL  = "repoUrl";
    private static final String F_ANALYSED  = "progressAnalysed";
    private static final String F_TOTAL     = "progressTotal";
    private static final String F_CREATED   = "createdAt";
    private static final String F_UPDATED   = "updatedAt";
    private static final String F_ERROR     = "errorMessage";

    private final StringRedisTemplate     redisTemplate;
    private final ApplicationEventPublisher eventPublisher;

    public JobStateService(StringRedisTemplate redisTemplate,
                           ApplicationEventPublisher eventPublisher) {
        this.redisTemplate  = redisTemplate;
        this.eventPublisher = eventPublisher;
    }

    /** Creates initial QUEUED state for a new job (no LLM override). */
    public void createJob(UUID jobId, String repoUrl) {
        createJob(jobId, repoUrl, null);
    }

    /**
     * Creates initial QUEUED state and optionally stores a per-job LLM override.
     * The override fields are written into the same Redis Hash so
     * {@link LlmOverrideStore} can read them back without a separate key.
     */
    public void createJob(UUID jobId, String repoUrl, LlmOverride llmOverride) {
        String now = Instant.now().toString();
        String key = JobState.redisKey(jobId);

        Map<String, Object> fields = new HashMap<>();
        fields.put(F_STATUS,   JobStatus.QUEUED.name());
        fields.put(F_REPO_URL, repoUrl);
        fields.put(F_ANALYSED, "0");
        fields.put(F_TOTAL,    "0");
        fields.put(F_CREATED,  now);
        fields.put(F_UPDATED,  now);
        fields.put(F_ERROR,    "");

        if (llmOverride != null && llmOverride.isPresent()) {
            if (llmOverride.provider() != null) fields.put(LlmOverrideStore.F_LLM_PROVIDER, llmOverride.provider());
            if (llmOverride.apiKey()   != null) fields.put(LlmOverrideStore.F_LLM_API_KEY,  llmOverride.apiKey());
            if (llmOverride.model()    != null) fields.put(LlmOverrideStore.F_LLM_MODEL,    llmOverride.model());
            if (llmOverride.baseUrl()  != null) fields.put(LlmOverrideStore.F_LLM_BASE_URL, llmOverride.baseUrl());
        }

        redisTemplate.opsForHash().putAll(key, fields);
        redisTemplate.expire(key, STATE_TTL);
        log.debug("[{}] Job state created: QUEUED", jobId);
        eventPublisher.publishEvent(new JobStatusChangedEvent(this, jobId, JobStatus.QUEUED, null));
    }

    /** Updates job status and fires a status-changed event. */
    public void updateStatus(UUID jobId, JobStatus status) {
        updateStatus(jobId, status, null);
    }

    public void updateStatus(UUID jobId, JobStatus status, String errorMessage) {
        String key = JobState.redisKey(jobId);
        redisTemplate.opsForHash().put(key, F_STATUS,  status.name());
        redisTemplate.opsForHash().put(key, F_UPDATED, Instant.now().toString());
        if (errorMessage != null) {
            redisTemplate.opsForHash().put(key, F_ERROR, errorMessage);
        }
        redisTemplate.expire(key, STATE_TTL);
        log.debug("[{}] Status → {}", jobId, status);
        eventPublisher.publishEvent(
                new JobStatusChangedEvent(this, jobId, status, errorMessage));
    }

    /** Increments the analysed chunk counter and fires a progress event. */
    public void incrementProgress(UUID jobId) {
        String key = JobState.redisKey(jobId);

        Long analysed = redisTemplate.opsForHash().increment(key, F_ANALYSED, 1);
        redisTemplate.opsForHash().put(key, F_UPDATED, Instant.now().toString());
        redisTemplate.expire(key, STATE_TTL);

        Object totalObj = redisTemplate.opsForHash().get(key, F_TOTAL);
        int total = parseIntOrZero(totalObj);
        int analysedInt = analysed == null ? 0 : analysed.intValue();

        log.trace("[{}] Progress: {}/{}", jobId, analysedInt, total);
        eventPublisher.publishEvent(
                new JobProgressUpdatedEvent(this, jobId, analysedInt, total));
    }

    /** Records total chunk count once chunking is complete. */
    public void updateTotal(UUID jobId, int total) {
        String key = JobState.redisKey(jobId);
        redisTemplate.opsForHash().put(key, F_TOTAL,   String.valueOf(total));
        redisTemplate.opsForHash().put(key, F_UPDATED, Instant.now().toString());
        redisTemplate.expire(key, STATE_TTL);
    }

    /** Returns the current state, or empty if the key has expired. */
    public Optional<JobState> getState(UUID jobId) {
        Map<Object, Object> fields = redisTemplate.opsForHash()
                .entries(JobState.redisKey(jobId));
        if (fields == null || fields.isEmpty()) return Optional.empty();

        return Optional.of(new JobState(
                jobId,
                str(fields, F_REPO_URL),
                parseStatus(str(fields, F_STATUS)),
                parseIntOrZero(fields.get(F_ANALYSED)),
                parseIntOrZero(fields.get(F_TOTAL)),
                parseInstant(str(fields, F_CREATED)),
                parseInstant(str(fields, F_UPDATED)),
                emptyToNull(str(fields, F_ERROR))
        ));
    }

    private static String str(Map<Object, Object> m, String key) {
        Object v = m.get(key);
        return v != null ? v.toString() : "";
    }

    private static int parseIntOrZero(Object v) {
        if (v == null) return 0;
        try { return Integer.parseInt(v.toString().trim()); }
        catch (NumberFormatException e) { return 0; }
    }

    private static JobStatus parseStatus(String s) {
        try { return JobStatus.valueOf(s); }
        catch (Exception e) { return JobStatus.QUEUED; }
    }

    private static Instant parseInstant(String s) {
        try { return Instant.parse(s); }
        catch (Exception e) { return Instant.EPOCH; }
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }
}
