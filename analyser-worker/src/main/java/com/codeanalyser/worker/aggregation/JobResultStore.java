package com.codeanalyser.worker.aggregation;

import com.codeanalyser.common.model.JobResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Saves and retrieves completed {@link JobResult}s from Redis as JSON
 * under {@code job:result:{jobId}} with a 24-hour TTL.
 */
@Service
public class JobResultStore {

    private static final Logger log = LoggerFactory.getLogger(JobResultStore.class);

    static final Duration RESULT_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper        objectMapper;

    public JobResultStore(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper  = objectMapper;
    }

    /** Persists a completed analysis result. */
    public void save(JobResult result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            redisTemplate.opsForValue().set(
                    JobResult.redisKey(result.jobId()), json, RESULT_TTL);
            log.info("[{}] Saved JobResult ({} modules, {} files) — expires in {}",
                    result.jobId(),
                    result.moduleSummaries().size(),
                    result.totalFilesAnalysed(),
                    RESULT_TTL);
        } catch (Exception e) {
            log.error("[{}] Failed to save JobResult: {}", result.jobId(), e.getMessage(), e);
            throw new RuntimeException("JobResult persistence failed for " + result.jobId(), e);
        }
    }

    /** Returns a completed result, or empty if not found or expired. */
    public Optional<JobResult> find(UUID jobId) {
        String json = redisTemplate.opsForValue().get(JobResult.redisKey(jobId));
        if (json == null) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(json, JobResult.class));
        } catch (Exception e) {
            log.warn("[{}] Could not deserialise JobResult: {}", jobId, e.getMessage());
            return Optional.empty();
        }
    }

    /** Returns true if a completed result exists for this job. */
    public boolean exists(UUID jobId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(JobResult.redisKey(jobId)));
    }
}
