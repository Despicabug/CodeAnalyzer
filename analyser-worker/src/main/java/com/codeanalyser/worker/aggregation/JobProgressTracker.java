package com.codeanalyser.worker.aggregation;

import com.codeanalyser.common.model.JobResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;

/**
 * Tracks per-job analysis progress using an atomic Redis counter plus a Set for
 * deduplication. SADD returns 0 for duplicate chunk keys, preventing re-delivered
 * stream messages from falsely triggering aggregation.
 */
@Service
public class JobProgressTracker {

    private static final Logger log = LoggerFactory.getLogger(JobProgressTracker.class);

    static final Duration JOB_TTL = Duration.ofHours(48);

    private final StringRedisTemplate redisTemplate;

    public JobProgressTracker(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /** Records the total chunk count. Must be called before any analysed-stream messages are processed. */
    public void recordTotal(UUID jobId, int totalChunks) {
        String key = JobResult.totalChunksKey(jobId);
        redisTemplate.opsForValue().set(key, String.valueOf(totalChunks), JOB_TTL);
        log.debug("[{}] Recorded total chunks: {}", jobId, totalChunks);
    }

    /**
     * Records a completed chunk and returns true if all chunks for this job are done.
     * totalChunks is used as a fallback if the Redis total key has expired.
     */
    public boolean recordChunkAndCheckComplete(UUID jobId, String cacheKey, int totalChunks) {
        String setKey      = JobResult.chunkKeysSetKey(jobId);
        String counterKey  = JobResult.progressKey(jobId);
        String totalKey    = JobResult.totalChunksKey(jobId);

        Long added = redisTemplate.opsForSet().add(setKey, cacheKey);
        redisTemplate.expire(setKey, JOB_TTL);

        if (added == null || added == 0) {
            log.debug("[{}] Duplicate chunk cacheKey={}, skipping INCR", jobId, cacheKey);
            return false;
        }

        Long count = redisTemplate.opsForValue().increment(counterKey);
        redisTemplate.expire(counterKey, JOB_TTL);

        String totalStr = redisTemplate.opsForValue().get(totalKey);
        int    total    = (totalStr != null) ? parseInt(totalStr, totalChunks) : totalChunks;

        log.debug("[{}] Progress: {}/{}", jobId, count, total);

        boolean complete = count != null && count >= total;
        if (complete) {
            log.info("[{}] All {} chunks analysed — triggering aggregation", jobId, total);
        }
        return complete;
    }

    /** Returns all cacheKeys for completed chunks of this job. */
    public Set<String> getChunkKeys(UUID jobId) {
        Set<String> keys = redisTemplate.opsForSet().members(JobResult.chunkKeysSetKey(jobId));
        return keys != null ? keys : Set.of();
    }

    private static int parseInt(String s, int fallback) {
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return fallback; }
    }
}
