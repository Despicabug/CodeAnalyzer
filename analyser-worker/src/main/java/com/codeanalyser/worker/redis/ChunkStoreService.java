package com.codeanalyser.worker.redis;

import com.codeanalyser.common.model.ChunkAnalysisResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

/**
 * Manages two Redis key spaces for chunk data:
 * <ul>
 *   <li>{@code chunk:content:{cacheKey}} — raw source text (24h TTL)</li>
 *   <li>{@code chunk:analysis:{cacheKey}} — serialised analysis result (7d TTL)</li>
 * </ul>
 *
 * <p>Stream messages carry only the cacheKey; workers fetch content from here
 * before calling the LLM. A cache hit on the analysis key skips the LLM call entirely.
 */
@Service
public class ChunkStoreService {

    private static final Logger log = LoggerFactory.getLogger(ChunkStoreService.class);

    static final Duration CONTENT_TTL  = Duration.ofHours(24);
    static final Duration ANALYSIS_TTL = Duration.ofDays(7);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper        objectMapper;

    public ChunkStoreService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper  = objectMapper;
    }

    /** Saves raw chunk content. Called before the stream message is published. */
    public void saveContent(String cacheKey, String content) {
        String key = ChunkAnalysisResult.contentRedisKey(cacheKey);
        redisTemplate.opsForValue().set(key, content, CONTENT_TTL);
        log.trace("Stored content for cacheKey={} ({} chars)", cacheKey, content.length());
    }

    /** Returns raw chunk content, or empty if expired. */
    public Optional<String> getContent(String cacheKey) {
        String value = redisTemplate.opsForValue().get(ChunkAnalysisResult.contentRedisKey(cacheKey));
        return Optional.ofNullable(value);
    }

    /** Saves a completed analysis result as JSON. */
    public void saveAnalysis(ChunkAnalysisResult result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            redisTemplate.opsForValue().set(
                    ChunkAnalysisResult.redisKey(result.cacheKey()), json, ANALYSIS_TTL);
            log.debug("Cached analysis for cacheKey={}", result.cacheKey());
        } catch (Exception e) {
            log.error("Failed to serialise ChunkAnalysisResult for cacheKey={}: {}",
                    result.cacheKey(), e.getMessage());
        }
    }

    /** Returns a cached analysis result, or empty on cache miss. */
    public Optional<ChunkAnalysisResult> getAnalysis(String cacheKey) {
        String json = redisTemplate.opsForValue().get(ChunkAnalysisResult.redisKey(cacheKey));
        if (json == null) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(json, ChunkAnalysisResult.class));
        } catch (Exception e) {
            log.warn("Could not deserialise cached analysis for cacheKey={}, will re-analyse: {}",
                    cacheKey, e.getMessage());
            return Optional.empty();
        }
    }

    /** Returns true if a valid analysis result exists for this cacheKey (cache hit check). */
    public boolean hasAnalysis(String cacheKey) {
        return Boolean.TRUE.equals(
                redisTemplate.hasKey(ChunkAnalysisResult.redisKey(cacheKey)));
    }
}
