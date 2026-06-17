package com.codeanalyser.common.model;

import java.time.Instant;
import java.util.UUID;

/**
 * The LLM's analysis of a single source chunk, stored in Redis under
 * {@code chunk:analysis:{cacheKey}}.
 *
 * <p>{@code confidence} is stubbed at 1.0; a real quality scorer is a future TODO.
 */
public record ChunkAnalysisResult(
        UUID    chunkId,
        UUID    jobId,
        String  cacheKey,
        String  filePath,
        int     chunkIndex,
        int     totalChunks,
        String  analysisText,
        double  confidence,
        Instant analysedAt
) {
    /** Redis key where this result is stored. Matches {@link FileChunk#cacheKey()}. */
    public static String redisKey(String cacheKey) {
        return "chunk:analysis:" + cacheKey;
    }

    /** Redis key where the source content is stored. */
    public static String contentRedisKey(String cacheKey) {
        return "chunk:content:" + cacheKey;
    }
}
