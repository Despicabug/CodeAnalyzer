package com.codeanalyser.common.model;

import java.util.UUID;

/**
 * An immutable slice of a source file ready for LLM analysis.
 *
 * <p>The cache key incorporates the file's SHA-256 hash and the commit SHA, so
 * unchanged files get a cache hit across re-submissions and different repos.
 */
public record FileChunk(
        UUID   chunkId,
        UUID   jobId,
        String repoUrl,
        String commitSha,
        String filePath,
        String fileHash,
        String content,
        int    chunkIndex,
        int    totalChunks,
        String language
) {
    /** Redis cache key: {@code analysis:<repoUrl>:<commitSha>:<fileHash>:<chunkIndex>} */
    public String cacheKey() {
        return "analysis:" + repoUrl + ":" + commitSha + ":" + fileHash + ":" + chunkIndex;
    }

    /** True if this file fit in a single chunk without splitting. */
    public boolean isSingleChunk() {
        return totalChunks == 1;
    }
}
