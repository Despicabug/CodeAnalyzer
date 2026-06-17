package com.codeanalyser.common.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Final output of a completed analysis job: the three documents produced by the
 * hierarchical aggregator, stored in Redis under {@code job:result:{jobId}}.
 */
public record JobResult(
        UUID              jobId,
        String            repoUrl,
        String            commitSha,
        String            architectureOverview,
        String            onboardingGuide,
        String            startMap,
        List<ModuleSummary> moduleSummaries,
        int               totalFilesAnalysed,
        Instant           generatedAt
) {
    /** Redis key where this result is stored. */
    public static String redisKey(UUID jobId) {
        return "job:result:" + jobId;
    }

    /** Redis key where the job's total chunk count is stored (set at ingestion time). */
    public static String totalChunksKey(UUID jobId) {
        return "job:total:" + jobId;
    }

    /** Redis key for the INCR completion counter. */
    public static String progressKey(UUID jobId) {
        return "job:progress:" + jobId;
    }

    /** Redis key for the Set of completed cacheKeys (dedup + retrieval). */
    public static String chunkKeysSetKey(UUID jobId) {
        return "job:chunk-keys:" + jobId;
    }
}
