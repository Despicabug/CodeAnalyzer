package com.codeanalyser.common.model;

import java.time.Instant;
import java.util.UUID;

/**
 * Observable state of an analysis job, stored in Redis as a Hash under
 * {@code job:state:{jobId}}. Written by the worker at each pipeline transition.
 */
public record JobState(
        UUID      jobId,
        String    repoUrl,
        JobStatus status,
        int       progressAnalysed,
        int       progressTotal,
        Instant   createdAt,
        Instant   updatedAt,
        String    errorMessage
) {
    /** Redis Hash key for this job's state. */
    public static String redisKey(UUID jobId) {
        return "job:state:" + jobId;
    }

    /** Maps a repo URL to its most recent jobId for idempotency checks. */
    public static String latestJobKey(String repoUrl) {
        return "job:latest:" + repoUrl;
    }

    /** Convenience: percentage complete for UI progress bars. */
    public int progressPercent() {
        if (progressTotal == 0) return 0;
        return (int) ((progressAnalysed * 100.0) / progressTotal);
    }
}
