package com.codeanalyser.common.model;

import java.util.Set;

/**
 * Pipeline states for an analysis job.
 *
 * <p>Happy path: QUEUED → CLONING → CHUNKING → ANALYSING → AGGREGATING → DONE
 * <p>Any state can transition to FAILED; after exhausting retries the job moves to DEAD_LETTERED.
 */
public enum JobStatus {

    /** Job has been accepted and placed on the Redis Stream. No work started yet. */
    QUEUED,

    /** Worker is executing `git clone --depth 1`. */
    CLONING,

    /** Clone succeeded; worker is walking the file tree and producing FileChunks. */
    CHUNKING,

    /** Chunks are on the stream; LLM workers are consuming and annotating them. */
    ANALYSING,

    /** All chunks analysed; aggregator is assembling the final documents. */
    AGGREGATING,

    /** Architecture overview, onboarding guide, and start-map are ready. */
    DONE,

    /**
     * A transient failure occurred. The job will be retried with exponential
     * backoff up to {@code ingestion.max-retries} times before moving to
     * DEAD_LETTERED.
     */
    FAILED,

    /**
     * Retry limit exhausted. The job sits in the dead-letter stream for
     * manual inspection or re-drive. No further automatic processing.
     */
    DEAD_LETTERED;

    /** Valid next states from this status. Used to validate transitions in the worker. */
    public Set<JobStatus> validNextStatuses() {
        return switch (this) {
            case QUEUED        -> Set.of(CLONING, FAILED);
            case CLONING       -> Set.of(CHUNKING, FAILED);
            case CHUNKING      -> Set.of(ANALYSING, FAILED);
            case ANALYSING     -> Set.of(AGGREGATING, FAILED);
            case AGGREGATING   -> Set.of(DONE, FAILED);
            case DONE          -> Set.of();           // terminal
            case FAILED        -> Set.of(QUEUED, DEAD_LETTERED); // re-queue or give up
            case DEAD_LETTERED -> Set.of();           // terminal
        };
    }

    /** Convenience: is this a terminal state (no further transitions possible)? */
    public boolean isTerminal() {
        return this == DONE || this == DEAD_LETTERED;
    }

    /** Convenience: should the UI show a spinner for this status? */
    public boolean isInProgress() {
        return this == CLONING || this == CHUNKING
                || this == ANALYSING || this == AGGREGATING;
    }
}
