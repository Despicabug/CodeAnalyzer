package com.codeanalyser.common.model;

import java.time.Instant;
import java.util.UUID;

/**
 * Mutable in-memory representation of an analysis job. Not a record because
 * status, retry count, and error message change as the job progresses.
 */
public class RepoJob {

    private final UUID    jobId;
    private final String  repoUrl;
    private final Instant submittedAt;

    private volatile String    commitSha;    // populated once clone succeeds
    private volatile JobStatus status;
    private volatile int       retryCount;
    private volatile String    errorMessage; // last error, if any
    private volatile Instant   updatedAt;

    private final int maxRetries;

    private RepoJob(Builder builder) {
        this.jobId       = builder.jobId;
        this.repoUrl     = builder.repoUrl;
        this.submittedAt = builder.submittedAt;
        this.commitSha   = builder.commitSha;
        this.status      = builder.status;
        this.retryCount  = builder.retryCount;
        this.errorMessage = builder.errorMessage;
        this.updatedAt   = builder.updatedAt;
        this.maxRetries  = builder.maxRetries;
    }

    /**
     * Transitions to {@code next}, throwing if the transition is not valid.
     */
    public synchronized void transitionTo(JobStatus next) {
        if (!status.validNextStatuses().contains(next)) {
            throw new IllegalStateException(
                    "Invalid transition: %s → %s for job %s".formatted(status, next, jobId));
        }
        this.status    = next;
        this.updatedAt = Instant.now();
    }

    /**
     * Records a failure. Returns true if retries remain, false if dead-lettered.
     */
    public synchronized boolean recordFailure(String reason) {
        this.errorMessage = reason;
        this.retryCount++;
        this.updatedAt = Instant.now();

        if (retryCount <= maxRetries) {
            this.status = JobStatus.FAILED;
            return true;
        } else {
            this.status = JobStatus.DEAD_LETTERED;
            return false;
        }
    }

    public UUID      jobId()        { return jobId; }
    public String    repoUrl()      { return repoUrl; }
    public Instant   submittedAt()  { return submittedAt; }
    public String    commitSha()    { return commitSha; }
    public JobStatus status()       { return status; }
    public int       retryCount()   { return retryCount; }
    public String    errorMessage() { return errorMessage; }
    public Instant   updatedAt()    { return updatedAt; }
    public int       maxRetries()   { return maxRetries; }

    public void setCommitSha(String sha) {
        this.commitSha = sha;
        this.updatedAt = Instant.now();
    }

    public static Builder builder(String repoUrl) {
        return new Builder(repoUrl);
    }

    public static final class Builder {
        private UUID    jobId       = UUID.randomUUID();
        private final String repoUrl;
        private Instant submittedAt = Instant.now();
        private String  commitSha;
        private JobStatus status    = JobStatus.QUEUED;
        private int     retryCount  = 0;
        private String  errorMessage;
        private Instant updatedAt   = Instant.now();
        private int     maxRetries  = 3;

        private Builder(String repoUrl) { this.repoUrl = repoUrl; }

        public Builder jobId(UUID id)           { this.jobId       = id;          return this; }
        public Builder commitSha(String sha)    { this.commitSha   = sha;         return this; }
        public Builder status(JobStatus s)      { this.status      = s;           return this; }
        public Builder retryCount(int n)        { this.retryCount  = n;           return this; }
        public Builder maxRetries(int n)        { this.maxRetries  = n;           return this; }
        public Builder errorMessage(String msg) { this.errorMessage = msg;        return this; }
        public Builder submittedAt(Instant t)   { this.submittedAt = t;           return this; }

        public RepoJob build() { return new RepoJob(this); }
    }

    @Override
    public String toString() {
        return "RepoJob{jobId=%s, repo=%s, status=%s, retries=%d/%d}"
                .formatted(jobId, repoUrl, status, retryCount, maxRetries);
    }
}
