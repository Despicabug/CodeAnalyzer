package com.codeanalyser.worker.state;

import com.codeanalyser.common.model.JobStatus;
import org.springframework.context.ApplicationEvent;

import java.util.UUID;

/**
 * Fired when a job transitions to a new status. Drives the SSE stage tracker in the UI.
 *
 * <p>Uses Spring's in-process event bus rather than Redis pub/sub since the API and
 * worker run in the same JVM. The @EventListener interface would stay the same if
 * the transport were replaced later.
 */
public class JobStatusChangedEvent extends ApplicationEvent {

    private final UUID      jobId;
    private final JobStatus status;
    private final String    errorMessage; // null unless FAILED/DEAD_LETTERED

    public JobStatusChangedEvent(Object source, UUID jobId, JobStatus status, String errorMessage) {
        super(source);
        this.jobId        = jobId;
        this.status       = status;
        this.errorMessage = errorMessage;
    }

    public UUID      jobId()        { return jobId; }
    public JobStatus status()       { return status; }
    public String    errorMessage() { return errorMessage; }
}
