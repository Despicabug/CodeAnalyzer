package com.codeanalyser.worker.state;

import org.springframework.context.ApplicationEvent;

import java.util.UUID;

/** Fired when a chunk analysis completes. Drives the SSE progress bar in the UI. */
public class JobProgressUpdatedEvent extends ApplicationEvent {

    private final UUID jobId;
    private final int  analysed;
    private final int  total;

    public JobProgressUpdatedEvent(Object source, UUID jobId, int analysed, int total) {
        super(source);
        this.jobId    = jobId;
        this.analysed = analysed;
        this.total    = total;
    }

    public UUID jobId()    { return jobId; }
    public int  analysed() { return analysed; }
    public int  total()    { return total; }

    /** Percentage complete, 0–100. */
    public int percent() {
        return total == 0 ? 0 : (int) ((analysed * 100.0) / total);
    }
}
