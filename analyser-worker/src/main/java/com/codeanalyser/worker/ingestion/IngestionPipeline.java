package com.codeanalyser.worker.ingestion;

import com.codeanalyser.common.model.FileChunk;
import com.codeanalyser.common.model.JobStatus;
import com.codeanalyser.common.model.RepoJob;
import com.codeanalyser.worker.config.IngestionProperties;
import com.codeanalyser.worker.redis.ChunkStreamPublisher;
import com.codeanalyser.worker.state.JobStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestrates the ingestion pipeline: clone → chunk → publish to Redis Stream.
 * Owns job state transitions and the {@link TempRepoWorkspace} lifecycle.
 * {@link GitCloneService.RepoTooLargeException} is non-retryable; all other
 * failures are retried via {@link RepoJob#recordFailure}.
 */
@Service
public class IngestionPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestionPipeline.class);

    private final GitCloneService      gitCloneService;
    private final FileChunkerService   fileChunkerService;
    private final ChunkStreamPublisher streamPublisher;
    private final JobStateService      jobStateService;
    private final IngestionProperties  props;

    public IngestionPipeline(GitCloneService gitCloneService,
                             FileChunkerService fileChunkerService,
                             ChunkStreamPublisher streamPublisher,
                             JobStateService jobStateService,
                             IngestionProperties props) {
        this.gitCloneService    = gitCloneService;
        this.fileChunkerService = fileChunkerService;
        this.streamPublisher    = streamPublisher;
        this.jobStateService    = jobStateService;
        this.props              = props;
    }

    /**
     * Runs the pipeline for the given job. On success the job is left in ANALYSING
     * with chunks published to the stream. On failure it is FAILED or DEAD_LETTERED.
     *
     * @return produced chunks, or empty if the job failed
     */
    public List<FileChunk> ingest(RepoJob job) {
        log.info("Starting ingestion for job {}", job);

        job.transitionTo(JobStatus.CLONING);
        jobStateService.updateStatus(job.jobId(), JobStatus.CLONING);

        // TempRepoWorkspace is AutoCloseable — the clone is deleted when the try block exits.
        try (var workspace = TempRepoWorkspace.create(job.jobId())) {

            String commitSha;
            try {
                commitSha = gitCloneService.cloneRepo(
                        job.jobId(), job.repoUrl(), workspace.repoDir());
                job.setCommitSha(commitSha);
            } catch (GitCloneService.RepoTooLargeException e) {
                // Non-retryable: repo is over the size limit. Dead-letter immediately.
                log.error("[{}] Repo too large, dead-lettering: {}", job.jobId(), e.getMessage());
                job.recordFailure(e.getMessage()); // burns remaining retries → DEAD_LETTERED
                while (!job.status().isTerminal()) {
                    job.recordFailure("repo too large");
                }
                return List.of();
            } catch (GitCloneService.GitCloneException | InterruptedException e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                log.warn("[{}] Clone failed (retryable): {}", job.jobId(), e.getMessage());
                job.recordFailure(e.getMessage());
                return List.of();
            }

            job.transitionTo(JobStatus.CHUNKING);
            jobStateService.updateStatus(job.jobId(), JobStatus.CHUNKING);

            List<FileChunk> chunks;
            try {
                chunks = fileChunkerService.chunkRepository(
                        job.jobId(), job.repoUrl(), commitSha, workspace.repoDir());
            } catch (Exception e) {
                log.error("[{}] Chunking failed: {}", job.jobId(), e.getMessage(), e);
                job.recordFailure("Chunking error: " + e.getMessage());
                return List.of();
            }

            if (chunks.isEmpty()) {
                log.warn("[{}] No chunks produced — repo may be empty or entirely vendored/binary",
                        job.jobId());
                job.recordFailure("No analysable source files found");
                return List.of();
            }

            job.transitionTo(JobStatus.ANALYSING);
            jobStateService.updateStatus(job.jobId(), JobStatus.ANALYSING);
            jobStateService.updateTotal(job.jobId(), chunks.size());
            logChunkSummary(job, chunks);

            int published = streamPublisher.publishAll(chunks);
            log.info("[{}] ✓ Ingestion complete — {}/{} chunks enqueued to {}",
                    job.jobId(), published, chunks.size(),
                    ChunkStreamPublisher.STREAM_PENDING);

            return chunks;

        } catch (Exception e) {
            log.error("[{}] Unexpected ingestion error: {}", job.jobId(), e.getMessage(), e);
            job.recordFailure("Unexpected error: " + e.getMessage());
            return List.of();
        }
    }

    private void logChunkSummary(RepoJob job, List<FileChunk> chunks) {
        long multiChunkFiles = chunks.stream()
                .filter(c -> !c.isSingleChunk())
                .map(FileChunk::filePath)
                .distinct()
                .count();

        long uniqueLanguages = chunks.stream()
                .map(FileChunk::language)
                .filter(l -> !l.equals("unknown"))
                .distinct()
                .count();

        log.info("[{}] Chunk summary: total={}, files={}, split-files={}, languages={}",
                job.jobId(),
                chunks.size(),
                chunks.stream().map(FileChunk::filePath).distinct().count(),
                multiChunkFiles,
                uniqueLanguages);

        // Log per-language breakdown — useful for verifying the LLM prompt will
        // be told the correct language for each chunk.
        chunks.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        FileChunk::language, java.util.stream.Collectors.counting()))
                .forEach((lang, count) ->
                        log.info("[{}]   {} → {} chunk(s)", job.jobId(), lang, count));
    }
}
