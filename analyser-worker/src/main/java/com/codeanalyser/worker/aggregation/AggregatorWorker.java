package com.codeanalyser.worker.aggregation;

import com.codeanalyser.common.model.JobResult;
import com.codeanalyser.common.model.JobStatus;
import com.codeanalyser.worker.llm.LlmClient;
import com.codeanalyser.worker.llm.LlmClientFactory;
import com.codeanalyser.worker.redis.ChunkStreamPublisher;
import com.codeanalyser.worker.state.JobStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Consumes {@code stream:chunks.analysed}, tracks per-job progress, and
 * triggers {@link HierarchicalAggregator} when all chunks are complete.
 *
 * <p>Aggregation runs synchronously on the scheduler thread, providing natural
 * backpressure — the poll loop pauses until all LLM synthesis calls finish.
 *
 * <p>Uses {@link LlmClientFactory#forJob(UUID)} to obtain the correct LLM client
 * for each job — either the server default or the user's own API key. The cache
 * entry is evicted after the job reaches a terminal state (DONE or FAILED).
 */
@Component
public class AggregatorWorker {

    private static final Logger log = LoggerFactory.getLogger(AggregatorWorker.class);

    private static final int POLL_BATCH_SIZE = 20; // aggregator can handle larger batches

    private final StringRedisTemplate    redisTemplate;
    private final JobProgressTracker     progressTracker;
    private final HierarchicalAggregator aggregator;
    private final JobResultStore         resultStore;
    private final JobStateService        jobStateService;
    private final LlmClientFactory       llmClientFactory;
    private final String                 consumerName;

    public AggregatorWorker(StringRedisTemplate redisTemplate,
                            JobProgressTracker progressTracker,
                            HierarchicalAggregator aggregator,
                            JobResultStore resultStore,
                            JobStateService jobStateService,
                            LlmClientFactory llmClientFactory) {
        this.redisTemplate    = redisTemplate;
        this.progressTracker  = progressTracker;
        this.aggregator       = aggregator;
        this.resultStore      = resultStore;
        this.jobStateService  = jobStateService;
        this.llmClientFactory = llmClientFactory;
        this.consumerName     = "aggregator-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("AggregatorWorker started as consumer '{}'", consumerName);
    }

    @Scheduled(fixedDelay = 300)
    public void poll() {
        List<MapRecord<String, String, String>> messages = readAnalysed();
        if (messages == null || messages.isEmpty()) return;

        log.debug("Aggregator poll received {} message(s)", messages.size());
        for (var message : messages) {
            processMessage(message);
        }
    }

    private void processMessage(MapRecord<String, String, String> message) {
        var fields = message.getValue();

        String rawJobId   = fields.get(ChunkStreamPublisher.FIELD_JOB_ID);
        String cacheKey   = fields.get(ChunkStreamPublisher.FIELD_CACHE_KEY);
        int totalChunks   = parseInt(fields.get(ChunkStreamPublisher.FIELD_TOTAL_CHUNKS), 0);

        if (rawJobId == null || cacheKey == null || totalChunks == 0) {
            log.warn("Malformed analysed message (missing fields), acking and skipping: {}", fields);
            xack(message);
            return;
        }

        UUID jobId = UUID.fromString(rawJobId);

        try {
            boolean complete = progressTracker.recordChunkAndCheckComplete(
                    jobId, cacheKey, totalChunks);
            xack(message);

            if (complete) {
                triggerAggregation(jobId);
            }
        } catch (Exception e) {
            log.error("[{}] Error processing analysed message: {}", jobId, e.getMessage(), e);
            xack(message); // ack to avoid infinite redelivery; job stays in ANALYSING
        }
    }

    private void triggerAggregation(UUID jobId) {
        if (resultStore.exists(jobId)) {
            log.info("[{}] Result already exists, skipping re-aggregation", jobId);
            return;
        }

        String repoUrl   = getJobMeta(jobId, "repoUrl");
        String commitSha = getJobMeta(jobId, "commitSha");

        if (repoUrl == null) {
            log.error("[{}] Cannot aggregate — job:meta missing repoUrl (ingestion pipeline " +
                    "must call JobMetaStore.save() — see TODO in IngestionPipeline)", jobId);
            return;
        }

        log.info("[{}] Triggering hierarchical aggregation for {}", jobId, repoUrl);
        Set<String> chunkKeys = progressTracker.getChunkKeys(jobId);

        jobStateService.updateStatus(jobId, JobStatus.AGGREGATING);

        // Resolve the per-job LLM client — uses the caller's API key if one was stored
        // in the job's Redis state hash, otherwise falls back to the server default.
        LlmClient jobClient = llmClientFactory.forJob(jobId);

        try {
            JobResult result = aggregator.aggregate(jobId, repoUrl, commitSha, chunkKeys, jobClient);
            resultStore.save(result);

            // Terminal success — fire the DONE transition so the SSE emitter can
            // complete its stream and the polling UI knows it can fetch the result.
            jobStateService.updateStatus(jobId, JobStatus.DONE);
            llmClientFactory.evict(jobId); // release the per-job client from the cache
            log.info("[{}] ✓ Job complete — result stored, expires in {}",
                    jobId, JobResultStore.RESULT_TTL);
        } catch (LlmClient.LlmException e) {
            log.error("[{}] Aggregation LLM failure: {}", jobId, e.getMessage(), e);
            jobStateService.updateStatus(jobId, JobStatus.FAILED, e.getMessage());
            llmClientFactory.evict(jobId); // also evict on failure to avoid memory leak
        }
    }

    /** Reads a field from {@code job:meta:{jobId}}, written by the ingestion pipeline. */
    private String getJobMeta(UUID jobId, String field) {
        return redisTemplate.opsForHash()
                .get(ChunkStreamPublisher.JOB_META_KEY_PREFIX + jobId, field) instanceof String s
                ? s : null;
    }

    @SuppressWarnings("unchecked")
    private List<MapRecord<String, String, String>> readAnalysed() {
        try {
            List<?> raw = redisTemplate.opsForStream().read(
                    Consumer.from(ChunkStreamPublisher.GROUP_AGGREGATORS, consumerName),
                    StreamReadOptions.empty().count(POLL_BATCH_SIZE),
                    StreamOffset.create(
                            ChunkStreamPublisher.STREAM_ANALYSED,
                            ReadOffset.lastConsumed())
            );
            return (List<MapRecord<String, String, String>>) (List<?>) raw;
        } catch (Exception e) {
            log.warn("Aggregator XREADGROUP failed (will retry): {}", e.getMessage());
            return List.of();
        }
    }

    private void xack(MapRecord<String, String, String> message) {
        try {
            redisTemplate.opsForStream().acknowledge(
                    ChunkStreamPublisher.STREAM_ANALYSED,
                    ChunkStreamPublisher.GROUP_AGGREGATORS,
                    message.getId()
            );
        } catch (Exception e) {
            log.warn("Aggregator XACK failed for {}: {}", message.getId(), e.getMessage());
        }
    }

    private static int parseInt(String s, int fallback) {
        if (s == null || s.isBlank()) return fallback;
        try { return Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return fallback; }
    }
}
