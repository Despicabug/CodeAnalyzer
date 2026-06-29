package com.codeanalyser.worker.analysis;

import com.codeanalyser.common.model.ChunkAnalysisResult;
import com.codeanalyser.worker.llm.LlmClient;
import com.codeanalyser.worker.llm.LlmClientFactory;
import com.codeanalyser.worker.llm.LlmRequest;
import com.codeanalyser.worker.llm.LlmResponse;
import com.codeanalyser.worker.redis.ChunkStoreService;
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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Polls {@code stream:chunks.pending}, calls the LLM for each chunk, and
 * publishes results to {@code stream:chunks.analysed}.
 *
 * <p>Uses {@link LlmClientFactory#forJob(UUID)} to obtain the correct LLM client
 * for each message — either the server default or the user's own API key (BYOK).
 * This means multiple concurrent jobs each use their own isolated credentials.
 *
 * <p>Uses {@code @Scheduled(fixedDelay)} rather than a listener container for
 * simpler per-message error handling and explicit batch size control.
 *
 * <p>On failure: XACK the original, then re-enqueue with retryCount+1 up to
 * maxRetries, after which the message goes to the DLQ stream.
 */
@Component
public class ChunkAnalysisWorker {

    private static final Logger log = LoggerFactory.getLogger(ChunkAnalysisWorker.class);

    /** Messages read per polling tick. Keep small so rate-limiting is fine-grained. */
    private static final int POLL_BATCH_SIZE = 5;

    private final StringRedisTemplate  redisTemplate;
    private final ChunkStoreService    chunkStore;
    private final ChunkStreamPublisher streamPublisher;
    private final LlmClientFactory     llmClientFactory;
    private final JobStateService      jobStateService;
    private final int                  maxRetries;
    private final String               consumerName;

    public ChunkAnalysisWorker(
            StringRedisTemplate redisTemplate,
            ChunkStoreService chunkStore,
            ChunkStreamPublisher streamPublisher,
            LlmClientFactory llmClientFactory,
            JobStateService jobStateService,
            @Value("${ingestion.max-retries:3}") int maxRetries) {
        this.redisTemplate    = redisTemplate;
        this.chunkStore       = chunkStore;
        this.streamPublisher  = streamPublisher;
        this.llmClientFactory = llmClientFactory;
        this.jobStateService  = jobStateService;
        this.maxRetries       = maxRetries;
        this.consumerName     = "worker-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("ChunkAnalysisWorker started as consumer '{}'", consumerName);
    }

    /** fixedDelay means the next poll waits until the previous tick finishes, preventing overlapping calls. */
    @Scheduled(fixedDelay = 200)
    public void poll() {
        List<MapRecord<String, String, String>> messages = readPending();
        if (messages == null || messages.isEmpty()) return;

        log.debug("Poll received {} message(s)", messages.size());
        for (var message : messages) {
            processMessage(message);
        }
    }

    private void processMessage(MapRecord<String, String, String> message) {
        Map<String, String> fields  = message.getValue();
        String cacheKey  = fields.get(ChunkStreamPublisher.FIELD_CACHE_KEY);
        String filePath  = fields.get(ChunkStreamPublisher.FIELD_FILE_PATH);
        String jobId     = fields.get(ChunkStreamPublisher.FIELD_JOB_ID);
        int    retryCount = parseInt(fields.get(ChunkStreamPublisher.FIELD_RETRY_COUNT), 0);

        log.debug("Processing chunk cacheKey={} file={} retry={}", cacheKey, filePath, retryCount);

        try {
            // Resolve per-job client each time — factory caches after first build so
            // repeated calls for the same jobId are effectively free.
            LlmClient jobClient = llmClientFactory.forJob(UUID.fromString(jobId));
            ChunkAnalysisResult result = analyseChunk(fields, cacheKey, filePath, jobId, jobClient);
            publishAnalysed(result, fields);
            xack(message);
            jobStateService.incrementProgress(UUID.fromString(jobId));
            log.info("✓ Analysed {} (jobId={}, cacheKey={})", filePath, jobId, cacheKey);

        } catch (Exception e) {
            log.warn("✗ Failed to analyse {} (retry {}/{}): {}",
                    filePath, retryCount, maxRetries, e.getMessage());
            xack(message); // always ack the original, then decide retry vs DLQ
            handleFailure(fields, retryCount, filePath, e.getMessage());
        }
    }

    /** Cache hit → skip LLM. Cache miss → load content → call LLM → store result. */
    private ChunkAnalysisResult analyseChunk(Map<String, String> fields,
                                              String cacheKey, String filePath,
                                              String jobId, LlmClient llmClient)
            throws LlmClient.LlmException {

        if (chunkStore.hasAnalysis(cacheKey)) {
            log.debug("Cache hit for cacheKey={}", cacheKey);
            return chunkStore.getAnalysis(cacheKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Cache claimed hit but returned empty for " + cacheKey));
        }

        String content = chunkStore.getContent(cacheKey)
                .orElseThrow(() -> new IllegalStateException(
                        "Content missing in Redis for cacheKey=" + cacheKey
                        + ". TTL may have expired before analysis started."));

        LlmRequest request = new LlmRequest(
                filePath,
                fields.getOrDefault(ChunkStreamPublisher.FIELD_LANGUAGE, "unknown"),
                content,
                parseInt(fields.get(ChunkStreamPublisher.FIELD_CHUNK_INDEX), 0),
                parseInt(fields.get(ChunkStreamPublisher.FIELD_TOTAL_CHUNKS), 1)
        );

        LlmResponse response = llmClient.analyse(request);

        ChunkAnalysisResult result = new ChunkAnalysisResult(
                UUID.fromString(fields.get(ChunkStreamPublisher.FIELD_CHUNK_ID)),
                UUID.fromString(jobId),
                cacheKey,
                filePath,
                parseInt(fields.get(ChunkStreamPublisher.FIELD_CHUNK_INDEX), 0),
                parseInt(fields.get(ChunkStreamPublisher.FIELD_TOTAL_CHUNKS), 1),
                response.analysisText(),
                response.confidence(),
                Instant.now()
        );

        chunkStore.saveAnalysis(result);
        return result;
    }

    /** Publishes a reference-only message to the analysed stream. Full result is in Redis. */
    private void publishAnalysed(ChunkAnalysisResult result, Map<String, String> sourceFields) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(ChunkStreamPublisher.FIELD_CHUNK_ID,     result.chunkId().toString());
        fields.put(ChunkStreamPublisher.FIELD_JOB_ID,       result.jobId().toString());
        fields.put(ChunkStreamPublisher.FIELD_CACHE_KEY,    result.cacheKey());
        fields.put(ChunkStreamPublisher.FIELD_FILE_PATH,    result.filePath());
        fields.put(ChunkStreamPublisher.FIELD_CHUNK_INDEX,  String.valueOf(result.chunkIndex()));
        fields.put(ChunkStreamPublisher.FIELD_TOTAL_CHUNKS, String.valueOf(result.totalChunks()));
        fields.put(ChunkStreamPublisher.FIELD_LANGUAGE,
                sourceFields.getOrDefault(ChunkStreamPublisher.FIELD_LANGUAGE, "unknown"));

        var record = org.springframework.data.redis.connection.stream.MapRecord
                .create(ChunkStreamPublisher.STREAM_ANALYSED, fields);
        redisTemplate.opsForStream().add(record);
    }

    private void handleFailure(Map<String, String> originalFields, int retryCount,
                                String filePath, String reason) {
        if (retryCount < maxRetries) {
            int nextRetry = retryCount + 1;
            log.info("Re-queuing {} for retry {}/{}", filePath, nextRetry, maxRetries);
            streamPublisher.republish(originalFields, nextRetry);
        } else {
            log.error("Dead-lettering {} after {} retries. Reason: {}", filePath, maxRetries, reason);
            Map<String, String> dlqFields = new LinkedHashMap<>(originalFields);
            dlqFields.put("failureReason", reason != null ? reason : "unknown");
            dlqFields.put("deadLetteredAt", Instant.now().toString());
            streamPublisher.publishToDlq(dlqFields);
        }
    }

    @SuppressWarnings("unchecked")
    private List<MapRecord<String, String, String>> readPending() {
        try {
            // Double-cast: Spring Data Redis returns Object types at runtime due to erasure.
            // StringRedisTemplate always deserialises as Strings, so the cast is safe.
            List<?> raw = redisTemplate.opsForStream().read(
                    Consumer.from(ChunkStreamPublisher.GROUP_ANALYSERS, consumerName),
                    StreamReadOptions.empty().count(POLL_BATCH_SIZE),
                    StreamOffset.create(ChunkStreamPublisher.STREAM_PENDING, ReadOffset.lastConsumed())
            );
            return (List<MapRecord<String, String, String>>) (List<?>) raw;
        } catch (Exception e) {
            log.warn("XREADGROUP failed (will retry): {}", e.getMessage());
            return List.of();
        }
    }

    private void xack(MapRecord<String, String, String> message) {
        try {
            redisTemplate.opsForStream().acknowledge(
                    ChunkStreamPublisher.STREAM_PENDING,
                    ChunkStreamPublisher.GROUP_ANALYSERS,
                    message.getId()
            );
        } catch (Exception e) {
            // Message stays in PEL and will be re-delivered on restart.
            // The cache hit check makes re-processing safe — no duplicate LLM calls.
            log.warn("XACK failed for message {}: {}", message.getId(), e.getMessage());
        }
    }

    private static int parseInt(String value, int defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try { return Integer.parseInt(value.trim()); }
        catch (NumberFormatException e) { return defaultValue; }
    }
}
