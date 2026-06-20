package com.codeanalyser.worker.redis;

import com.codeanalyser.common.model.FileChunk;
import com.codeanalyser.common.model.JobResult;
import com.codeanalyser.worker.aggregation.JobProgressTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Publishes chunks to {@code stream:chunks.pending}. For each chunk, content is
 * written to Redis first, then a reference-only stream message is added. This
 * ensures consumers always find content ready when they pick up a message.
 */
@Service
public class ChunkStreamPublisher {

    private static final Logger log = LoggerFactory.getLogger(ChunkStreamPublisher.class);

    public static final String STREAM_PENDING  = "stream:chunks.pending";
    public static final String STREAM_ANALYSED = "stream:chunks.analysed";
    public static final String STREAM_DLQ      = "stream:chunks.dlq";

    // Consumer group names — also referenced by StreamConsumerSetup.
    public static final String GROUP_ANALYSERS   = "analysers";
    public static final String GROUP_AGGREGATORS = "aggregators";

    /** Per-job metadata hash. Full key: {@code job:meta:{jobId}} */
    public static final String JOB_META_KEY_PREFIX = "job:meta:";

    // Stream message field names — typed constants to prevent typos.
    public static final String FIELD_CHUNK_ID     = "chunkId";
    public static final String FIELD_JOB_ID       = "jobId";
    public static final String FIELD_CACHE_KEY    = "cacheKey";
    public static final String FIELD_FILE_PATH    = "filePath";
    public static final String FIELD_LANGUAGE     = "language";
    public static final String FIELD_CHUNK_INDEX  = "chunkIndex";
    public static final String FIELD_TOTAL_CHUNKS = "totalChunks";
    public static final String FIELD_RETRY_COUNT  = "retryCount";

    /** TTL for job:meta and job:total keys — same as progress tracker. */
    private static final Duration JOB_META_TTL = Duration.ofHours(48);

    private final StringRedisTemplate  redisTemplate;
    private final ChunkStoreService    chunkStore;
    private final JobProgressTracker   progressTracker;

    public ChunkStreamPublisher(StringRedisTemplate redisTemplate,
                                ChunkStoreService chunkStore,
                                JobProgressTracker progressTracker) {
        this.redisTemplate   = redisTemplate;
        this.chunkStore      = chunkStore;
        this.progressTracker = progressTracker;
    }

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /**
     * Publishes all chunks to the pending stream.
     * @return the number of chunks successfully enqueued
     */
    public int publishAll(List<FileChunk> chunks) {
        if (chunks.isEmpty()) return 0;

        UUID   jobId   = chunks.get(0).jobId();
        String repoUrl = chunks.get(0).repoUrl();
        String commitSha = chunks.get(0).commitSha();

        progressTracker.recordTotal(jobId, chunks.size());

        writeJobMeta(jobId, repoUrl, commitSha);

        int published = 0;
        for (FileChunk chunk : chunks) {
            try {
                publish(chunk);
                published++;
            } catch (Exception e) {
                log.error("Failed to publish chunk {} (file={}): {}",
                        chunk.chunkId(), chunk.filePath(), e.getMessage());
                // Continue — partial publish is better than aborting the whole job.
                // The failed chunk is effectively lost for this run; the job's retry
                // logic will re-ingest the whole repo if the final chunk count is wrong.
            }
        }
        log.info("[{}] Published {}/{} chunks to {}",
                chunks.isEmpty() ? "?" : chunks.get(0).jobId(),
                published, chunks.size(), STREAM_PENDING);
        return published;
    }

    /** Publishes a single chunk with retryCount=0. */
    public RecordId publish(FileChunk chunk) {
        return publish(chunk, 0);
    }

    /** Re-enqueues a failed message with an incremented retry count. */
    public RecordId republish(Map<String, String> originalFields, int newRetryCount) {
        Map<String, String> fields = new LinkedHashMap<>(originalFields);
        fields.put(FIELD_RETRY_COUNT, String.valueOf(newRetryCount));
        // New chunkId so the message gets a fresh stream ID and isn't confused
        // with the original in XPENDING.
        fields.put(FIELD_CHUNK_ID, UUID.randomUUID().toString());
        return xadd(STREAM_PENDING, fields);
    }

    /** Sends a message to the dead-letter stream, preserving all original fields. */
    public RecordId publishToDlq(Map<String, String> fields) {
        return xadd(STREAM_DLQ, fields);
    }

    private RecordId publish(FileChunk chunk, int retryCount) {
        chunkStore.saveContent(chunk.cacheKey(), chunk.content());

        Map<String, String> fields = new LinkedHashMap<>();
        fields.put(FIELD_CHUNK_ID,     chunk.chunkId().toString());
        fields.put(FIELD_JOB_ID,       chunk.jobId().toString());
        fields.put(FIELD_CACHE_KEY,    chunk.cacheKey());
        fields.put(FIELD_FILE_PATH,    chunk.filePath());
        fields.put(FIELD_LANGUAGE,     chunk.language());
        fields.put(FIELD_CHUNK_INDEX,  String.valueOf(chunk.chunkIndex()));
        fields.put(FIELD_TOTAL_CHUNKS, String.valueOf(chunk.totalChunks()));
        fields.put(FIELD_RETRY_COUNT,  String.valueOf(retryCount));

        RecordId id = xadd(STREAM_PENDING, fields);
        log.trace("Enqueued chunk {} (file={}) as stream message {}",
                chunk.chunkId(), chunk.filePath(), id);
        return id;
    }

    /** Writes repoUrl and commitSha to {@code job:meta:{jobId}} for the aggregator. */
    private void writeJobMeta(UUID jobId, String repoUrl, String commitSha) {
        String key = JOB_META_KEY_PREFIX + jobId;
        redisTemplate.opsForHash().put(key, "repoUrl", repoUrl != null ? repoUrl : "");
        redisTemplate.opsForHash().put(key, "commitSha", commitSha != null ? commitSha : "unknown");
        redisTemplate.expire(key, JOB_META_TTL);
    }

    private RecordId xadd(String stream, Map<String, String> fields) {
        MapRecord<String, String, String> record = MapRecord.create(stream, fields);
        RecordId id = redisTemplate.opsForStream().add(record);
        if (id == null) {
            throw new IllegalStateException("XADD to " + stream + " returned null record ID");
        }
        return id;
    }
}
