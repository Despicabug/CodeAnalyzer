package com.codeanalyser.worker.analysis;

import com.codeanalyser.worker.redis.ChunkStreamPublisher;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Creates Redis Streams and consumer groups on startup. Runs before the
 * @Scheduled polling loops start. BUSYGROUP errors are swallowed — idempotent
 * on container restarts.
 */
@Component
public class StreamConsumerSetup {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumerSetup.class);

    private final StringRedisTemplate redisTemplate;

    public StreamConsumerSetup(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void createStreamsAndGroups() {
        ensureGroup(ChunkStreamPublisher.STREAM_PENDING,  ChunkStreamPublisher.GROUP_ANALYSERS);
        ensureGroup(ChunkStreamPublisher.STREAM_ANALYSED, ChunkStreamPublisher.GROUP_AGGREGATORS);
        ensureStreamExists(ChunkStreamPublisher.STREAM_DLQ); // no consumer group; inspected manually

        log.info("Redis Streams and consumer groups verified.");
    }

    private void ensureGroup(String stream, String group) {
        try {
            // ReadOffset "0" replays unacked messages from a previous run on restart.
            redisTemplate.opsForStream()
                    .createGroup(stream, ReadOffset.from("0"), group);
            log.info("Created consumer group '{}' on stream '{}'", group, stream);
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : "";
            if (msg.contains("BUSYGROUP")) {
                log.debug("Consumer group '{}' on '{}' already exists", group, stream);
            } else if (msg.contains("ERR no such key")) {
                ensureStreamExists(stream);
                ensureGroup(stream, group);
            } else {
                log.error("Unexpected error creating group '{}' on '{}': {}", group, stream, msg);
                throw new IllegalStateException("Failed to set up consumer group: " + msg, e);
            }
        }
    }

    private void ensureStreamExists(String stream) {
        try {
            redisTemplate.opsForStream().createGroup(stream, ReadOffset.latest(), "_init_probe");
            redisTemplate.opsForStream().destroyGroup(stream, "_init_probe");
        } catch (Exception e) {
            // BUSYGROUP means stream already exists — fine.
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                log.debug("ensureStreamExists '{}' probe result: {}", stream, e.getMessage());
            }
        }
    }
}
