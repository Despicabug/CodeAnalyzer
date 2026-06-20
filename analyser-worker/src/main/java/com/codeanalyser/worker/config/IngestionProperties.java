package com.codeanalyser.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * All tunable knobs for the ingestion pipeline, bound from application.yml
 * under the {@code ingestion} prefix.
 *
 * <p>Using @ConfigurationProperties instead of @Value means:
 * <ul>
 *   <li>All settings are grouped in one class — easy to find and document.</li>
 *   <li>Spring validates types at startup (a bad integer in YAML fails fast).</li>
 *   <li>IDE auto-completes {@code ingestion.*} properties.</li>
 * </ul>
 *
 * <p>Example application.yml:
 * <pre>
 * ingestion:
 *   max-repo-size-mb: 150
 *   clone-timeout-seconds: 120
 *   max-concurrent-clones: 3
 *   max-chunk-bytes: 8192
 *   max-retries: 3
 *   github-api-token: ${GITHUB_TOKEN:}   # empty = unauthenticated (60 req/hr)
 * </pre>
 */
@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(

        /**
         * Repos larger than this are rejected before any disk is touched.
         * GitHub reports size in KB; we compare against maxRepoSizeMb * 1024.
         * Default 200 MB is generous but protects against monorepos on free hosting.
         */
        @DefaultValue("200") int maxRepoSizeMb,

        /**
         * Wall-clock timeout for the `git clone` subprocess.
         * Prevents a slow network or a huge repo from holding a thread forever.
         */
        @DefaultValue("120") int cloneTimeoutSeconds,

        /**
         * Maximum number of git clones that may exist on disk simultaneously.
         * This is the primary bound on peak disk usage.
         * Formula: peak disk ≈ maxConcurrentClones × maxRepoSizeMb
         * With defaults: 3 × 200 MB = 600 MB maximum.
         */
        @DefaultValue("3") int maxConcurrentClones,

        /**
         * Maximum size of a single FileChunk's content in bytes.
         * Files larger than this are split at line boundaries.
         * ~8 KB is a safe default that fits within most LLM context windows
         * while keeping chunks semantically meaningful.
         *
         * TODO (Milestone N): replace line-boundary splitting with heuristic
         * structural splitting to avoid cutting mid-function.
         */
        @DefaultValue("8192") int maxChunkBytes,

        /**
         * Number of times a failed job is retried before being dead-lettered.
         */
        @DefaultValue("3") int maxRetries,

        /**
         * Optional GitHub personal access token.
         * Unauthenticated: 60 API requests/hour (enough for pre-clone size checks).
         * Authenticated: 5000 requests/hour.
         * Leave blank for the free demo; power users supply their own token.
         */
        @DefaultValue("") String githubApiToken
) {}
