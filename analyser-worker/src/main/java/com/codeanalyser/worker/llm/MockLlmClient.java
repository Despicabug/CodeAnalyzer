package com.codeanalyser.worker.llm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stub {@link LlmClient} that returns canned responses without making any
 * network calls. Intended for local development, CI, and portfolio demos
 * where no LLM API key is available.
 *
 * <h3>Activation</h3>
 * <pre>
 *   LLM_PROVIDER=mock   # in .env or environment
 * </pre>
 * All other LLM env vars are ignored when the mock provider is active.
 *
 * <h3>What it does</h3>
 * <ul>
 *   <li>{@link #analyse} returns a per-file stub analysis that includes the
 *       real file path and language so the pipeline output looks plausible.</li>
 *   <li>{@link #synthesise} returns a canned architecture summary so the full
 *       pipeline can complete end-to-end and the result tabs render real content.</li>
 * </ul>
 *
 * <h3>What it proves</h3>
 * Running with the mock provider exercises every part of the pipeline except the
 * LLM call itself: Git clone, chunking, Redis stream publish, consumer group
 * polling, retry/DLQ logic, aggregation trigger, result storage, SSE events,
 * and the final UI render. This is enough to demonstrate the architecture works.
 *
 * <p><strong>Note:</strong> mock output is clearly labelled in every response
 * so it cannot be mistaken for real LLM output.
 */
public class MockLlmClient implements LlmClient {

    private static final Logger log = LoggerFactory.getLogger(MockLlmClient.class);

    // Slight artificial delay (ms) so the demo UI progress bar moves visibly.
    private static final long MOCK_DELAY_MS = 80;

    public MockLlmClient() {
        log.warn("=================================================");
        log.warn("  MockLlmClient active — no real LLM calls made.");
        log.warn("  Set LLM_PROVIDER=ollama or LLM_PROVIDER=openai");
        log.warn("  with a valid model/key for real analysis.");
        log.warn("=================================================");
    }

    // ---------------------------------------------------------------------------
    // LlmClient implementation
    // ---------------------------------------------------------------------------

    @Override
    public LlmResponse analyse(LlmRequest request) throws LlmException {
        simulateLatency();
        log.debug("[MOCK] analyse: file={} chunk={}/{} lang={}",
                request.filePath(), request.chunkIndex() + 1,
                request.totalChunks(), request.language());

        String chunkNote = request.isCompleteFile()
                ? "complete file"
                : "chunk " + (request.chunkIndex() + 1) + " of " + request.totalChunks();

        String text = String.format(
                "[MOCK ANALYSIS — no real LLM was called] " +
                "This is a %s source file (%s). " +
                "It defines classes or functions that contribute to the application's " +
                "core functionality. The file follows standard %s conventions and " +
                "interacts with other components via well-defined interfaces. " +
                "No real architectural conclusions can be drawn from this stub.",
                request.language(), chunkNote, request.language());

        return LlmResponse.of(text);
    }

    @Override
    public LlmResponse synthesise(String prompt) throws LlmException {
        simulateLatency();
        log.debug("[MOCK] synthesise: prompt-length={}", prompt.length());

        // Return a response that includes the section delimiters HierarchicalAggregator
        // expects, so the result tabs render properly even with mock data.
        String text = """
                --- ARCHITECTURE ---
                [MOCK ARCHITECTURE OVERVIEW — no real LLM was called]

                This is a Spring Boot application built on a Redis Streams pipeline. \
                The system accepts a GitHub repository URL, clones it, splits source \
                files into chunks, and dispatches them to an LLM for per-chunk analysis. \
                A hierarchical aggregator then synthesises chunk analyses into three \
                developer documents: an architecture overview, an onboarding guide, and \
                a start map.

                The codebase is organised into three Maven modules: analyser-common \
                (shared models), analyser-worker (pipeline and LLM logic), and \
                analyser-api (HTTP layer and static UI). All modules are packaged into \
                a single Spring Boot fat JAR for deployment simplicity, while the \
                internal boundaries allow splitting into separate containers later.

                Key patterns: Strategy (LlmClient interface with provider implementations), \
                Redis Streams for decoupled async processing, SSE for real-time browser \
                updates, and a two-pass hierarchical LLM aggregation to stay within \
                context limits at any repo size.

                --- ONBOARDING ---
                [MOCK ONBOARDING GUIDE — no real LLM was called]

                This system analyses a Git repository and produces three developer \
                documents from it. The core concepts to understand are: (1) the ingestion \
                pipeline that clones and chunks source files; (2) the Redis Streams queue \
                that decouples ingestion from LLM analysis; (3) the LlmClient strategy \
                pattern that abstracts provider selection; (4) the hierarchical aggregator \
                that runs two LLM passes to produce the final output.

                Start reading at ApiApplication, then AnalysisService to understand job \
                submission, then IngestionPipeline for the clone/chunk flow, then \
                ChunkAnalysisWorker for the stream consumer, and finally \
                HierarchicalAggregator for the two-pass synthesis logic.

                --- START MAP ---
                [MOCK START MAP — no real LLM was called]

                1. analyser-api/src/main/java/com/codeanalyser/api/ApiApplication.java — Application entry point
                2. analyser-api/src/main/java/com/codeanalyser/api/service/AnalysisService.java — Job submission and idempotency
                3. analyser-worker/src/main/java/com/codeanalyser/worker/ingestion/IngestionPipeline.java — Clone → chunk → publish orchestration
                4. analyser-worker/src/main/java/com/codeanalyser/worker/analysis/ChunkAnalysisWorker.java — Redis stream consumer and LLM dispatch
                5. analyser-worker/src/main/java/com/codeanalyser/worker/aggregation/HierarchicalAggregator.java — Two-pass LLM synthesis
                6. analyser-worker/src/main/java/com/codeanalyser/worker/llm/LlmClient.java — Provider-agnostic LLM interface
                7. analyser-worker/src/main/java/com/codeanalyser/worker/llm/LlmConfig.java — Strategy wiring at startup
                8. analyser-api/src/main/resources/static/index.html — Single-page UI with SSE progress tracking
                """;

        return LlmResponse.of(text);
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    private static void simulateLatency() {
        try {
            Thread.sleep(MOCK_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
