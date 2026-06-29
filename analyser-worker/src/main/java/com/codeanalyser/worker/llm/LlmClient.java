package com.codeanalyser.worker.llm;

/**
 * Provider-agnostic interface for sending a code chunk to an LLM and receiving
 * a structured analysis back.
 *
 * <h3>Why an interface here?</h3>
 * The Strategy pattern lets {@link LlmConfig} wire any implementation at
 * startup based on {@code llm.provider}. The rest of the codebase — especially
 * {@link com.codeanalyser.worker.analysis.ChunkAnalysisWorker} — depends only
 * on this interface and never on a concrete provider class. Swapping from Ollama
 * to OpenAI requires changing one line in {@code application.yml}.
 *
 * <h3>Implementations</h3>
 * <ul>
 *   <li>{@link OllamaLlmClient}   — local Ollama, free, no API key</li>
 *   <li>{@link OpenAiLlmClient}   — OpenAI-compatible API (OpenAI, Groq, Together AI, etc.)</li>
 *   <li>{@link MockLlmClient}     — canned responses, no network, for demos and CI</li>
 *   <li>AnthropicLlmClient        — TODO: native Messages API client</li>
 *   <li>GeminiLlmClient           — TODO: native generateContent API client</li>
 * </ul>
 * All implementations are wrapped by {@link RateLimitedLlmClient} before being
 * injected into the worker — callers never rate-limit themselves.
 */
public interface LlmClient {

    /**
     * Analyses a single source-code chunk.
     * Used by {@link com.codeanalyser.worker.analysis.ChunkAnalysisWorker}.
     *
     * @throws LlmException if the provider returns an error or the request
     *                      times out. The worker treats this as a retryable failure.
     */
    LlmResponse analyse(LlmRequest request) throws LlmException;

    /**
     * Sends a free-form synthesis prompt to the LLM and returns the response.
     * Used by {@link com.codeanalyser.worker.aggregation.HierarchicalAggregator}
     * for both aggregation passes (module summary and final document assembly).
     *
     * <p>Unlike {@link #analyse}, this method does not impose any prompt
     * structure — the caller constructs the full prompt. The rate limiter
     * still applies.
     *
     * @throws LlmException if the provider returns an error or times out.
     */
    LlmResponse synthesise(String prompt) throws LlmException;

    /** Thrown on any LLM call failure. Always retryable by the caller. */
    class LlmException extends Exception {
        public LlmException(String message)                  { super(message); }
        public LlmException(String message, Throwable cause) { super(message, cause); }
    }
}
