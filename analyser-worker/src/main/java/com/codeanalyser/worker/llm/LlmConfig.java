package com.codeanalyser.worker.llm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the correct {@link LlmClient} implementation based on {@code llm.provider}
 * and wraps it in a {@link RateLimitedLlmClient}. Switching providers is a one-line
 * env var change — nothing else in the codebase references a concrete provider class.
 */
@Configuration
@EnableConfigurationProperties(LlmProperties.class)
public class LlmConfig {

    private static final Logger log = LoggerFactory.getLogger(LlmConfig.class);

    @Bean
    public LlmClient llmClient(LlmProperties props, ObjectMapper objectMapper) {
        LlmClient raw = switch (props.resolvedProvider()) {
            case OLLAMA -> {
                log.info("LLM provider: Ollama @ {} model={}", props.baseUrl(), props.model());
                yield new OllamaLlmClient(props, objectMapper);
            }
            case OPENAI -> {
                // If LLM_BASE_URL still has the Ollama default, swap in OpenAI's URL.
                // An explicit override (Groq, Together AI, etc.) is respected as-is.
                String baseUrl = props.baseUrl().contains("11434")
                        ? OpenAiLlmClient.DEFAULT_BASE_URL
                        : props.baseUrl();
                log.info("LLM provider: OpenAI-compatible @ {} model={}", baseUrl, props.model());
                yield new OpenAiLlmClient(props, objectMapper, baseUrl);
            }
            case ANTHROPIC -> {
                // TODO: implement AnthropicLlmClient
                // Anthropic uses the Messages API (/v1/messages) with a different
                // request/response shape and requires the x-api-key header.
                // For now, fall back to OpenAI-compatible if an API key is present.
                log.warn("Anthropic provider not yet implemented — falling back to OpenAI-compatible");
                yield new OpenAiLlmClient(props, objectMapper, OpenAiLlmClient.DEFAULT_BASE_URL);
            }
            case GEMINI -> {
                // TODO: implement GeminiLlmClient
                // Gemini uses the generateContent API with a distinct schema.
                log.warn("Gemini provider not yet implemented — falling back to Ollama");
                yield new OllamaLlmClient(props, objectMapper);
            }
            case MOCK -> {
                // No-network stub for demos and CI — returns canned responses.
                // Activate with LLM_PROVIDER=mock (no API key required).
                log.warn("LLM provider: MOCK — canned responses only, no real LLM calls");
                yield new MockLlmClient();
            }
        };

        return new RateLimitedLlmClient(raw, props.requestsPerSecond());
    }
}
