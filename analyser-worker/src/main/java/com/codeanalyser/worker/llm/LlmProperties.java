package com.codeanalyser.worker.llm;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * LLM provider settings bound from {@code application.yml} under the {@code llm} prefix.
 * Supported providers: {@code ollama}, {@code openai}, {@code mock}.
 */
@ConfigurationProperties(prefix = "llm")
public record LlmProperties(
        @DefaultValue("ollama")                  String provider,
        @DefaultValue("llama3.2")                String model,
        @DefaultValue("http://localhost:11434")  String baseUrl,
        @DefaultValue("")                        String apiKey,
        @DefaultValue("2.0")                     double requestsPerSecond,
        @DefaultValue("120")                     int    timeoutSeconds
) {
    public enum Provider { OLLAMA, OPENAI, ANTHROPIC, GEMINI, MOCK }

    public Provider resolvedProvider() {
        return switch (provider.toLowerCase()) {
            case "openai"    -> Provider.OPENAI;
            case "anthropic" -> Provider.ANTHROPIC;
            case "gemini"    -> Provider.GEMINI;
            case "mock"      -> Provider.MOCK;
            default          -> Provider.OLLAMA;
        };
    }
}
