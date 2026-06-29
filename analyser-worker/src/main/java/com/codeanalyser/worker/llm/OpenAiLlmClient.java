package com.codeanalyser.worker.llm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;

import java.util.List;

/**
 * LLM client backed by the OpenAI Chat Completions API ({@code POST /v1/chat/completions}).
 * Compatible with any OpenAI-compatible endpoint — point {@code LLM_BASE_URL} at
 * Groq, Together AI, Azure OpenAI, etc.
 */
public class OpenAiLlmClient implements LlmClient {

    private static final Logger log = LoggerFactory.getLogger(OpenAiLlmClient.class);

    static final String DEFAULT_BASE_URL = "https://api.openai.com";

    private final String       model;
    private final RestClient   restClient;
    private final ObjectMapper objectMapper;

    /**
     * Used by {@link LlmConfig} — reads settings from {@code application.yml}.
     * If the stored base URL is still the Ollama default, the caller is expected
     * to pass in {@link #DEFAULT_BASE_URL} instead.
     */
    public OpenAiLlmClient(LlmProperties props, ObjectMapper objectMapper, String baseUrl) {
        this(props.apiKey(), props.model(), baseUrl, objectMapper);
    }

    /**
     * Used by {@link LlmClientFactory} — builds a per-job client from user-supplied credentials.
     */
    public OpenAiLlmClient(String apiKey, String model, String baseUrl, ObjectMapper objectMapper) {
        this.model        = model;
        this.objectMapper = objectMapper;
        this.restClient   = RestClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader("Content-Type", "application/json")
                .defaultHeader("Authorization", "Bearer " + apiKey)
                .build();
    }

    // ---------------------------------------------------------------------------
    // LlmClient implementation
    // ---------------------------------------------------------------------------

    @Override
    public LlmResponse analyse(LlmRequest request) throws LlmException {
        log.debug("Calling OpenAI model={} for file={} chunk={}/{}",
                model, request.filePath(),
                request.chunkIndex() + 1, request.totalChunks());
        return chat(buildAnalysePrompt(request), request.filePath());
    }

    @Override
    public LlmResponse synthesise(String prompt) throws LlmException {
        log.debug("Calling OpenAI synthesise, model={}, prompt-length={}", model, prompt.length());
        return chat(prompt, "<synthesis>");
    }

    private LlmResponse chat(String userPrompt, String contextLabel) throws LlmException {
        ChatRequest body = new ChatRequest(
                model,
                List.of(new ChatMessage("user", userPrompt)),
                0.3,   // low temperature for deterministic analysis
                2048
        );

        try {
            String json = restClient.post()
                    .uri("/v1/chat/completions")
                    .body(objectMapper.writeValueAsString(body))
                    .retrieve()
                    .body(String.class);

            ChatResponse response = objectMapper.readValue(json, ChatResponse.class);

            if (response == null || response.choices() == null || response.choices().isEmpty()) {
                throw new LlmException("OpenAI returned empty choices for " + contextLabel);
            }

            String text = response.choices().get(0).message().content();
            if (text == null || text.isBlank()) {
                throw new LlmException("OpenAI returned blank content for " + contextLabel);
            }

            log.debug("OpenAI response complete for {} ({} chars)", contextLabel, text.length());
            return LlmResponse.of(text);

        } catch (LlmException e) {
            throw e;
        } catch (Exception e) {
            throw new LlmException(
                    "OpenAI request failed for " + contextLabel + ": " + e.getMessage(), e);
        }
    }

    private static String buildAnalysePrompt(LlmRequest req) {
        String chunkContext = req.isCompleteFile()
                ? "This is the complete file."
                : "This is chunk %d of %d (the file was split due to size)."
                        .formatted(req.chunkIndex() + 1, req.totalChunks());

        return """
                You are a senior software architect performing a codebase analysis.
                Analyse the following %s source code from the file: %s
                %s

                Provide a concise technical analysis covering:
                1. What this code does and its primary responsibility
                2. Key classes, functions, or patterns it defines or uses
                3. How it likely fits into the broader application architecture
                4. Any notable design decisions, dependencies, or concerns

                Be technical and precise. Aim for 3-5 sentences. Do not explain \
                basic language syntax.

                ```%s
                %s
                ```
                """.formatted(req.language(), req.filePath(), chunkContext,
                              req.language(), req.content());
    }

    private record ChatRequest(
            String model,
            List<ChatMessage> messages,
            double temperature,
            @JsonProperty("max_tokens") int maxTokens
    ) {}

    private record ChatMessage(String role, String content) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ChatResponse(List<Choice> choices) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Choice(ChatMessage message) {}
}
