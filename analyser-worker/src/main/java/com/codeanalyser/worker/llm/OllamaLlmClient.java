package com.codeanalyser.worker.llm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;

/**
 * LLM client backed by a local Ollama instance. Uses the native {@code POST /api/chat}
 * endpoint with {@code "stream": false}.
 */
public class OllamaLlmClient implements LlmClient {

    private static final Logger log = LoggerFactory.getLogger(OllamaLlmClient.class);

    private final String       model;
    private final RestClient   restClient;
    private final ObjectMapper objectMapper;

    /** Used by {@link LlmConfig} — reads settings from {@code application.yml}. */
    public OllamaLlmClient(LlmProperties props, ObjectMapper objectMapper) {
        this(props.baseUrl(), props.model(), objectMapper);
    }

    /** Used by {@link LlmClientFactory} — builds a per-job client from user-supplied config. */
    public OllamaLlmClient(String baseUrl, String model, ObjectMapper objectMapper) {
        this.model        = model;
        this.objectMapper = objectMapper;
        this.restClient   = RestClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader("Content-Type", "application/json")
                .build();
    }

    @Override
    public LlmResponse analyse(LlmRequest request) throws LlmException {
        String prompt = buildPrompt(request);
        log.debug("Calling Ollama model={} for file={} chunk={}/{}",
                model, request.filePath(), request.chunkIndex() + 1, request.totalChunks());

        OllamaChatRequest body = new OllamaChatRequest(
                model,
                new OllamaMessage[]{ new OllamaMessage("user", prompt) },
                false,  // stream: false — we want the full response in one HTTP reply
                256     // num_predict: cap output tokens — 3-5 sentences needs ~150 max
        );

        try {
            String json = restClient.post()
                    .uri("/api/chat")
                    .body(objectMapper.writeValueAsString(body))
                    .retrieve()
                    .body(String.class);

            OllamaChatResponse response = objectMapper.readValue(json, OllamaChatResponse.class);

            if (response == null || response.message() == null) {
                throw new LlmException("Ollama returned null or empty response for " + request.filePath());
            }

            String text = response.message().content();
            log.debug("Ollama analysis complete for {} ({} chars)", request.filePath(), text.length());
            return LlmResponse.of(text);

        } catch (LlmException e) {
            throw e;
        } catch (Exception e) {
            throw new LlmException("Ollama request failed for " + request.filePath() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public LlmResponse synthesise(String prompt) throws LlmException {
        log.debug("Calling Ollama synthesise, model={}, prompt-length={}", model, prompt.length());
        OllamaChatRequest body = new OllamaChatRequest(
                model,
                new OllamaMessage[]{ new OllamaMessage("user", prompt) },
                false,
                512     // synthesis gets more tokens — it's producing a full summary
        );
        try {
            String json = restClient.post()
                    .uri("/api/chat")
                    .body(objectMapper.writeValueAsString(body))
                    .retrieve()
                    .body(String.class);
            OllamaChatResponse response = objectMapper.readValue(json, OllamaChatResponse.class);
            if (response == null || response.message() == null) {
                throw new LlmException("Ollama returned null response for synthesis prompt");
            }
            return LlmResponse.of(response.message().content());
        } catch (LlmException e) {
            throw e;
        } catch (Exception e) {
            throw new LlmException("Ollama synthesise request failed: " + e.getMessage(), e);
        }
    }

    private String buildPrompt(LlmRequest req) {
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

    private record OllamaChatRequest(String model, OllamaMessage[] messages, boolean stream,
                                     @com.fasterxml.jackson.annotation.JsonProperty("num_predict") int numPredict) {}
    private record OllamaMessage(String role, String content) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record OllamaChatResponse(OllamaMessage message, boolean done) {}
}
