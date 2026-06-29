package com.codeanalyser.worker.llm;

/**
 * Everything an LLM implementation needs to produce a code analysis.
 *
 * <p>All fields are passed into the prompt template inside each
 * {@link LlmClient} implementation. Keeping them here (rather than building
 * the prompt in the worker) means the prompt template is co-located with the
 * provider-specific API call, which makes it easy to tune per-provider.
 *
 * @param filePath    Repository-relative path (e.g. {@code src/main/java/Foo.java}).
 *                    Tells the LLM where in the project this file lives.
 * @param language    Detected language label (e.g. {@code java}, {@code python}).
 *                    Used in the prompt: "The following is {language} code."
 * @param content     The actual source text of the chunk.
 * @param chunkIndex  Zero-based chunk index within the file. If {@code > 0},
 *                    the prompt notes this is a continuation of a larger file.
 * @param totalChunks Total chunks this file was split into. Used to tell the
 *                    LLM that it is seeing a partial file when {@code > 1}.
 */
public record LlmRequest(
        String filePath,
        String language,
        String content,
        int    chunkIndex,
        int    totalChunks
) {
    /** True when the LLM is seeing the complete file (no splitting). */
    public boolean isCompleteFile() {
        return totalChunks == 1;
    }
}
