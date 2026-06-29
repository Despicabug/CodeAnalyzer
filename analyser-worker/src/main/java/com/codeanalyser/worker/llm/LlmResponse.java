package com.codeanalyser.worker.llm;

/**
 * The structured analysis returned by any {@link LlmClient} implementation.
 *
 * @param analysisText  The LLM's raw analysis of the chunk. Plain prose —
 *                      no structured parsing imposed at this stage. The
 *                      aggregator (Milestone 3) will combine these into the
 *                      final architecture document.
 * @param confidence    A 0.0–1.0 quality score.
 *                      <b>Milestone 2 stub:</b> all implementations return
 *                      {@code 1.0} here. A real scorer (e.g. checking response
 *                      length, detecting refusal phrases, or using a second
 *                      LLM call) is a Milestone-N TODO for the eval/guardrail
 *                      pass.
 * @param promptTokens  Approximate input token count (for cost/quota tracking).
 *                      Providers that don't expose this return {@code -1}.
 * @param completionTokens Approximate output token count. {@code -1} if unavailable.
 */
public record LlmResponse(
        String analysisText,
        double confidence,
        int    promptTokens,
        int    completionTokens
) {
    /** Convenience factory for implementations that don't have token counts. */
    public static LlmResponse of(String text) {
        return new LlmResponse(text, 1.0, -1, -1);
    }
}
