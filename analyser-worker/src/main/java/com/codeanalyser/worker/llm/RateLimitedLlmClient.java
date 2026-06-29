package com.codeanalyser.worker.llm;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorator that enforces a per-second request rate on any {@link LlmClient}
 * using a Guava token-bucket limiter.
 *
 * <p>In-process only — each pod has its own limiter. A Redis sliding-window
 * counter would be needed for correct rate limiting across multiple pods.
 */
@SuppressWarnings("UnstableApiUsage") // RateLimiter is @Beta but stable in practice
public class RateLimitedLlmClient implements LlmClient {

    private static final Logger log = LoggerFactory.getLogger(RateLimitedLlmClient.class);

    private final LlmClient   delegate;
    private final RateLimiter rateLimiter;
    private final double      permitsPerSecond;

    public RateLimitedLlmClient(LlmClient delegate, double requestsPerSecond) {
        this.delegate         = delegate;
        this.permitsPerSecond = requestsPerSecond;
        this.rateLimiter      = RateLimiter.create(requestsPerSecond);
        log.info("LLM rate limiter initialised at {:.1f} req/s (in-process; see TODO for multi-pod)",
                requestsPerSecond);
    }

    @Override
    public LlmResponse synthesise(String prompt) throws LlmException {
        double waitSeconds = rateLimiter.acquire();
        if (waitSeconds > 0.1) {
            log.debug("Rate limiter waited {:.2f}s before synthesise call", waitSeconds);
        }
        return delegate.synthesise(prompt);
    }

    @Override
    public LlmResponse analyse(LlmRequest request) throws LlmException {
        double waitSeconds = rateLimiter.acquire();
        if (waitSeconds > 0.1) {
            log.debug("Rate limiter waited {:.2f}s before LLM call for {}",
                    waitSeconds, request.filePath());
        }
        return delegate.analyse(request);
    }

    public double permitsPerSecond() { return permitsPerSecond; }
}
