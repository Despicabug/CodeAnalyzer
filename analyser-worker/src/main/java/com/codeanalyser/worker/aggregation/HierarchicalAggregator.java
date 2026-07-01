package com.codeanalyser.worker.aggregation;

import com.codeanalyser.common.model.ChunkAnalysisResult;
import com.codeanalyser.common.model.JobResult;
import com.codeanalyser.common.model.ModuleSummary;
import com.codeanalyser.worker.llm.LlmClient;
import com.codeanalyser.worker.redis.ChunkStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Produces the three final documents from chunk analyses using two LLM passes.
 *
 * <p>Pass 1: chunks are grouped by top-level directory; one LLM call per group
 * produces a {@link ModuleSummary}. Pass 2: all module summaries feed a single
 * prompt that returns the architecture overview, onboarding guide, and start map,
 * delimited by exact markers so parsing is mechanical rather than heuristic.
 *
 * <p>The {@link LlmClient} is passed in at call time rather than injected at
 * construction so that {@link AggregatorWorker} can supply a per-job client
 * (user's own API key) via {@link com.codeanalyser.worker.llm.LlmClientFactory}.
 *
 * <p>Delimiter-based parsing is used instead of JSON because small models
 * frequently produce malformed JSON; missing delimiters fall back to raw text.
 */
@Service
public class HierarchicalAggregator {

    private static final Logger log = LoggerFactory.getLogger(HierarchicalAggregator.class);

    private static final String DELIMITER_ARCH       = "--- ARCHITECTURE ---";
    private static final String DELIMITER_ONBOARDING = "--- ONBOARDING ---";
    private static final String DELIMITER_START_MAP  = "--- START MAP ---";

    private final ChunkStoreService chunkStore;

    public HierarchicalAggregator(ChunkStoreService chunkStore) {
        this.chunkStore = chunkStore;
    }

    /**
     * Runs both aggregation passes and returns the completed {@link JobResult}.
     *
     * @param llmClient the client to use for LLM calls — resolved by the caller
     *                  from {@link com.codeanalyser.worker.llm.LlmClientFactory}
     *                  so that per-job API key overrides are honoured
     */
    public JobResult aggregate(UUID jobId, String repoUrl, String commitSha,
                               Set<String> cacheKeys, LlmClient llmClient)
            throws LlmClient.LlmException {

        log.info("[{}] Starting aggregation for {} with {} chunk keys", jobId, repoUrl, cacheKeys.size());

        List<ChunkAnalysisResult> analyses = fetchAnalyses(jobId, cacheKeys);
        if (analyses.isEmpty()) {
            throw new LlmClient.LlmException("No chunk analyses found in Redis for job " + jobId
                    + " — all content keys may have expired");
        }

        int totalFiles = (int) analyses.stream().map(ChunkAnalysisResult::filePath).distinct().count();
        log.info("[{}] Fetched {} analyses ({} unique files)", jobId, analyses.size(), totalFiles);

        Map<String, List<ChunkAnalysisResult>> groups = groupByTopLevelDir(analyses);
        List<ModuleSummary> moduleSummaries = new ArrayList<>();

        for (var entry : groups.entrySet()) {
            String groupName = entry.getKey();
            List<ChunkAnalysisResult> groupAnalyses = entry.getValue();
            log.info("[{}] Pass 1: summarising module '{}' ({} chunks)", jobId, groupName, groupAnalyses.size());

            String prompt = buildModulePrompt(groupName, groupAnalyses);
            String summaryText = llmClient.synthesise(prompt).analysisText();

            String langBreakdown = languageBreakdown(groupAnalyses);
            List<String> filePaths = groupAnalyses.stream()
                    .map(ChunkAnalysisResult::filePath)
                    .distinct().sorted().toList();

            moduleSummaries.add(new ModuleSummary(groupName, filePaths, langBreakdown, summaryText));
        }

        log.info("[{}] Pass 1 complete: {} module summaries", jobId, moduleSummaries.size());

        log.info("[{}] Pass 2: synthesising final documents", jobId);
        String finalPrompt = buildFinalPrompt(repoUrl, moduleSummaries, totalFiles, analyses);
        String rawResponse = llmClient.synthesise(finalPrompt).analysisText();

        // Parse the three sections from the delimited response.
        String architectureOverview = extractSection(rawResponse, DELIMITER_ARCH,      DELIMITER_ONBOARDING);
        String onboardingGuide      = extractSection(rawResponse, DELIMITER_ONBOARDING, DELIMITER_START_MAP);
        String startMap             = extractSection(rawResponse, DELIMITER_START_MAP,  null);

        JobResult result = new JobResult(
                jobId, repoUrl, commitSha,
                architectureOverview, onboardingGuide, startMap,
                moduleSummaries, totalFiles, Instant.now()
        );

        log.info("[{}] Aggregation complete: arch={} chars, onboarding={} chars, startMap={} chars",
                jobId,
                architectureOverview.length(),
                onboardingGuide.length(),
                startMap.length());

        return result;
    }

    private List<ChunkAnalysisResult> fetchAnalyses(UUID jobId, Set<String> cacheKeys) {
        List<ChunkAnalysisResult> results = new ArrayList<>();
        for (String ck : cacheKeys) {
            chunkStore.getAnalysis(ck).ifPresentOrElse(
                    results::add,
                    () -> log.warn("[{}] Analysis missing for cacheKey={} (TTL may have expired)", jobId, ck)
            );
        }
        return results;
    }

    /** Groups analyses by first path segment. Root-level files go into "(root)". */
    private Map<String, List<ChunkAnalysisResult>> groupByTopLevelDir(
            List<ChunkAnalysisResult> analyses) {

        Map<String, List<ChunkAnalysisResult>> groups = new LinkedHashMap<>();
        for (ChunkAnalysisResult a : analyses) {
            String topLevel = topLevelSegment(a.filePath());
            groups.computeIfAbsent(topLevel, k -> new ArrayList<>()).add(a);
        }
        return groups;
    }

    private static String topLevelSegment(String filePath) {
        int slash = filePath.indexOf('/');
        return (slash > 0) ? filePath.substring(0, slash) : "(root)";
    }

    private static String languageBreakdown(List<ChunkAnalysisResult> analyses) {
        return analyses.stream()
                .collect(Collectors.groupingBy(
                        a -> inferLanguage(a.filePath()), Collectors.counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .map(e -> e.getKey() + " (" + e.getValue() + ")")
                .collect(Collectors.joining(", "));
    }

    private static String inferLanguage(String filePath) {
        int dot = filePath.lastIndexOf('.');
        return (dot >= 0) ? filePath.substring(dot + 1).toLowerCase() : "unknown";
    }

    private String buildModulePrompt(String groupName, List<ChunkAnalysisResult> analyses) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are analysing a software codebase.\n");
        sb.append("The following are analyses of files in the '").append(groupName)
          .append("' directory (").append(analyses.size()).append(" files):\n\n");

        analyses.stream()
                .sorted((a, b) -> a.filePath().compareTo(b.filePath()))
                .limit(20)
                .forEach(a -> sb.append("FILE: ").append(a.filePath()).append("\n")
                                .append(a.analysisText()).append("\n\n"));

        sb.append("""
                Provide a 3-4 sentence technical summary of this module:
                1. Its primary responsibility in the system
                2. Key abstractions or patterns it implements
                3. How it likely interacts with other parts of the codebase
                Be concise and technical. Do not list individual files.
                """);
        return sb.toString();
    }

    private String buildFinalPrompt(String repoUrl, List<ModuleSummary> summaries,
                                    int totalFiles, List<ChunkAnalysisResult> allAnalyses) {
        String languages = allAnalyses.stream()
                .map(a -> inferLanguage(a.filePath()))
                .filter(l -> !l.equals("unknown"))
                .distinct().sorted()
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("You are creating developer documentation for a software project.\n");
        sb.append("Repository: ").append(repoUrl).append("\n");
        sb.append("Languages: ").append(languages).append("\n");
        sb.append("Total files analysed: ").append(totalFiles).append("\n\n");
        sb.append("The codebase has ").append(summaries.size()).append(" top-level modules:\n\n");

        summaries.forEach(m ->
            sb.append("MODULE: ").append(m.moduleName())
              .append(" (").append(m.fileCount()).append(" files, ").append(m.languageBreakdown()).append(")\n")
              .append(m.summaryText()).append("\n\n")
        );

        sb.append("""
                Generate three sections of developer documentation.
                Use EXACTLY these delimiters on their own lines between sections.
                Do not include any text before the first delimiter.

                """);
        sb.append(DELIMITER_ARCH).append("\n");
        sb.append("""
                Write a 3-5 paragraph technical architecture overview:
                - Overall system purpose and design philosophy
                - How the major modules relate and interact
                - Key architectural patterns (e.g. event-driven, layered, microservices)
                - Notable technology choices

                """);
        sb.append(DELIMITER_ONBOARDING).append("\n");
        sb.append("""
                Write a practical onboarding guide for a new developer:
                - What the system does in plain language
                - The 3-5 most important concepts to understand first
                - Recommended reading order for the codebase
                - How to mentally model the data flow

                """);
        sb.append(DELIMITER_START_MAP).append("\n");
        sb.append("""
                List the 5-10 most important entry-point files a developer should read first.
                Format as a numbered list: "<filepath> — <one sentence explaining why it matters>"
                Prioritise: main application classes, core domain models, primary APIs, configuration.
                """);

        return sb.toString();
    }

    /** Extracts the text between two delimiters, falling back gracefully if a delimiter is missing. */
    private static String extractSection(String response, String startDelimiter,
                                         String endDelimiter) {
        int start = response.indexOf(startDelimiter);
        if (start < 0) {
            log.warn("Section delimiter '{}' not found in LLM response, using fallback", startDelimiter);
            return startDelimiter.equals(DELIMITER_ARCH) ? response.trim()
                    : "(Section not generated — see architecture overview for full analysis)";
        }

        int contentStart = response.indexOf('\n', start + startDelimiter.length()) + 1;

        if (endDelimiter == null) {
            return response.substring(contentStart).trim();
        }

        int end = response.indexOf(endDelimiter, contentStart);
        if (end < 0) {
            return response.substring(contentStart).trim();
        }

        return response.substring(contentStart, end).trim();
    }
}
