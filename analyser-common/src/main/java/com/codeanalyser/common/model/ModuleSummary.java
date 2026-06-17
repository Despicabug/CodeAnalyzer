package com.codeanalyser.common.model;

import java.util.List;

/**
 * Aggregation pass-1 output: an LLM-generated summary for one top-level directory.
 * Flat repos with no subdirectories produce a single "(root)" module.
 */
public record ModuleSummary(
        String       moduleName,
        List<String> filePaths,
        String       languageBreakdown,
        String       summaryText
) {
    /** Number of files in this module. */
    public int fileCount() {
        return filePaths.size();
    }
}
