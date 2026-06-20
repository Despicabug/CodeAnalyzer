package com.codeanalyser.worker.ingestion;

import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.List;

/**
 * Excludes vendored, generated, and build-artifact paths from analysis.
 * Segments are matched individually to avoid false positives — e.g.
 * {@code src/vendor-utils/} is not excluded, only exact {@code vendor/}.
 */
@Component
public class VendorPathFilter {

    /**
     * Path segments that are excluded when matched exactly (case-sensitive).
     * These are the most common vendored/generated/irrelevant directories.
     */
    private static final List<String> EXCLUDED_EXACT_SEGMENTS = List.of(
            // JavaScript / Node
            "node_modules",
            // Go
            "vendor",
            // Python virtual environments
            ".venv", "venv", "env", ".env",
            // Build outputs (exact names)
            "build", "dist", "out", "target", "bin", "obj",
            // IDE and tool directories
            ".git", ".github", ".idea", ".vscode", ".eclipse",
            // Test coverage and reports
            "coverage", ".nyc_output",
            // Package lock directories
            ".yarn", ".pnp",
            // Generated protobuf/gRPC output
            "generated", "generated-sources", "gen",
            // Gradle wrapper internals
            ".gradle",
            // Maven wrapper internals (the wrapper jar itself is excluded via binary detection)
            ".mvn"
    );

    /** Segments excluded by prefix — kept minimal to avoid false positives. */
    private static final List<String> EXCLUDED_SEGMENT_PREFIXES = List.of(
            "__pycache__"
    );

    /** File extensions excluded regardless of location (generated or binary-adjacent). */
    private static final List<String> EXCLUDED_FILE_EXTENSIONS = List.of(
            "min.js",   // minified JavaScript
            "min.css",  // minified CSS
            "map",      // source maps (text but machine-generated)
            "lock"      // lock files (yarn.lock, Gemfile.lock, poetry.lock …)
    );

    /** Exact filenames excluded — lock files whose extension isn't {@code .lock}. */
    private static final List<String> EXCLUDED_EXACT_FILENAMES = List.of(
            "package-lock.json",   // npm
            "pnpm-lock.yaml",      // pnpm
            "shrinkwrap.json",     // npm shrinkwrap
            "Pipfile.lock",        // pip
            "Cargo.lock",          // Rust (debatable — included in apps, not libs — exclude for safety)
            "composer.lock"        // PHP Composer
    );

    /**
     * Returns {@code true} if the given path (relative to the repo root) should
     * be excluded from analysis.
     *
     * @param repoRelativePath Path relative to the repo root directory.
     */
    public boolean isExcluded(Path repoRelativePath) {
        // Check each segment in the path.
        for (Path segment : repoRelativePath) {
            String name = segment.toString();

            if (EXCLUDED_EXACT_SEGMENTS.contains(name)) {
                return true;
            }

            for (String prefix : EXCLUDED_SEGMENT_PREFIXES) {
                if (name.startsWith(prefix)) {
                    return true;
                }
            }
        }

        // Check file extension and exact-filename exclusions on the filename itself.
        String filename = repoRelativePath.getFileName().toString();
        String filenameLower = filename.toLowerCase();

        for (String ext : EXCLUDED_FILE_EXTENSIONS) {
            if (filenameLower.endsWith("." + ext) || filenameLower.equals(ext)) {
                return true;
            }
        }

        // Case-sensitive exact filename match (lock files like package-lock.json).
        if (EXCLUDED_EXACT_FILENAMES.contains(filename)) {
            return true;
        }

        return false;
    }
}
