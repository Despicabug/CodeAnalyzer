package com.codeanalyser.worker.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * Determines whether a file is binary (non-text) and should be skipped during
 * chunking.
 *
 * <h3>Detection strategy: two-pass</h3>
 * <ol>
 *   <li><b>Extension allowlist</b> – if the extension is in our known-text list,
 *       the file is text without reading it. Fast and handles most source files.</li>
 *   <li><b>Extension denylist</b> – if the extension is in our known-binary list,
 *       it is binary without reading it.</li>
 *   <li><b>Byte scan</b> – for unknown extensions, read the first
 *       {@value #SCAN_BYTES} bytes. If any byte is a null byte (0x00), it is
 *       almost certainly binary. This is the same heuristic used by {@code git}
 *       and {@code grep}.</li>
 * </ol>
 *
 * <h3>Why not MIME type detection?</h3>
 * {@code Files.probeContentType} relies on the OS content-type database and is
 * inconsistent across Linux distributions. The null-byte heuristic is portable,
 * fast, and sufficient for our purposes (we only need to distinguish text from
 * binary, not identify the exact format).
 */
@Component
public class BinaryFileDetector {

    private static final Logger log = LoggerFactory.getLogger(BinaryFileDetector.class);

    /** Number of bytes to read for the binary heuristic scan. */
    private static final int SCAN_BYTES = 8192;

    // Source code and text formats we always treat as text.
    private static final Set<String> TEXT_EXTENSIONS = Set.of(
            "java", "kt", "kts", "scala", "groovy",
            "py", "pyi", "rb", "go", "rs", "c", "cpp", "cc", "cxx", "h", "hpp",
            "cs", "fs", "fsx", "vb",
            "js", "mjs", "cjs", "jsx", "ts", "tsx",
            "html", "htm", "css", "scss", "sass", "less",
            "xml", "xsd", "xsl", "xslt", "wsdl",
            "json", "yaml", "yml", "toml", "ini", "cfg", "conf", "properties",
            "md", "markdown", "rst", "txt", "csv", "tsv",
            "sh", "bash", "zsh", "fish", "ps1", "bat", "cmd",
            "sql", "graphql", "proto", "thrift",
            "tf", "tfvars",          // Terraform
            "gradle",                // Gradle build scripts (.kts already listed above)
            "Dockerfile", "Makefile" // no extension but matched by name elsewhere
    );

    // Known binary formats — skip without reading.
    private static final Set<String> BINARY_EXTENSIONS = Set.of(
            // Images
            "png", "jpg", "jpeg", "gif", "bmp", "ico", "svg", "webp", "tiff",
            // Fonts
            "ttf", "otf", "woff", "woff2", "eot",
            // Archives
            "zip", "tar", "gz", "bz2", "xz", "7z", "rar", "jar", "war", "ear",
            // Compiled / object files
            "class", "pyc", "pyo", "o", "obj", "a", "so", "dll", "dylib", "exe",
            // Documents
            "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
            // Media
            "mp3", "mp4", "wav", "avi", "mov", "mkv",
            // Databases / blobs
            "db", "sqlite", "sqlite3", "bin", "dat"
    );

    /**
     * Returns {@code true} if the file should be skipped (it is binary or
     * unreadable).
     */
    public boolean isBinary(Path file) {
        String name = file.getFileName().toString();
        String ext  = extensionOf(name).toLowerCase();

        if (TEXT_EXTENSIONS.contains(ext)) {
            return false; // fast path — known text
        }
        if (BINARY_EXTENSIONS.contains(ext)) {
            return true;  // fast path — known binary
        }

        // Unknown extension: scan bytes.
        return containsNullByte(file);
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    private boolean containsNullByte(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buffer = new byte[SCAN_BYTES];
            int    read   = in.read(buffer);
            for (int i = 0; i < read; i++) {
                if (buffer[i] == 0) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            // If we can't read the file, treat it as binary so it is skipped.
            log.warn("Could not read file for binary detection, skipping: {} ({})",
                    file, e.getMessage());
            return true;
        }
    }

    private static String extensionOf(String filename) {
        int dot = filename.lastIndexOf('.');
        return (dot >= 0 && dot < filename.length() - 1)
                ? filename.substring(dot + 1)
                : "";
    }
}
