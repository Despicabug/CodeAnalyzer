package com.codeanalyser.worker.ingestion;

import com.codeanalyser.common.model.FileChunk;
import com.codeanalyser.worker.config.IngestionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Walks a cloned repository tree, filters out binary and vendored files, and
 * produces a flat list of {@link FileChunk} objects ready for LLM analysis.
 *
 * <h3>Chunking algorithm (current — Option A)</h3>
 * <ol>
 *   <li>Walk the tree with {@link Files#walk} (depth-first, all files).</li>
 *   <li>Skip directories, symlinks, vendor paths, and binary files.</li>
 *   <li>Read the remaining files as UTF-8 text.</li>
 *   <li>If the file fits within {@code ingestion.max-chunk-bytes}, produce one
 *       chunk with {@code chunkIndex=0, totalChunks=1}.</li>
 *   <li>Otherwise split at the last newline before each byte boundary, producing
 *       N chunks. The split is clean (no mid-line breaks) but may cut mid-function.
 *       See the TODO in {@link FileChunk} about improving this later.</li>
 * </ol>
 *
 * <h3>File hash</h3>
 * Each chunk carries the SHA-256 of the <em>full file's bytes</em> (not just the
 * chunk's content). This means all chunks of the same file share the same hash,
 * which is the correct behaviour for cache keying: if file X is unchanged between
 * two commits, all its chunks get a cache hit.
 *
 * <h3>Language detection</h3>
 * Currently file-extension-based (a simple map lookup). Sufficient for Milestone 1.
 * A future milestone could integrate a real language detector (e.g., linguist-compatible
 * heuristics) for better accuracy on files without standard extensions.
 */
@Service
public class FileChunkerService {

    private static final Logger log = LoggerFactory.getLogger(FileChunkerService.class);

    private final IngestionProperties props;
    private final BinaryFileDetector  binaryDetector;
    private final VendorPathFilter    vendorFilter;

    public FileChunkerService(IngestionProperties props,
                              BinaryFileDetector binaryDetector,
                              VendorPathFilter vendorFilter) {
        this.props          = props;
        this.binaryDetector = binaryDetector;
        this.vendorFilter   = vendorFilter;
    }

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /**
     * Walks {@code repoRoot} and produces all {@link FileChunk}s for the job.
     *
     * @param jobId    The parent job's ID (embedded in every chunk).
     * @param repoUrl  The repo's canonical URL (used in cache keys).
     * @param commitSha The HEAD SHA (used in cache keys).
     * @param repoRoot Path to the cloned repo's root directory.
     * @return Immutable list of chunks, ordered by file path then chunk index.
     */
    public List<FileChunk> chunkRepository(UUID jobId, String repoUrl,
                                           String commitSha, Path repoRoot) throws IOException {
        List<FileChunk> chunks     = new ArrayList<>();
        AtomicInteger   fileCount  = new AtomicInteger(0);
        AtomicInteger   skipCount  = new AtomicInteger(0);

        try (Stream<Path> tree = Files.walk(repoRoot)) {
            tree.filter(Files::isRegularFile)  // directories and symlinks excluded
                .sorted()                       // deterministic ordering for reproducibility
                .forEach(absolutePath -> {
                    Path repoRelative = repoRoot.relativize(absolutePath);

                    // Filter 1: vendored / generated paths
                    if (vendorFilter.isExcluded(repoRelative)) {
                        skipCount.incrementAndGet();
                        log.trace("[{}] Skipping (vendor): {}", jobId, repoRelative);
                        return;
                    }

                    // Filter 2: binary files
                    if (binaryDetector.isBinary(absolutePath)) {
                        skipCount.incrementAndGet();
                        log.trace("[{}] Skipping (binary): {}", jobId, repoRelative);
                        return;
                    }

                    // Process the file
                    try {
                        List<FileChunk> fileChunks = chunkFile(
                                jobId, repoUrl, commitSha, absolutePath, repoRelative);
                        chunks.addAll(fileChunks);
                        fileCount.incrementAndGet();
                    } catch (IOException e) {
                        // Log and continue — one unreadable file should not abort the job.
                        log.warn("[{}] Could not read file {}: {}", jobId, repoRelative, e.getMessage());
                        skipCount.incrementAndGet();
                    }
                });
        }

        log.info("[{}] Chunking complete: {} files → {} chunks ({} skipped)",
                jobId, fileCount.get(), chunks.size(), skipCount.get());

        return Collections.unmodifiableList(chunks);
    }

    // ---------------------------------------------------------------------------
    // Private: per-file chunking
    // ---------------------------------------------------------------------------

    private List<FileChunk> chunkFile(UUID jobId, String repoUrl, String commitSha,
                                      Path absolutePath, Path repoRelativePath) throws IOException {

        byte[] rawBytes = Files.readAllBytes(absolutePath);
        String fileHash = sha256Hex(rawBytes);
        String language = detectLanguage(repoRelativePath.getFileName().toString());
        String content  = new String(rawBytes, StandardCharsets.UTF_8);

        int maxBytes = props.maxChunkBytes();

        if (rawBytes.length <= maxBytes) {
            // Happy path: file fits in one chunk.
            return List.of(new FileChunk(
                    UUID.randomUUID(), jobId, repoUrl, commitSha,
                    repoRelativePath.toString().replace('\\', '/'),
                    fileHash, content, 0, 1, language));
        }

        // File needs splitting. Split at line boundaries to avoid cutting mid-line.
        // TODO (Milestone N): improve to heuristic structural splitting so we
        // don't cut mid-function. See FileChunk Javadoc for the rationale.
        return splitAtLineBoundaries(jobId, repoUrl, commitSha,
                repoRelativePath, fileHash, content, language, maxBytes);
    }

    private List<FileChunk> splitAtLineBoundaries(
            UUID jobId, String repoUrl, String commitSha,
            Path repoRelativePath, String fileHash, String content,
            String language, int maxBytes) {

        List<String> lines = content.lines().toList();

        List<FileChunk> chunks          = new ArrayList<>();
        StringBuilder   currentChunk    = new StringBuilder();
        List<String[]>  pendingContents = new ArrayList<>(); // collect before we know totalChunks

        for (String line : lines) {
            String lineWithNewline = line + "\n";
            byte[] lineBytes       = lineWithNewline.getBytes(StandardCharsets.UTF_8);

            if (currentChunk.length() > 0
                    && currentChunk.toString().getBytes(StandardCharsets.UTF_8).length
                       + lineBytes.length > maxBytes) {
                // Flush current chunk
                pendingContents.add(new String[]{ currentChunk.toString() });
                currentChunk = new StringBuilder();
            }
            currentChunk.append(lineWithNewline);
        }
        if (!currentChunk.isEmpty()) {
            pendingContents.add(new String[]{ currentChunk.toString() });
        }

        int totalChunks = pendingContents.size();
        String filePath = repoRelativePath.toString().replace('\\', '/');

        for (int i = 0; i < totalChunks; i++) {
            chunks.add(new FileChunk(
                    UUID.randomUUID(), jobId, repoUrl, commitSha,
                    filePath, fileHash, pendingContents.get(i)[0],
                    i, totalChunks, language));
        }

        log.debug("[{}] Split {} into {} chunks", jobId, filePath, totalChunks);
        return chunks;
    }

    // ---------------------------------------------------------------------------
    // Private: utilities
    // ---------------------------------------------------------------------------

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest md     = MessageDigest.getInstance("SHA-256");
            byte[]        digest = md.digest(bytes);
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed by the JVM spec — this can never happen.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /**
     * Maps common file extensions to a language label.
     * This label is passed to the LLM as context (e.g. "The following is Java code:").
     */
    private static String detectLanguage(String filename) {
        String lower = filename.toLowerCase();
        if (lower.endsWith(".java"))              return "java";
        if (lower.endsWith(".kt") || lower.endsWith(".kts")) return "kotlin";
        if (lower.endsWith(".py"))               return "python";
        if (lower.endsWith(".js") || lower.endsWith(".mjs")) return "javascript";
        if (lower.endsWith(".ts") || lower.endsWith(".tsx")) return "typescript";
        if (lower.endsWith(".go"))               return "go";
        if (lower.endsWith(".rs"))               return "rust";
        if (lower.endsWith(".rb"))               return "ruby";
        if (lower.endsWith(".cpp") || lower.endsWith(".cc") || lower.endsWith(".cxx")) return "cpp";
        if (lower.endsWith(".c"))                return "c";
        if (lower.endsWith(".cs"))               return "csharp";
        if (lower.endsWith(".scala"))            return "scala";
        if (lower.endsWith(".yaml") || lower.endsWith(".yml")) return "yaml";
        if (lower.endsWith(".json"))             return "json";
        if (lower.endsWith(".xml"))              return "xml";
        if (lower.endsWith(".sql"))              return "sql";
        if (lower.endsWith(".sh") || lower.endsWith(".bash")) return "bash";
        if (lower.endsWith(".md") || lower.endsWith(".markdown")) return "markdown";
        if (lower.endsWith(".tf"))               return "terraform";
        if (lower.equals("dockerfile"))         return "dockerfile";
        if (lower.equals("makefile"))           return "makefile";
        return "unknown";
    }
}
