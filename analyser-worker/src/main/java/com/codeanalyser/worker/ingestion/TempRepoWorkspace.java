package com.codeanalyser.worker.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

/**
 * A temporary directory that holds a cloned repository for the duration of the
 * chunking step, then deletes itself unconditionally.
 *
 * <h3>Disk safety design</h3>
 * <p>The clone must not outlive the chunking step. We enforce this by making
 * {@code TempRepoWorkspace} implement {@link AutoCloseable} and always using it
 * inside a try-with-resources block:
 * <pre>
 *   try (var workspace = TempRepoWorkspace.create(jobId)) {
 *       git.clone(repoUrl, workspace.repoDir());
 *       chunker.chunk(workspace.repoDir()); // produces chunks in memory / on queue
 *   } // ← workspace.close() deletes the entire directory tree here,
 *     //   even if chunker threw an exception
 * </pre>
 *
 * <p>This is belt-and-suspenders with the concurrency semaphore in
 * {@link GitCloneService}: the semaphore limits how many workspaces can exist
 * simultaneously; this class ensures each one is deleted when its work is done.
 *
 * <h3>Why not Files.createTempDirectory alone?</h3>
 * {@code Files.createTempDirectory} creates the directory but gives you no
 * lifecycle management. The JVM does not guarantee temp-file deletion on exit
 * (it is best-effort and fails if the process is killed). An explicit
 * AutoCloseable gives us deterministic, exception-safe cleanup.
 */
public final class TempRepoWorkspace implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TempRepoWorkspace.class);

    private final Path    workDir;   // the per-job temp directory
    private final Path    repoDir;   // workDir/repo — the clone target
    private final UUID    jobId;
    private       boolean closed = false;

    private TempRepoWorkspace(UUID jobId, Path workDir) throws IOException {
        this.jobId   = jobId;
        this.workDir = workDir;
        this.repoDir = workDir.resolve("repo");
        Files.createDirectories(repoDir);
    }

    /**
     * Creates a new temporary workspace under the system temp directory.
     * The directory is named {@code codeanalyser-<jobId>} for easy identification
     * if a cleanup sweep ever needs to find orphaned workspaces.
     */
    public static TempRepoWorkspace create(UUID jobId) throws IOException {
        Path base = Files.createTempDirectory("codeanalyser-" + jobId);
        log.debug("[{}] Created temp workspace at {}", jobId, base);
        return new TempRepoWorkspace(jobId, base);
    }

    /**
     * The directory into which git should clone the repository.
     * Pass this path to {@code ProcessBuilder} as the clone target.
     */
    public Path repoDir() {
        ensureOpen();
        return repoDir;
    }

    /**
     * Deletes the entire workspace directory tree.
     *
     * <p>Called automatically by try-with-resources. Safe to call multiple times
     * (subsequent calls are no-ops). Logs a warning if deletion fails so the
     * operator knows to inspect disk usage, but does not rethrow — a cleanup
     * failure should not mask the original business exception.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            deleteRecursively(workDir);
            log.debug("[{}] Deleted temp workspace {}", jobId, workDir);
        } catch (IOException e) {
            // Log and swallow: cleanup failure must not mask pipeline exceptions.
            // An operator alert on this log line would be appropriate in production.
            log.warn("[{}] Failed to delete temp workspace {} — manual cleanup may be needed: {}",
                    jobId, workDir, e.getMessage());
        }
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("TempRepoWorkspace already closed for job " + jobId);
    }

    /**
     * Recursively deletes a directory tree.
     *
     * <p>We use {@link Files#walkFileTree} rather than {@code File.delete()} in a
     * loop because it handles symlinks correctly: it deletes the symlink itself,
     * not the target, which prevents accidentally deleting files outside the
     * workspace if the cloned repo contains symlinks.
     */
    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) return;

        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) throw exc;
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
