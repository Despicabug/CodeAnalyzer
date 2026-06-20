package com.codeanalyser.worker.ingestion;

import com.codeanalyser.worker.config.IngestionProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Clones a public GitHub repository to a local temporary directory using
 * {@code git clone --depth 1 --single-branch}.
 *
 * <h3>Disk safety: three layers</h3>
 * <ol>
 *   <li><b>Pre-clone size guard</b> – hits the GitHub API to read the repo's
 *       declared size in KB. Rejects repos over {@code ingestion.max-repo-size-mb}
 *       before any disk is touched.</li>
 *   <li><b>Concurrency semaphore</b> – permits at most
 *       {@code ingestion.max-concurrent-clones} clones to run simultaneously.
 *       Excess jobs block until a slot is free. This bounds
 *       peak disk usage to {@code maxConcurrentClones × maxRepoSizeMb}.</li>
 *   <li><b>TempRepoWorkspace (AutoCloseable)</b> – the workspace is always
 *       deleted by the caller's try-with-resources, whether or not cloning
 *       or chunking succeeds.</li>
 * </ol>
 *
 * <h3>Why ProcessBuilder over JGit?</h3>
 * {@code git clone --depth 1 --single-branch} is a single command that is
 * well-understood, fast, and correct. JGit replicates the same logic in ~30
 * lines of verbose API calls that are hard to read in a code review. The
 * tradeoff is that {@code git} must be available in the container (one line
 * in the Dockerfile: {@code RUN apt-get install -y git}).
 *
 * <h3>commitSha extraction</h3>
 * After cloning, we run {@code git rev-parse HEAD} to capture the exact commit
 * SHA. This is the idempotency key component: two requests for the same repo
 * at different commits produce different analysis jobs.
 */
@Service
public class GitCloneService {

    private static final Logger log = LoggerFactory.getLogger(GitCloneService.class);

    // Matches "github.com/{owner}/{repo}" with optional .git suffix and trailing slash.
    private static final Pattern GITHUB_REPO_PATTERN =
            Pattern.compile("https?://github\\.com/([^/]+)/([^/]+?)(?:\\.git)?/?$");

    private final IngestionProperties props;
    private final Semaphore           cloneSemaphore;
    private final RestClient          restClient;
    private final ObjectMapper        objectMapper;

    public GitCloneService(IngestionProperties props, ObjectMapper objectMapper) {
        this.props          = props;
        this.objectMapper   = objectMapper;
        this.cloneSemaphore = new Semaphore(props.maxConcurrentClones(), true /* fair */);

        // RestClient for GitHub API calls (size guard).
        // We configure a base URL and optionally add the auth token header.
        RestClient.Builder builder = RestClient.builder()
                .baseUrl("https://api.github.com")
                .defaultHeader("Accept", "application/vnd.github+json")
                .defaultHeader("X-GitHub-Api-Version", "2022-11-28");

        if (!props.githubApiToken().isBlank()) {
            builder.defaultHeader("Authorization", "Bearer " + props.githubApiToken());
        }

        this.restClient = builder.build();
    }

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /**
     * Clones {@code repoUrl} into {@code targetDir} (which must already exist).
     *
     * <p>Blocks until a semaphore slot is available, then performs the clone.
     *
     * @param jobId     Used only for structured logging.
     * @param repoUrl   Public GitHub HTTPS URL.
     * @param targetDir Directory to clone into (use {@link TempRepoWorkspace#repoDir()}).
     * @return The HEAD commit SHA after cloning.
     * @throws RepoTooLargeException    if the repo exceeds the configured size limit.
     * @throws GitCloneException        if git exits non-zero or times out.
     * @throws InterruptedException     if the thread is interrupted while waiting
     *                                  for a semaphore slot.
     */
    public String cloneRepo(UUID jobId, String repoUrl, Path targetDir)
            throws RepoTooLargeException, GitCloneException, InterruptedException {

        // Layer 1: size guard — no disk touched yet.
        checkRepoSize(jobId, repoUrl);

        // Layer 2: semaphore — wait for a concurrent-clone slot.
        log.info("[{}] Waiting for clone slot ({} available of {})",
                jobId, cloneSemaphore.availablePermits(), props.maxConcurrentClones());
        cloneSemaphore.acquire();

        try {
            log.info("[{}] Clone slot acquired, starting git clone of {}", jobId, repoUrl);
            runGitClone(jobId, repoUrl, targetDir);
            String sha = readHeadCommitSha(jobId, targetDir);
            log.info("[{}] Clone complete, HEAD = {}", jobId, sha);
            return sha;
        } finally {
            // Always release — even if clone or SHA-read threw.
            cloneSemaphore.release();
            log.debug("[{}] Clone slot released ({} now available)",
                    jobId, cloneSemaphore.availablePermits());
        }
    }

    // ---------------------------------------------------------------------------
    // Private: size guard
    // ---------------------------------------------------------------------------

    private void checkRepoSize(UUID jobId, String repoUrl) throws RepoTooLargeException {
        Matcher m = GITHUB_REPO_PATTERN.matcher(repoUrl);
        if (!m.matches()) {
            // Non-GitHub URLs skip the size check for now.
            // TODO: extend to GitLab, Bitbucket, etc.
            log.debug("[{}] Skipping size check for non-GitHub URL: {}", jobId, repoUrl);
            return;
        }

        String owner = m.group(1);
        String repo  = m.group(2);

        try {
            String json = restClient.get()
                    .uri("/repos/{owner}/{repo}", owner, repo)
                    .retrieve()
                    .body(String.class);

            JsonNode root     = objectMapper.readTree(json);
            long    sizeKb    = root.path("size").asLong(0);
            long    limitKb   = (long) props.maxRepoSizeMb() * 1024;

            log.info("[{}] Repo {}/{} reported size: {} KB (limit: {} KB)",
                    jobId, owner, repo, sizeKb, limitKb);

            if (sizeKb > limitKb) {
                throw new RepoTooLargeException(
                        "Repo %s/%s is %d KB, exceeding the %d MB limit"
                                .formatted(owner, repo, sizeKb, props.maxRepoSizeMb()));
            }
        } catch (RepoTooLargeException e) {
            throw e; // re-throw as-is
        } catch (Exception e) {
            // Size check failure is non-fatal: log and proceed.
            // A bad GitHub API response should not block legitimate clones.
            log.warn("[{}] Could not check repo size (proceeding anyway): {}", jobId, e.getMessage());
        }
    }

    // ---------------------------------------------------------------------------
    // Private: git clone subprocess
    // ---------------------------------------------------------------------------

    private void runGitClone(UUID jobId, String repoUrl, Path targetDir)
            throws GitCloneException, InterruptedException {

        // --depth 1         : shallow clone — only the latest commit tree, no history.
        //                     This is the single biggest size reduction (~10-100x).
        // --single-branch   : only fetch the default branch, not all remote branches.
        // --no-tags         : skip tag objects (further reduces download size).
        // The final argument is the destination directory.
        List<String> command = List.of(
                "git", "clone",
                "--depth", "1",
                "--single-branch",
                "--no-tags",
                repoUrl,
                targetDir.toAbsolutePath().toString()
        );

        log.debug("[{}] Running: {}", jobId, String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command)
                .redirectErrorStream(true); // merge stderr into stdout so we capture everything

        try {
            Process process = pb.start();

            // Drain stdout+stderr in this thread to prevent the subprocess from
            // blocking on a full pipe buffer (a classic ProcessBuilder pitfall).
            List<String> outputLines = drainOutput(process);

            boolean finished = process.waitFor(props.cloneTimeoutSeconds(), TimeUnit.SECONDS);

            if (!finished) {
                process.destroyForcibly();
                throw new GitCloneException(
                        "git clone timed out after %d seconds for %s"
                                .formatted(props.cloneTimeoutSeconds(), repoUrl));
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                String output = String.join("\n", outputLines);
                throw new GitCloneException(
                        "git clone failed (exit %d) for %s:\n%s"
                                .formatted(exitCode, repoUrl, output));
            }

        } catch (IOException e) {
            throw new GitCloneException("Failed to start git process: " + e.getMessage(), e);
        }
    }

    private static List<String> drainOutput(Process process) throws IOException {
        List<String> lines = new ArrayList<>();
        try (var reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    // ---------------------------------------------------------------------------
    // Private: extract HEAD commit SHA
    // ---------------------------------------------------------------------------

    private String readHeadCommitSha(UUID jobId, Path repoDir)
            throws GitCloneException, InterruptedException {

        List<String> command = List.of("git", "-C", repoDir.toAbsolutePath().toString(),
                "rev-parse", "HEAD");
        try {
            Process process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();

            List<String> lines   = drainOutput(process);
            boolean      finished = process.waitFor(10, TimeUnit.SECONDS);

            if (!finished || process.exitValue() != 0) {
                throw new GitCloneException("Could not read HEAD SHA in " + repoDir);
            }

            return lines.isEmpty() ? "unknown" : lines.get(0).trim();

        } catch (IOException e) {
            throw new GitCloneException("Failed to run git rev-parse: " + e.getMessage(), e);
        }
    }

    // ---------------------------------------------------------------------------
    // Exception types (static inner classes — keep related code together)
    // ---------------------------------------------------------------------------

    /** Thrown when a repo exceeds the configured size limit. Not retryable. */
    public static class RepoTooLargeException extends Exception {
        public RepoTooLargeException(String message) { super(message); }
    }

    /** Thrown when git clone exits non-zero or times out. Retryable. */
    public static class GitCloneException extends Exception {
        public GitCloneException(String message)                  { super(message); }
        public GitCloneException(String message, Throwable cause) { super(message, cause); }
    }
}
