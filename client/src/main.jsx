import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { marked } from 'marked';
import './styles.css';

marked.setOptions({ breaks: true, gfm: true });

const STAGE_ORDER = ['QUEUED', 'CLONING', 'CHUNKING', 'ANALYSING', 'AGGREGATING', 'DONE'];
const TERMINAL = new Set(['DONE', 'FAILED', 'DEAD_LETTERED']);

const STATUS_MESSAGES = {
  QUEUED: 'Job queued - waiting for a free worker...',
  CLONING: 'Cloning repository...',
  CHUNKING: 'Chunking source files...',
  ANALYSING: 'Sending chunks to LLM for analysis...',
  AGGREGATING: 'Synthesising module summaries into final documents...',
  DONE: 'Analysis complete!',
  FAILED: 'Job failed.',
  DEAD_LETTERED: 'Job dead-lettered after maximum retries.',
};

const PROVIDER_DEFAULTS = {
  openai: { model: 'gpt-4o-mini', baseUrl: '' },
  groq: { model: 'llama-3.1-8b-instant', baseUrl: '' },
  together: { model: 'meta-llama/Llama-3-8b-chat-hf', baseUrl: '' },
  ollama: { model: 'llama3.2', baseUrl: 'http://localhost:11434' },
  custom: { model: '', baseUrl: '' },
};

const TABS = [
  { id: 'architecture', label: 'Architecture' },
  { id: 'onboarding', label: 'Onboarding' },
  { id: 'startmap', label: 'Where to Start' },
  { id: 'modules', label: 'Modules' },
];

function escapeHtml(value) {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function renderMarkdown(text) {
  if (!text) {
    return '<em style="color:var(--muted)">No content.</em>';
  }
  return marked.parse(text);
}

function App() {
  const eventSourceRef = useRef(null);
  const [repoUrl, setRepoUrl] = useState('');
  const [force, setForce] = useState(false);
  const [showByok, setShowByok] = useState(false);
  const [provider, setProvider] = useState('');
  const [apiKey, setApiKey] = useState('');
  const [model, setModel] = useState('');
  const [baseUrl, setBaseUrl] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const [job, setJob] = useState(null);
  const [currentStatus, setCurrentStatus] = useState(null);
  const [statusText, setStatusText] = useState('Submitting...');
  const [spinning, setSpinning] = useState(false);
  const [progress, setProgress] = useState({ visible: false, analysed: 0, total: 0, percent: 0 });
  const [result, setResult] = useState(null);
  const [activeTab, setActiveTab] = useState('architecture');

  useEffect(() => {
    return () => closeEventSource();
  }, []);

  function closeEventSource() {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }

  function handleProviderChange(nextProvider) {
    setProvider(nextProvider);

    if (!nextProvider) {
      setApiKey('');
      setModel('');
      setBaseUrl('');
      return;
    }

    const defaults = PROVIDER_DEFAULTS[nextProvider] ?? { model: '', baseUrl: '' };
    setModel((current) => current || defaults.model);
    setBaseUrl((current) => current || defaults.baseUrl);
  }

  async function handleSubmit(event) {
    event.preventDefault();
    if (!repoUrl.trim()) return;

    closeEventSource();
    setError('');
    resetProgress();
    setSubmitting(true);

    try {
      const selectedProvider = provider || null;
      const body = {
        repoUrl: repoUrl.trim(),
        force,
        llmProvider: selectedProvider,
        llmApiKey: selectedProvider && selectedProvider !== 'ollama' && apiKey.trim() ? apiKey.trim() : null,
        llmModel: selectedProvider && model.trim() ? model.trim() : null,
        llmBaseUrl: selectedProvider && baseUrl.trim() ? baseUrl.trim() : null,
      };

      const response = await fetch('/api/v1/analyse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Server error ${response.status}: ${text}`);
      }

      const data = await response.json();
      setJob(data);
      setResult(null);
      connectStream(data.jobId);
    } catch (submitError) {
      setError(submitError.message);
      setSubmitting(false);
    }
  }

  function connectStream(jobId) {
    closeEventSource();
    setStatusText('Connecting to live stream...');
    setSpinning(true);

    const source = new EventSource(`/api/v1/jobs/${jobId}/stream`);
    eventSourceRef.current = source;

    source.addEventListener('status', (event) => {
      const { status, errorMessage } = JSON.parse(event.data);
      handleStatus(jobId, status, errorMessage);
    });

    source.addEventListener('progress', (event) => {
      const { analysed, total, percent } = JSON.parse(event.data);
      setProgress({ visible: true, analysed, total, percent });
    });

    source.onerror = () => {
      if (TERMINAL.has(currentStatus)) {
        closeEventSource();
      }
    };
  }

  function handleStatus(jobId, status, errorMessage) {
    setCurrentStatus(status);
    setStatusText(STATUS_MESSAGES[status] || status);
    setSpinning(!TERMINAL.has(status));

    if (status === 'ANALYSING') {
      setProgress((current) => ({ ...current, visible: true }));
    }

    if (status === 'DONE') {
      setProgress((current) => ({ ...current, visible: false }));
      closeEventSource();
      setSubmitting(false);
      fetchResult(jobId);
    }

    if (status === 'FAILED' || status === 'DEAD_LETTERED') {
      closeEventSource();
      setSubmitting(false);
      const reason = errorMessage ? ` - ${errorMessage}` : '';
      setError(`Job ${status.toLowerCase()}${reason}`);
    }
  }

  async function fetchResult(jobId) {
    setStatusText('Fetching final result...');
    setSpinning(true);

    try {
      const response = await fetch(`/api/v1/jobs/${jobId}/result`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const finalResult = await response.json();
      setResult(finalResult);
      setStatusText('Analysis complete!');
      setSpinning(false);
      requestAnimationFrame(() => {
        document.getElementById('results-card')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
    } catch (resultError) {
      setError(`Failed to load result: ${resultError.message}`);
      setStatusText('Done - but result fetch failed.');
      setSpinning(false);
    }
  }

  function resetProgress() {
    setCurrentStatus(null);
    setResult(null);
    setActiveTab('architecture');
    setProgress({ visible: false, analysed: 0, total: 0, percent: 0 });
    setStatusText('Submitting...');
    setSpinning(true);
  }

  return (
    <>
      <Header />
      <main>
        <SubmitCard
          repoUrl={repoUrl}
          setRepoUrl={setRepoUrl}
          force={force}
          setForce={setForce}
          submitting={submitting}
          showByok={showByok}
          setShowByok={setShowByok}
          provider={provider}
          onProviderChange={handleProviderChange}
          apiKey={apiKey}
          setApiKey={setApiKey}
          model={model}
          setModel={setModel}
          baseUrl={baseUrl}
          setBaseUrl={setBaseUrl}
          onSubmit={handleSubmit}
        />

        {error && <div className="error-banner" role="alert">Warning: {error}</div>}

        {job && (
          <ProgressCard
            job={job}
            status={currentStatus}
            statusText={statusText}
            spinning={spinning}
            progress={progress}
          />
        )}

        {result && (
          <ResultsCard
            result={result}
            activeTab={activeTab}
            setActiveTab={setActiveTab}
          />
        )}
      </main>
    </>
  );
}

function Header() {
  return (
    <header>
      <span className="logo" aria-hidden="true">CA</span>
      <div>
        <h1>CodeAnalyser</h1>
        <span className="tagline">AI-powered architecture overview for any GitHub repo</span>
      </div>
    </header>
  );
}

function SubmitCard(props) {
  const hideApiKey = !props.provider || props.provider === 'ollama';
  const hideModel = !props.provider;
  const hideBaseUrl = props.provider !== 'ollama' && props.provider !== 'custom';

  return (
    <section className="card">
      <form className="submit-form" autoComplete="off" onSubmit={props.onSubmit}>
        <div className="row">
          <input
            type="url"
            placeholder="https://github.com/owner/repo"
            required
            pattern="https?://.+"
            value={props.repoUrl}
            onChange={(event) => props.setRepoUrl(event.target.value)}
          />
          <button type="submit" disabled={props.submitting}>
            {props.submitting ? 'Submitting...' : 'Analyse'}
          </button>
        </div>

        <label className="force-row">
          <input
            type="checkbox"
            checked={props.force}
            onChange={(event) => props.setForce(event.target.checked)}
          />
          <span>Force re-analysis (ignore cached result)</span>
        </label>

        <div className="byok-toggle">
          <button
            type="button"
            className={props.showByok ? 'open' : ''}
            aria-expanded={props.showByok}
            aria-controls="byok-panel"
            onClick={() => props.setShowByok(!props.showByok)}
          >
            <span className="chevron">&gt;</span>
            Use your own LLM API key <span className="optional">(optional)</span>
          </button>
        </div>

        {props.showByok && (
          <div id="byok-panel" className="byok-panel" role="region" aria-label="LLM provider settings">
            <div className="byok-field span-all">
              <label htmlFor="llm-provider">Provider</label>
              <select
                id="llm-provider"
                value={props.provider}
                onChange={(event) => props.onProviderChange(event.target.value)}
              >
                <option value="">Server Default</option>
                <option value="openai">OpenAI</option>
                <option value="groq">Groq</option>
                <option value="together">Together AI</option>
                <option value="ollama">Ollama (local)</option>
                <option value="custom">Custom endpoint</option>
              </select>
            </div>

            {!hideApiKey && (
              <div className="byok-field">
                <label htmlFor="llm-api-key">API Key</label>
                <input
                  type="password"
                  id="llm-api-key"
                  placeholder="sk-..."
                  autoComplete="off"
                  spellCheck="false"
                  value={props.apiKey}
                  onChange={(event) => props.setApiKey(event.target.value)}
                />
              </div>
            )}

            {!hideModel && (
              <div className="byok-field">
                <label htmlFor="llm-model">Model</label>
                <input
                  type="text"
                  id="llm-model"
                  placeholder="Leave blank to use server default"
                  value={props.model}
                  onChange={(event) => props.setModel(event.target.value)}
                />
              </div>
            )}

            {!hideBaseUrl && (
              <div className="byok-field">
                <label htmlFor="llm-base-url">Base URL</label>
                <input
                  type="text"
                  id="llm-base-url"
                  placeholder="https://..."
                  autoComplete="off"
                  value={props.baseUrl}
                  onChange={(event) => props.setBaseUrl(event.target.value)}
                />
              </div>
            )}

            <p className="byok-hint">
              Your key is sent directly to the server and used only for this job.
              It is stored in Redis only for the duration of the analysis, then discarded.
            </p>
          </div>
        )}
      </form>
    </section>
  );
}

function ProgressCard({ job, status, statusText, spinning, progress }) {
  return (
    <section className="card progress-card">
      <div className="job-meta">
        <span className="job-id">Job: {job.jobId}</span>
        {job.isNew && <span className="is-new-badge">new</span>}
      </div>

      <div className="pipeline" aria-label="Analysis progress">
        {STAGE_ORDER.map((stage, index) => (
          <PipelineStage key={stage} stage={stage} index={index} status={status} />
        ))}
      </div>

      {progress.visible && (
        <div className="progress-bar-wrap">
          <div className="progress-label">
            <span>Analysed {progress.analysed} / {progress.total} chunks</span>
            <span>{progress.percent}%</span>
          </div>
          <div className="progress-track">
            <div className="progress-fill" style={{ width: `${progress.percent}%` }} />
          </div>
        </div>
      )}

      <div className="status-msg">
        {spinning && <span className="spinner" aria-hidden="true" />}
        <span>{statusText}</span>
      </div>
    </section>
  );
}

function PipelineStage({ stage, index, status }) {
  const stageIndex = STAGE_ORDER.indexOf(status);
  const isFailed = status === 'FAILED' || status === 'DEAD_LETTERED';
  let className = 'stage';

  if (isFailed && index <= Math.max(stageIndex, 0)) {
    className += index < stageIndex ? ' done' : ' failed';
  } else if (index < stageIndex) {
    className += ' done';
  } else if (index === stageIndex) {
    className += ' active';
  }

  return <div className={className}>{stage}</div>;
}

function ResultsCard({ result, activeTab, setActiveTab }) {
  return (
    <section className="card results-card" id="results-card">
      <div className="stats-row">
        <div className="stat">
          <div className="value">{result.totalFilesAnalysed ?? '-'}</div>
          <div className="label">Files analysed</div>
        </div>
        <div className="stat">
          <div className="value">{result.moduleSummaries?.length ?? '-'}</div>
          <div className="label">Modules</div>
        </div>
        <div className="stat">
          <div className="value commit">{(result.commitSha ?? '-').substring(0, 8)}</div>
          <div className="label">Commit SHA</div>
        </div>
      </div>

      <div className="tabs">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            className={activeTab === tab.id ? 'tab-btn active' : 'tab-btn'}
            type="button"
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === 'architecture' && <MarkdownPanel text={result.architectureOverview} />}
      {activeTab === 'onboarding' && <MarkdownPanel text={result.onboardingGuide} />}
      {activeTab === 'startmap' && <MarkdownPanel text={result.startMap} />}
      {activeTab === 'modules' && <ModuleAccordion modules={result.moduleSummaries ?? []} />}
    </section>
  );
}

function MarkdownPanel({ text }) {
  return (
    <div
      className="md-content"
      dangerouslySetInnerHTML={{ __html: renderMarkdown(text) }}
    />
  );
}

function ModuleAccordion({ modules }) {
  if (modules.length === 0) {
    return <p className="empty-text">No module summaries available.</p>;
  }

  return (
    <div className="modules-section">
      <h3>Module Summaries</h3>
      {modules.map((module, index) => (
        <ModuleItem key={`${module.moduleName ?? 'module'}-${index}`} module={module} />
      ))}
    </div>
  );
}

function ModuleItem({ module }) {
  const [open, setOpen] = useState(false);
  const languages = module.languageBreakdown
    ? Object.entries(module.languageBreakdown)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([language, count]) => `${language}(${count})`)
        .join(', ')
    : '';

  return (
    <div className={open ? 'module-item open' : 'module-item'}>
      <button type="button" className="module-header" onClick={() => setOpen(!open)}>
        <span>{module.moduleName ?? 'unknown'}</span>
        <span className="meta">
          {module.fileCount ?? 0} files{languages ? ` - ${languages}` : ''}
        </span>
        <span className="chevron">&gt;</span>
      </button>
      {open && (
        <div
          className="module-body"
          dangerouslySetInnerHTML={{ __html: renderMarkdown(module.summaryText) }}
        />
      )}
    </div>
  );
}

createRoot(document.getElementById('root')).render(<App />);
