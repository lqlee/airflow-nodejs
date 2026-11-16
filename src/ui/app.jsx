const { useState, useEffect, useCallback, useRef } = React;

const BASE = '';

// ── Session-aware fetch ───────────────────────────────────────────────
// The UI authenticates with an HttpOnly session cookie (sent automatically, unreadable by scripts);
// state-changing requests also carry the session's CSRF token. No credential is kept in browser storage.
let csrfToken = null;
let onSessionLost = () => {};
const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

function apiFetch(url, options = {}) {
  const method = (options.method ?? 'GET').toUpperCase();
  const headers = { ...(options.headers ?? {}) };
  if (!SAFE_METHODS.has(method) && csrfToken) headers['X-CSRF-Token'] = csrfToken;
  return fetch(url, { ...options, headers, credentials: 'same-origin' }).then(res => {
    if (res.status === 401) onSessionLost();
    return res;
  });
}

const LOGIN_ERRORS = {
  idp_error: 'The identity provider refused the sign-in.',
  idp_unavailable: 'The identity provider could not be reached.',
  invalid_state: 'The sign-in link expired or was already used — please try again.',
  invalid_callback: 'The sign-in response was incomplete — please try again.',
  invalid_id_token: 'The identity provider returned an invalid token.',
  no_role: 'You signed in, but your account has no role in Airflow Node. Ask an admin for access.',
  token_exchange_failed: 'Signing in failed while talking to the identity provider.',
};

// ── Login screen ──────────────────────────────────────────────────────
function LoginScreen({ methods, onLogin }) {
  const [key, setKey] = useState('');
  const [error, setError] = useState(() => {
    const code = new URLSearchParams(window.location.search).get('login_error');
    return code ? (LOGIN_ERRORS[code] ?? 'Sign-in failed.') : '';
  });
  const [loading, setLoading] = useState(false);

  // Drop ?login_error= from the address bar once shown
  useEffect(() => {
    if (window.location.search) window.history.replaceState(null, '', window.location.pathname);
  }, []);

  const submit = async (e) => {
    e.preventDefault();
    if (!key.trim()) return;
    setLoading(true);
    setError('');
    const res = await fetch(`${BASE}/auth/key-login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'same-origin',
      body: JSON.stringify({ key: key.trim() }),
    }).catch(() => null);
    setLoading(false);
    if (!res || !res.ok) {
      setError(res?.status === 401 ? 'Invalid API key' : res?.status === 429 ? 'Too many attempts — wait a minute' : 'Sign-in failed');
      return;
    }
    setKey('');
    onLogin();
  };

  return (
    <div className="login-wrap">
      <div className="login-box">
        <h2>✈ Airflow Node</h2>
        <p>Sign in to continue</p>
        {methods?.sso && (
          <button className="btn btn-primary login-sso" onClick={() => { window.location.href = `${BASE}/auth/login`; }}>
            Sign in with SSO
          </button>
        )}
        {methods?.sso && methods?.api_key && <div className="login-or">or use an API key</div>}
        {methods?.api_key && (
          <form onSubmit={submit}>
            <input type="password" placeholder="API key" value={key} autoComplete="off"
              onChange={e => setKey(e.target.value)} autoFocus={!methods?.sso} />
            <button className="btn btn-primary" disabled={loading}>
              {loading ? 'Checking…' : 'Sign in'}
            </button>
          </form>
        )}
        {error && <div className="login-error">{error}</div>}
      </div>
    </div>
  );
}

const statusBadge = (state) => <span className={`badge badge-${state}`}>{state}</span>;

const duration = (start, end) => {
  if (!start) return '-';
  const ms = new Date(end ?? Date.now()) - new Date(start);
  return ms < 1000 ? `${ms}ms` : `${(ms/1000).toFixed(1)}s`;
};

function useInterval(fn, ms) {
  useEffect(() => {
    fn();
    const id = setInterval(fn, ms);
    return () => clearInterval(id);
  }, [fn, ms]);
}

// ── Health ────────────────────────────────────────────────────────────
function Health() {
  const [info, setInfo] = useState(null);
  const fetch_ = useCallback(() =>
    apiFetch(`${BASE}/health`).then(r => r.json()).then(setInfo).catch(()=>{}), []);
  useInterval(fetch_, 3000);
  if (!info) return null;
  return (
    <span className="workers-badge">
      workers: {info.workers?.active ?? 0} active · {info.workers?.queued ?? 0} waiting
    </span>
  );
}

// ── API Keys Panel ────────────────────────────────────────────────────
function ApiKeysPanel({ onClose }) {
  const [keys, setKeys] = useState([]);
  const [name, setName] = useState('');
  const [creating, setCreating] = useState(false);
  const [newKey, setNewKey] = useState(null);  // shown once

  const fetchKeys = useCallback(() =>
    apiFetch(`${BASE}/api-keys`).then(r => r.json()).then(data => {
      if (Array.isArray(data)) setKeys(data);
    }).catch(() => {}), []);

  useEffect(() => { fetchKeys(); }, []);

  const create = async (e) => {
    e.preventDefault();
    if (!name.trim()) return;
    setCreating(true);
    const res = await apiFetch(`${BASE}/api-keys`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim() }),
    });
    const data = await res.json();
    setCreating(false);
    if (res.ok) {
      setNewKey(data.key);
      setName('');
      fetchKeys();
    }
  };

  const revoke = async (id) => {
    await apiFetch(`${BASE}/api-keys/${id}`, { method: 'DELETE' });
    fetchKeys();
  };

  const fmtDate = d => d ? new Date(d).toLocaleDateString() : '—';

  return (
    <div className="keys-panel">
      <h4>
        🔑 API Keys
        <span style={{flex:1}} />
        <button className="log-btn" onClick={onClose}>✕</button>
      </h4>
      <form onSubmit={create} style={{display:'flex',gap:8,marginBottom:16}}>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="Key name (e.g. ci-bot)"
          style={{flex:1,background:'#0f1117',border:'1px solid #4a5568',borderRadius:4,padding:'5px 10px',color:'#e2e8f0',fontSize:12}} />
        <button type="submit" className="btn btn-primary btn-sm" disabled={creating || !name.trim()}>
          {creating ? '…' : '+ Create'}
        </button>
      </form>
      {newKey && (
        <div>
          <div style={{fontSize:11,color:'#68d391',marginBottom:4}}>✓ Key created — copy it now, it won't be shown again:</div>
          <div className="key-new-val">{newKey}</div>
          <button className="log-btn" style={{marginTop:6}} onClick={() => setNewKey(null)}>Dismiss</button>
        </div>
      )}
      {keys.length === 0 && !newKey && <div className="empty" style={{padding:'12px 0'}}>No API keys yet</div>}
      {keys.map(k => (
        <div key={k.id} className="keys-row">
          <span className="key-name">{k.name}</span>
          <span className="key-meta">created {fmtDate(k.created_at)} · last used {fmtDate(k.last_used_at)}</span>
          {k.revoked
            ? <span className="key-revoked">revoked</span>
            : <button className="log-btn" style={{color:'#fc8181',borderColor:'#4a1515'}}
                onClick={() => { if (confirm(`Revoke key "${k.name}"?`)) revoke(k.id); }}>Revoke</button>
          }
        </div>
      ))}
    </div>
  );
}

// ── Docker Images Panel ───────────────────────────────────────────────
function ImagesPanel({ onClose }) {
  const [images, setImages] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState('');
  const [uploadError, setUploadError] = useState('');
  const [deleting, setDeleting] = useState(null);

  const load = useCallback(() =>
    apiFetch(`${BASE}/images`).then(r => r.json()).then(d => setImages(Array.isArray(d) ? d : [])).catch(() => {}), []);

  useEffect(() => { load(); }, [load]);

  const upload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!file.name.endsWith('.tar')) { setUploadError('Only .tar files accepted'); return; }
    setUploading(true); setUploadMsg(''); setUploadError('');
    const form = new FormData();
    form.append('file', file);
    const res = await apiFetch(`${BASE}/images/upload`, { method: 'POST', body: form });
    const data = await res.json().catch(() => ({}));
    setUploading(false);
    if (res.ok) { setUploadMsg(data.message ?? 'Uploaded'); load(); }
    else setUploadError(data.error ?? 'Upload failed');
    e.target.value = '';
  };

  const del = async (name) => {
    setDeleting(name);
    await apiFetch(`${BASE}/images/${encodeURIComponent(name)}`, { method: 'DELETE' });
    setDeleting(null);
    load();
  };

  const fmt = (bytes) => bytes < 1024*1024 ? `${(bytes/1024).toFixed(0)} KB` : `${(bytes/1024/1024).toFixed(1)} MB`;

  return (
    <div className="panel">
      <div className="panel-header">
        <span>🐳 Docker Images</span>
        <button className="refresh-btn" onClick={onClose}>✕</button>
      </div>
      <div style={{marginBottom:12}}>
        <p style={{fontSize:12,color:'#a0aec0',margin:'0 0 8px'}}>
          Upload Docker image <code>.tar</code> files exported with <code>docker save</code>.
          Images are loaded into Docker automatically and can be referenced in DAG <code>requiredImages</code>.
        </p>
        <label style={{display:'inline-block',cursor:'pointer'}}>
          <input type="file" accept=".tar" style={{display:'none'}} onChange={upload} disabled={uploading} />
          <span className="btn btn-primary" style={{fontSize:12,padding:'4px 12px'}}>
            {uploading ? '⏳ Uploading...' : '⬆ Upload .tar'}
          </span>
        </label>
        {uploadMsg && <span style={{marginLeft:8,color:'#68d391',fontSize:12}}>✓ {uploadMsg}</span>}
        {uploadError && <span style={{marginLeft:8,color:'#fc8181',fontSize:12}}>{uploadError}</span>}
      </div>
      {images.length === 0
        ? <p style={{color:'#4a5568',fontSize:12}}>No image tars uploaded yet.</p>
        : <table style={{width:'100%',fontSize:12,borderCollapse:'collapse'}}>
            <thead>
              <tr style={{color:'#718096',textAlign:'left'}}>
                <th style={{padding:'4px 8px'}}>File</th>
                <th style={{padding:'4px 8px'}}>Size</th>
                <th style={{padding:'4px 8px'}}>Use in DAG</th>
                <th style={{padding:'4px 8px'}}></th>
              </tr>
            </thead>
            <tbody>
              {images.map(img => (
                <tr key={img.name} style={{borderTop:'1px solid #2d3748'}}>
                  <td style={{padding:'6px 8px',fontFamily:'monospace'}}>{img.name}</td>
                  <td style={{padding:'6px 8px',color:'#a0aec0'}}>{fmt(img.size_bytes)}</td>
                  <td style={{padding:'6px 8px',color:'#4a5568',fontFamily:'monospace',fontSize:11}}>
                    requiredImages: ['{img.path}']
                  </td>
                  <td style={{padding:'6px 8px'}}>
                    <button className="btn" style={{fontSize:11,padding:'2px 8px',background:'#4a1515',color:'#fc8181'}}
                      disabled={deleting === img.name}
                      onClick={() => del(img.name)}>
                      {deleting === img.name ? '…' : '✕'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
      }
    </div>
  );
}

// ── SLA Alerts ────────────────────────────────────────────────────────
function useSlaAlerts() {
  const [alerts, setAlerts] = useState([]);
  const fetch_ = useCallback(() =>
    apiFetch(`${BASE}/sla-alerts`).then(r => r.json()).then(data => {
      if (Array.isArray(data)) setAlerts(data);
    }).catch(() => {}), []);
  useInterval(fetch_, 10000);
  const refresh = fetch_;
  return { alerts, refresh };
}

function ImportErrorBanner({ errors }) {
  if (!errors.length) return null;
  return (
    <div className="sla-banner" style={{background:'#4a1515',borderColor:'#c05621'}}>
      <span>🔴</span>
      <strong>{errors.length} Dag import error{errors.length > 1 ? 's' : ''}</strong>
      <span>— {errors.map(e => e.filename).join(', ')}</span>
    </div>
  );
}

function useImportErrors() {
  const [errors, setErrors] = useState([]);
  useInterval(() =>
    apiFetch(`${BASE}/import-errors`).then(r => r.json()).then(setErrors).catch(() => {}),
    10000
  );
  return errors;
}

function SlaBanner({ alerts, onViewAlerts }) {
  const unacked = alerts.filter(a => !a.acked);
  if (!unacked.length) return null;
  return (
    <div className="sla-banner">
      <span>⚠️</span>
      <strong>{unacked.length} SLA breach{unacked.length > 1 ? 'es' : ''}</strong>
      <span>— one or more runs exceeded their deadline.</span>
      <button className="refresh-btn" style={{marginLeft:'auto',borderColor:'#c05621',color:'#f6ad55'}}
        onClick={onViewAlerts}>View alerts</button>
    </div>
  );
}

function SlaPanel({ alerts, onAck, onClose }) {
  const fmt = ms => ms < 60000 ? `${(ms/1000).toFixed(1)}s` : `${(ms/60000).toFixed(1)}m`;
  // Other deadline alerts (the sla alias is listed above as SLA alerts)
  const [missed, setMissed] = useState([]);
  useEffect(() => {
    apiFetch(`${BASE}/deadlines?missed=true&limit=50`).then(r => r.json())
      .then(d => { if (Array.isArray(d)) setMissed(d.filter(x => x.name !== 'sla')); }).catch(() => {});
  }, [alerts.length]);
  return (
    <div className="sla-panel">
      <h4>
        ⚠️ SLA Alerts
        <span style={{flex:1}} />
        <button className="log-btn" onClick={onClose}>✕</button>
      </h4>
      {alerts.length === 0 && <div className="empty" style={{padding:'12px 0'}}>No alerts</div>}
      {alerts.map(a => (
        <div key={a.id} className="sla-row">
          <span className="sla-dag">{a.dag_id}</span>
          <span className="sla-time">
            run <code style={{fontSize:11}}>{a.dag_run_id.slice(-8)}</code>
            {' · '}elapsed {fmt(a.elapsed_ms)} / SLA {fmt(a.sla_ms)}
            {' · '}{new Date(a.fired_at).toLocaleTimeString()}
          </span>
          {a.acked
            ? <span className="sla-acked">acked</span>
            : <button className="log-btn" style={{color:'#f6ad55',borderColor:'#c05621'}}
                onClick={() => onAck(a.id)}>Ack</button>
          }
        </div>
      ))}
      {missed.length > 0 && <h4 style={{marginTop:12}}>⏰ Missed deadlines</h4>}
      {missed.map(d => (
        <div key={d.id} className="sla-row">
          <span className="sla-dag">{d.dag_id}</span>
          <span className="sla-time">
            run <code style={{fontSize:11}}>{d.dag_run_id.slice(-8)}</code>
            {' · '}{d.name} ({d.reference}) due {new Date(d.deadline_time).toLocaleString()}
          </span>
        </div>
      ))}
    </div>
  );
}

// ── Dag Cards ─────────────────────────────────────────────────────────
function DagCard({ dag, onTrigger, onSelect, onTogglePause, onTagClick, onLineage, selected }) {
  const [triggering, setTriggering] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [showTriggerForm, setShowTriggerForm] = useState(false);
  const [confText, setConfText] = useState('{}');
  const [tagsText, setTagsText] = useState('');
  const [triggerNote, setTriggerNote] = useState('');
  const [triggerError, setTriggerError] = useState('');

  const trigger = async (withForm = false) => {
    setTriggerError('');
    let body = {};
    if (withForm) {
      try {
        const conf = JSON.parse(confText);
        if (typeof conf !== 'object' || Array.isArray(conf)) throw new Error('"conf" must be a JSON object');
        const tags = tagsText.trim() ? tagsText.split(',').map(t => t.trim()).filter(Boolean) : [];
        body = { conf, tags, ...(triggerNote ? { note: triggerNote } : {}) };
      } catch (e) { setTriggerError(e.message); return; }
    }
    setTriggering(true);
    await apiFetch(`${BASE}/dags/${dag.id}/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    setTriggering(false);
    setShowTriggerForm(false);
    onTrigger();
  };

  const togglePause = async () => {
    setToggling(true);
    const action = dag.is_paused ? 'resume' : 'pause';
    await apiFetch(`${BASE}/dags/${dag.id}/${action}`, { method: 'POST' });
    setToggling(false);
    onTogglePause();
  };

  return (
    <div className={`dag-card${dag.is_paused ? ' paused' : ''}`}
      style={{ borderColor: selected && !dag.is_paused ? '#3182ce' : undefined }}>
      <h3>
        {dag.id}
        {dag.is_paused && <span className="paused-badge">paused</span>}
      </h3>
      <div className="last-run" title={dag.last_run ? `Last run ${dag.last_run.run_id} — ${new Date(dag.last_run.created_at).toLocaleString()}` : undefined}>
        {dag.last_run
          ? <>{statusBadge(dag.last_run.state)} <span>{timeAgo(dag.last_run.created_at)}</span></>
          : <span style={{color:'#4a5568'}}>never run</span>}
      </div>
      <div className="dag-meta">
        {dag.datasets?.length > 0
          ? <span title={`Consumes: ${dag.dataset_condition_text || dag.datasets.join(' & ')}`}>
              📥 dataset-triggered{dag.schedule ? ` or ⏰ ${dag.schedule}` : ''}
            </span>
          : dag.schedule ? `⏰ ${dag.schedule}` : '● manual only'}
        {dag.outlets?.length > 0 && (
          <span title={`Produces: ${dag.outlets.join(', ')}`} style={{marginLeft:6,color:'#68d391'}}>
            📤 {dag.outlets.length} outlet{dag.outlets.length > 1 ? 's' : ''}
          </span>
        )}
      </div>
      {dag.tags?.length > 0 && (
        <div style={{display:'flex',gap:4,flexWrap:'wrap',marginBottom:8}}>
          {dag.tags.map(t => <span key={t} className="tag-chip" title={`Show Dags tagged ${t}`} onClick={() => onTagClick(t)}>#{t}</span>)}
        </div>
      )}
      <div className="tasks-list">
        {dag.tasks.map(t => <span key={t}>{t}</span>)}
      </div>
      <div style={{ display:'flex', gap:8, flexWrap:'wrap' }}>
        <button className="btn btn-primary btn-sm" onClick={() => trigger(false)} disabled={triggering}>
          {triggering ? '…' : '▶ Trigger'}
        </button>
        <button className="btn btn-sm" style={{background:'#2d3a50',color:'#90cdf4'}}
          onClick={() => setShowTriggerForm(v => !v)} title="Trigger with conf/tags">
          ⚙ With params
        </button>
        <button className={`btn btn-sm ${dag.is_paused ? 'btn-resume' : 'btn-pause'}`}
          onClick={togglePause} disabled={toggling}>
          {toggling ? '…' : dag.is_paused ? '▶ Resume' : '⏸ Pause'}
        </button>
        <button className="btn btn-sm" style={{background:'#2d3748',color:'#a0aec0'}}
          onClick={() => onSelect(dag.id)}>
          {selected ? 'Hide runs' : 'View runs'}
        </button>
        {(dag.datasets?.length > 0 || dag.outlets?.length > 0) && (
          <button className="btn btn-sm" style={{background:'#1a2f1a',color:'#68d391'}} title="Dataset lineage of this Dag"
            onClick={() => onLineage(dag.id)}>🧬 Lineage</button>
        )}
      </div>
      {showTriggerForm && (
        <div style={{marginTop:8,padding:10,background:'#1a2130',borderRadius:6,border:'1px solid #2d3748'}}>
          <div style={{fontSize:11,color:'#a0aec0',marginBottom:4}}>conf (JSON object)</div>
          <textarea value={confText} onChange={e => setConfText(e.target.value)} rows={3}
            style={{width:'100%',background:'#2d3748',color:'#e2e8f0',border:'none',borderRadius:4,padding:'4px 6px',fontSize:11,fontFamily:'monospace',boxSizing:'border-box'}} />
          <div style={{fontSize:11,color:'#a0aec0',marginTop:6,marginBottom:2}}>tags (comma-separated)</div>
          <input value={tagsText} onChange={e => setTagsText(e.target.value)} placeholder="etl, prod, debug"
            style={{width:'100%',background:'#2d3748',color:'#e2e8f0',border:'none',borderRadius:4,padding:'4px 6px',fontSize:11,boxSizing:'border-box'}} />
          <div style={{fontSize:11,color:'#a0aec0',marginTop:6,marginBottom:2}}>note (optional)</div>
          <input value={triggerNote} onChange={e => setTriggerNote(e.target.value)} placeholder="Reason for manual run…"
            style={{width:'100%',background:'#2d3748',color:'#e2e8f0',border:'none',borderRadius:4,padding:'4px 6px',fontSize:11,boxSizing:'border-box'}} />
          {triggerError && <div style={{fontSize:11,color:'#fc8181',marginTop:4}}>{triggerError}</div>}
          <button className="btn btn-primary btn-sm" style={{marginTop:8}} onClick={() => trigger(true)} disabled={triggering}>
            {triggering ? '…' : '▶ Trigger with params'}
          </button>
        </div>
      )}
    </div>
  );
}

// ── Log Panel ─────────────────────────────────────────────────────────
function LogPanel({ runId, taskId, onClose }) {
  const [lines, setLines] = useState([]);
  const fetch_ = useCallback(() =>
    apiFetch(`${BASE}/dag-runs/${runId}/tasks/${taskId}/logs`)
      .then(r => r.json()).then(setLines).catch(() => {}), [runId, taskId]);
  useInterval(fetch_, 2000);
  const fmt = ts => new Date(ts).toLocaleTimeString('en', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  return (
    <div className="log-panel">
      <div className="log-panel-header">
        <strong>{taskId}</strong> logs
        <span style={{flex:1}} />
        <button className="log-btn" onClick={fetch_}>↻</button>
        <button className="log-btn" onClick={onClose}>✕</button>
      </div>
      <div className="log-lines">
        {lines.length === 0 && <span style={{color:'#4a5568'}}>No logs yet</span>}
        {lines.map((l, i) => (
          <div key={i} className="log-line">
            <span className="log-ts">{fmt(l.ts)}</span>
            <span className={l.stream === 'stderr' ? 'log-stderr' : 'log-stdout'}>{l.line}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── DAG Graph ─────────────────────────────────────────────────────────
// Pure SVG graph, no external library.
// Layout: topological layers left-to-right, nodes sorted within each layer.

const STATE_COLORS = {
  success:   { fill: '#1c4532', stroke: '#68d391', text: '#68d391' },
  failed:    { fill: '#4a1515', stroke: '#fc8181', text: '#fc8181' },
  running:   { fill: '#1a365d', stroke: '#63b3ed', text: '#63b3ed' },
  queued:    { fill: '#1e2130', stroke: '#4a5568', text: '#a0aec0' },
  cancelled: { fill: '#1a1a1a', stroke: '#4a5568', text: '#718096' },
  skipped:   { fill: '#1e2340', stroke: '#667eea', text: '#a3bffa' },
  deferred:  { fill: '#2a2140', stroke: '#9f7aea', text: '#d6bcfa' },
  up_for_retry:      { fill: '#3d2c0a', stroke: '#d69e2e', text: '#f6e05e' },
  up_for_reschedule: { fill: '#13302f', stroke: '#38b2ac', text: '#81e6d9' },
  upstream_failed:   { fill: '#3d1f0f', stroke: '#ed8936', text: '#fbd38d' },
  removed:   { fill: '#141414', stroke: '#2d3748', text: '#4a5568' },
};

/**
 * Expand mapped tasks into uniquely-keyed nodes for graph rendering.
 * A task with map_index gets node_id = 'task[0]', 'task[1]', etc.
 * Downstream tasks that depend_on a mapped task get edges to ALL instances.
 * Returns array of enriched task objects with node_id, display_label, resolved_deps.
 */
function expandForGraph(tasks) {
  // Build set of mapped task_ids (those with non-null map_index)
  const mappedIds = new Set(tasks.filter(t => t.map_index !== null).map(t => t.task_id));
  // Build map: task_id → all node_ids for that task_id
  const nodesByTaskId = {};
  tasks.forEach(t => {
    const nodeId = t.map_index !== null ? `${t.task_id}[${t.map_index}]` : t.task_id;
    (nodesByTaskId[t.task_id] = nodesByTaskId[t.task_id] || []).push(nodeId);
  });

  return tasks.map(t => {
    const nodeId = t.map_index !== null ? `${t.task_id}[${t.map_index}]` : t.task_id;
    const label = t.map_index !== null
      ? `${t.task_id.length > 10 ? t.task_id.slice(0,9)+'…' : t.task_id}[${t.map_index}]`
      : t.task_id;
    // Expand deps: if dep is a mapped task, fan out to all its instances
    const resolvedDeps = (t.depends_on ?? []).flatMap(dep =>
      mappedIds.has(dep) ? (nodesByTaskId[dep] || [dep]) : [dep]
    );
    return { ...t, node_id: nodeId, display_label: label, resolved_deps: resolvedDeps };
  });
}

function topoLayers(tasks) {
  // tasks: enriched with node_id and resolved_deps from expandForGraph
  const layers = [];
  const assigned = {};

  // Kahn's algorithm using node_id as the unique key
  const inDeg = {};
  tasks.forEach(t => { inDeg[t.node_id] = 0; });
  tasks.forEach(t => t.resolved_deps?.forEach(() => { inDeg[t.node_id] = (inDeg[t.node_id] || 0) + 1; }));

  let queue = tasks.filter(t => (inDeg[t.node_id] || 0) === 0).map(t => t.node_id);
  while (queue.length) {
    layers.push([...queue]);
    queue.forEach(id => { assigned[id] = layers.length - 1; });
    const next = [];
    queue.forEach(id => {
      tasks.forEach(t => {
        if (t.resolved_deps?.includes(id)) {
          inDeg[t.node_id]--;
          if (inDeg[t.node_id] === 0) next.push(t.node_id);
        }
      });
    });
    queue = next;
  }
  // Any remaining (cycles / unresolved) go in last layer
  tasks.forEach(t => {
    if (assigned[t.node_id] === undefined) layers.push([t.node_id]);
  });

  return layers;
}

// Group colours — cycle through a palette by group index
const GROUP_PALETTE = ['#1a2744','#1a2f1a','#2f1a2f','#2f2a1a','#1a2f2f','#2a1a1f'];
const GROUP_LABEL_COLOR = ['#90cdf4','#68d391','#d6bcfa','#f6ad55','#81e6d9','#fc8181'];

// ── State icons (Unicode, no dependencies) ────────────────────────────
const STATE_ICON = {
  success:   '✓',
  failed:    '✗',
  running:   '▶',
  queued:    '◷',
  cancelled: '⊘',
  skipped:   '↷',
  deferred:  '⏸',
  up_for_retry:      '↻',
  up_for_reschedule: '⟳',
  upstream_failed:   '⚠',
  removed:   '−',
};

function DagGraph({ tasks, onClickTask }) {
  const NODE_W = 148, NODE_H = 52, GAP_X = 80, GAP_Y = 20, PAD = 28;
  const GROUP_PAD = 12, GROUP_LABEL_H = 18;

  // ── Zoom / pan state ────────────────────────────────────────────────
  const [vp, setVp] = useState({ tx: 0, ty: 0, k: 1 });
  const wrapRef = useRef(null);
  const dragRef = useRef(null);   // { startX, startY, startTx, startTy } | null

  // ── Layout (unchanged) ──────────────────────────────────────────────
  const expanded = expandForGraph(tasks);
  const layers = topoLayers(expanded);

  const pos = {};
  const maxRows = layers.reduce((m, l) => Math.max(m, l.length), 0);
  layers.forEach((layer, col) => {
    const totalH = layer.length * NODE_H + (layer.length - 1) * GAP_Y;
    layer.forEach((nodeId, row) => {
      pos[nodeId] = {
        x: PAD + col * (NODE_W + GAP_X),
        y: PAD + GROUP_LABEL_H + row * (NODE_H + GAP_Y) + (maxRows * (NODE_H + GAP_Y) - totalH) / 2,
      };
    });
  });

  const graphW = PAD * 2 + layers.length * NODE_W + Math.max(0, layers.length - 1) * GAP_X;
  const graphH = PAD * 2 + GROUP_LABEL_H + maxRows * NODE_H + Math.max(0, maxRows - 1) * GAP_Y;

  // ── Fit-to-viewport helper ──────────────────────────────────────────
  // Use refs for graphW/graphH to avoid stale closure in the timeout
  const graphSizeRef = useRef({ graphW, graphH });
  graphSizeRef.current = { graphW, graphH };

  const fitView = useCallback(() => {
    const wrap = wrapRef.current;
    if (!wrap) return;
    const { width: W, height: H } = wrap.getBoundingClientRect();
    if (W === 0 || H === 0) return;
    const { graphW: gW, graphH: gH } = graphSizeRef.current;
    const k = Math.min(1, Math.min(W / gW, H / gH)) * 0.92;
    setVp({ tx: (W - gW * k) / 2, ty: (H - gH * k) / 2, k });
  }, []);

  // Auto-fit on first render and when tasks change
  useEffect(() => { const t = setTimeout(fitView, 80); return () => clearTimeout(t); }, [tasks, fitView]);

  // ── Wheel zoom (anchored to cursor) ─────────────────────────────────
  const onWheel = useCallback((e) => {
    e.preventDefault();
    const wrap = wrapRef.current;
    if (!wrap) return;
    const rect = wrap.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const delta = e.deltaY < 0 ? 1.12 : 1 / 1.12;
    setVp(prev => {
      const k = Math.max(0.15, Math.min(4, prev.k * delta));
      // Keep the point under the cursor fixed: solve for new tx,ty
      const tx = mx - (mx - prev.tx) * (k / prev.k);
      const ty = my - (my - prev.ty) * (k / prev.k);
      return { tx, ty, k };
    });
  }, []);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, [onWheel]);

  // ── Pointer drag (pan) ───────────────────────────────────────────────
  const onPointerDown = useCallback((e) => {
    if (e.button !== 0) return;
    wrapRef.current?.classList.add('dragging');
    dragRef.current = { startX: e.clientX, startY: e.clientY, startTx: vp.tx, startTy: vp.ty };
    e.currentTarget.setPointerCapture(e.pointerId);
  }, [vp.tx, vp.ty]);

  const onPointerMove = useCallback((e) => {
    if (!dragRef.current) return;
    const dx = e.clientX - dragRef.current.startX;
    const dy = e.clientY - dragRef.current.startY;
    setVp(prev => ({ ...prev, tx: dragRef.current.startTx + dx, ty: dragRef.current.startTy + dy }));
  }, []);

  const onPointerUp = useCallback((e) => {
    wrapRef.current?.classList.remove('dragging');
    dragRef.current = null;
  }, []);

  // ── Click vs drag discrimination ────────────────────────────────────
  // Store pointer-down position; if moved > 4px it was a drag, not a click
  const clickOrigin = useRef(null);
  const onNodePointerDown = useCallback((e, taskId) => {
    clickOrigin.current = { x: e.clientX, y: e.clientY, taskId };
  }, []);
  const onNodePointerUp = useCallback((e) => {
    if (!clickOrigin.current) return;
    const dx = Math.abs(e.clientX - clickOrigin.current.x);
    const dy = Math.abs(e.clientY - clickOrigin.current.y);
    if (dx < 5 && dy < 5) onClickTask(clickOrigin.current.taskId);
    clickOrigin.current = null;
  }, [onClickTask]);

  // ── Duration formatting ─────────────────────────────────────────────
  const dur = (t) => {
    if (!t.started_at) return '';
    const ms = new Date(t.ended_at ?? Date.now()) - new Date(t.started_at);
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms/1000).toFixed(1)}s`;
    return `${Math.floor(ms/60000)}m ${Math.round((ms%60000)/1000)}s`;
  };

  // ── Group bounding boxes ────────────────────────────────────────────
  const groups = {};
  expanded.forEach(t => {
    if (!t.group_id) return;
    const p = pos[t.node_id];
    if (!p) return;
    if (!groups[t.group_id]) groups[t.group_id] = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
    const g = groups[t.group_id];
    g.minX = Math.min(g.minX, p.x);
    g.minY = Math.min(g.minY, p.y);
    g.maxX = Math.max(g.maxX, p.x + NODE_W);
    g.maxY = Math.max(g.maxY, p.y + NODE_H);
  });
  const groupIds = Object.keys(groups);

  const WRAP_H = Math.max(220, Math.min(520, graphH + 60));

  return (
    <div ref={wrapRef} className="dag-graph-wrap" style={{ height: WRAP_H }}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
    >
      {/* Zoom controls */}
      <div className="dag-graph-controls" onPointerDown={e => e.stopPropagation()}>
        <button title="Zoom in"  onClick={() => setVp(p => ({ ...p, k: Math.min(4, p.k * 1.2) }))}>+</button>
        <button title="Zoom out" onClick={() => setVp(p => ({ ...p, k: Math.max(0.15, p.k / 1.2) }))}>−</button>
        <button title="Fit to view" onClick={fitView}>⤢</button>
      </div>

      <svg width="100%" height="100%" style={{ overflow: 'visible' }}>
        <defs>
          <marker id="arr" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
            <path d="M0,0 L7,3.5 L0,7 Z" fill="#4a5568" />
          </marker>
          <marker id="arr-hi" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
            <path d="M0,0 L7,3.5 L0,7 Z" fill="#63b3ed" />
          </marker>
        </defs>

        <g transform={`translate(${vp.tx},${vp.ty}) scale(${vp.k})`}>

          {/* Group background rects */}
          {groupIds.map((gid, i) => {
            const { minX, minY, maxX, maxY } = groups[gid];
            const rx = minX - GROUP_PAD, ry = minY - GROUP_LABEL_H - GROUP_PAD;
            const rw = maxX - minX + GROUP_PAD * 2, rh = maxY - minY + GROUP_LABEL_H + GROUP_PAD * 2;
            const fill = GROUP_PALETTE[i % GROUP_PALETTE.length];
            const lc   = GROUP_LABEL_COLOR[i % GROUP_LABEL_COLOR.length];
            return (
              <g key={gid}>
                <rect x={rx} y={ry} width={rw} height={rh}
                  fill={fill} stroke={lc} strokeWidth="1" rx="10" opacity="0.55" />
                <text x={rx + 10} y={ry + 13} fill={lc} fontSize="11" fontFamily="monospace" fontWeight="600">{gid}</text>
              </g>
            );
          })}

          {/* Edges */}
          {expanded.map(t =>
            (t.resolved_deps ?? []).map(dep => {
              if (!pos[dep] || !pos[t.node_id]) return null;
              const x1 = pos[dep].x + NODE_W,     y1 = pos[dep].y + NODE_H / 2;
              const x2 = pos[t.node_id].x,         y2 = pos[t.node_id].y + NODE_H / 2;
              const cx = (x1 + x2) / 2;
              const isRunning = t.state === 'running' || expanded.find(n => n.node_id === dep)?.state === 'running';
              return (
                <path key={`${dep}->${t.node_id}`}
                  className={`dag-edge${isRunning ? ' edge-running' : ''}`}
                  d={`M${x1},${y1} C${cx},${y1} ${cx},${y2} ${x2},${y2}`}
                  fill="none"
                  stroke={isRunning ? '#4299e1' : '#4a5568'}
                  strokeWidth={isRunning ? 2 : 1.5}
                  markerEnd={isRunning ? 'url(#arr-hi)' : 'url(#arr)'}
                />
              );
            })
          )}

          {/* Nodes */}
          {expanded.map(t => {
            const { x, y } = pos[t.node_id] ?? { x: 0, y: 0 };
            const c = STATE_COLORS[t.state] ?? STATE_COLORS.queued;
            const icon = STATE_ICON[t.state] ?? '';
            const d = dur(t);
            // Truncate label to fit node width
            const maxChars = 14;
            const label = t.display_label.length > maxChars
              ? t.display_label.slice(0, maxChars - 1) + '…'
              : t.display_label;
            return (
              <g key={t.node_id} className="dag-node"
                transform={`translate(${x},${y})`}
                onPointerDown={e => { e.stopPropagation(); onNodePointerDown(e, t.task_id); }}
                onPointerUp={e => { e.stopPropagation(); onNodePointerUp(e); }}
              >
                {/* Drop shadow */}
                <rect x="2" y="3" width={NODE_W} height={NODE_H} fill="rgba(0,0,0,0.35)" rx="8" />
                {/* Main rect */}
                <rect width={NODE_W} height={NODE_H}
                  fill={c.fill} stroke={c.stroke} strokeWidth="1.5" rx="8" />
                {/* Left state bar */}
                <rect width="4" height={NODE_H} fill={c.stroke} rx="2" />
                {/* Icon */}
                <text x="14" y={NODE_H/2 + 5} fill={c.stroke} fontSize="13" fontWeight="bold">{icon}</text>
                {/* Task label */}
                <text x={NODE_W/2 + 6} y={d ? 18 : NODE_H/2 + 5}
                  textAnchor="middle" fill={c.text} fontSize="12" fontFamily="monospace" fontWeight="600">
                  {label}
                </text>
                {/* Duration */}
                {d && (
                  <text x={NODE_W/2 + 6} y={NODE_H - 9}
                    textAnchor="middle" fill="#718096" fontSize="10" fontFamily="monospace">
                    {d}
                  </text>
                )}
              </g>
            );
          })}

        </g>
      </svg>
    </div>
  );
}

// ── XCom Panel ───────────────────────────────────────────────────────
function XComPanel({ runId }) {
  const [xcoms, setXcoms] = useState([]);
  const fetch_ = useCallback(() =>
    apiFetch(`${BASE}/dag-runs/${runId}/xcoms`)
      .then(r => r.json())
      .then(data => { if (Array.isArray(data)) setXcoms(data); })
      .catch(() => {}), [runId]);
  useInterval(fetch_, 3000);

  const fmt = ts => new Date(ts).toLocaleTimeString('en', { hour12:false, hour:'2-digit', minute:'2-digit', second:'2-digit' });
  const fmtVal = v => {
    if (v === null || v === undefined) return 'null';
    if (typeof v === 'object') {
      try { return JSON.stringify(v); } catch { return String(v); }
    }
    return String(v);
  };

  return (
    <div className="xcom-panel">
      <div className="xcom-panel-header">
        <span>XCom values</span>
        <span style={{marginLeft:'auto',fontSize:11}}>{xcoms.length} entr{xcoms.length === 1 ? 'y' : 'ies'}</span>
        <button className="log-btn" onClick={fetch_}>↻</button>
      </div>
      {xcoms.length === 0
        ? <div style={{padding:'10px 12px',color:'#4a5568',fontSize:12}}>No XCom values yet</div>
        : xcoms.map((x, i) => (
          <div key={i} className="xcom-row">
            <span className="xcom-task">{x.task_id}</span>
            <span className="xcom-key">{x.key}</span>
            <span className="xcom-val" title={fmtVal(x.value)}>{fmtVal(x.value)}</span>
            <span className="xcom-ts">{fmt(x.pushed_at)}</span>
          </div>
        ))
      }
    </div>
  );
}

// ── Note Editor ──────────────────────────────────────────────────────
function NoteEditor({ runId, initialNote }) {
  const [editing, setEditing] = useState(false);
  const [note, setNote] = useState(initialNote ?? '');
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    await apiFetch(`${BASE}/dag-runs/${runId}/note`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ note }),
    });
    setSaving(false);
    setEditing(false);
  };

  if (!editing) return (
    <div style={{marginBottom:8,fontSize:12}}>
      {note
        ? <span style={{color:'#a0aec0'}}>📝 {note} <button className="log-btn" onClick={() => setEditing(true)}>edit</button></span>
        : <button className="log-btn" onClick={() => setEditing(true)}>+ Add note</button>
      }
    </div>
  );
  return (
    <div style={{marginBottom:8,display:'flex',gap:6,alignItems:'flex-start'}}>
      <textarea value={note} onChange={e => setNote(e.target.value)} rows={2}
        placeholder="Add a note…"
        style={{flex:1,background:'#2d3748',color:'#e2e8f0',border:'1px solid #4a5568',borderRadius:4,padding:'4px 6px',fontSize:12,resize:'vertical'}} />
      <div style={{display:'flex',flexDirection:'column',gap:4}}>
        <button className="btn btn-sm" onClick={save} disabled={saving}>{saving ? '…' : 'Save'}</button>
        <button className="refresh-btn" onClick={() => setEditing(false)}>✕</button>
      </div>
    </div>
  );
}

// ── Gantt Chart ──────────────────────────────────────────────────────
// Shows task duration bars for a single run on a shared time axis.
function GanttChart({ tasks }) {
  if (!tasks || !tasks.length) return <div className="empty">No tasks</div>;

  const started = tasks.filter(t => t.started_at);
  if (!started.length) return <div className="empty">No tasks have started yet</div>;

  const minTs = Math.min(...started.map(t => new Date(t.started_at).getTime()));
  const maxTs = Math.max(...started.map(t => {
    const end = t.ended_at ? new Date(t.ended_at).getTime() : Date.now();
    return end;
  }));
  const totalMs = Math.max(maxTs - minTs, 1);

  const STATE_COLORS = {
    success: '#48bb78', failed: '#fc8181', running: '#63b3ed',
    queued: '#a0aec0', skipped: '#667eea', deferred: '#9f7aea',
    cancelled: '#718096', up_for_retry: '#d69e2e', up_for_reschedule: '#38b2ac',
    upstream_failed: '#ed8936', removed: '#2d3748',
  };

  const rowH = 28, padding = { left: 120, right: 16, top: 8, bottom: 24 };
  const chartW = 460;
  const svgH = tasks.length * rowH + padding.top + padding.bottom;

  // Time axis ticks (4 ticks)
  const tickCount = 4;
  const ticks = Array.from({ length: tickCount + 1 }, (_, i) => ({
    ms: minTs + (totalMs * i) / tickCount,
    x: padding.left + (chartW * i) / tickCount,
  }));

  const fmtDuration = (ms) => ms < 1000 ? `${ms}ms` : ms < 60000 ? `${(ms/1000).toFixed(1)}s` : `${Math.floor(ms/60000)}m${Math.floor((ms%60000)/1000)}s`;

  return (
    <div style={{overflowX:'auto'}}>
      <svg width={padding.left + chartW + padding.right} height={svgH}
        style={{fontFamily:'monospace',fontSize:11}}>
        {/* Background stripes */}
        {tasks.map((_, i) => (
          <rect key={i} x={0} y={padding.top + i*rowH} width={padding.left+chartW+padding.right}
            height={rowH} fill={i%2===0 ? '#1a1f2e' : '#1e2438'} />
        ))}
        {/* Grid lines */}
        {ticks.map((t, i) => (
          <line key={i} x1={t.x} y1={padding.top} x2={t.x} y2={svgH-padding.bottom}
            stroke="#2d3748" strokeWidth={1} strokeDasharray={i===0?'none':'3,3'} />
        ))}
        {/* Tick labels */}
        {ticks.map((t, i) => (
          <text key={i} x={t.x} y={svgH-6} textAnchor="middle" fill="#718096" fontSize={9}>
            {i===0 ? '0s' : fmtDuration(t.ms - minTs)}
          </text>
        ))}
        {/* Task rows */}
        {tasks.map((t, i) => {
          const y = padding.top + i * rowH;
          const color = STATE_COLORS[t.state] || '#718096';
          const hasStart = !!t.started_at;
          const startX = hasStart
            ? padding.left + (new Date(t.started_at).getTime() - minTs) / totalMs * chartW
            : padding.left;
          const endX = hasStart
            ? padding.left + ((t.ended_at ? new Date(t.ended_at).getTime() : Date.now()) - minTs) / totalMs * chartW
            : padding.left;
          const barW = Math.max(endX - startX, 4);
          const durMs = hasStart
            ? (t.ended_at ? new Date(t.ended_at) : new Date()).getTime() - new Date(t.started_at).getTime()
            : 0;

          return (
            <g key={t.task_id}>
              {/* Task label */}
              <text x={padding.left-4} y={y + rowH/2 + 4} textAnchor="end"
                fill="#e2e8f0" fontSize={10} style={{maxWidth:110}}>
                {t.task_id.length > 16 ? t.task_id.slice(0,14)+'…' : t.task_id}
              </text>
              {/* Duration bar */}
              {hasStart && (
                <rect x={startX} y={y+4} width={barW} height={rowH-8}
                  rx={3} fill={color} opacity={t.state==='running' ? 0.9 : 0.75} />
              )}
              {/* State dot (queued/not started) */}
              {!hasStart && (
                <circle cx={padding.left+8} cy={y+rowH/2} r={4} fill={color} opacity={0.6} />
              )}
              {/* Duration label inside bar if wide enough */}
              {hasStart && barW > 30 && (
                <text x={startX+barW/2} y={y+rowH/2+4} textAnchor="middle"
                  fill="#fff" fontSize={9} fontWeight="bold">
                  {fmtDuration(durMs)}
                </text>
              )}
            </g>
          );
        })}
        {/* Current time marker for running runs */}
        {tasks.some(t => t.state === 'running') && (() => {
          const nowX = padding.left + (Date.now() - minTs) / totalMs * chartW;
          return nowX <= padding.left + chartW ? (
            <line x1={nowX} y1={padding.top} x2={nowX} y2={svgH-padding.bottom}
              stroke="#63b3ed" strokeWidth={1.5} strokeDasharray="4,2" opacity={0.7} />
          ) : null;
        })()}
      </svg>
    </div>
  );
}

// ── Grid View ─────────────────────────────────────────────────────────
// Run × task heatmap: rows = recent runs, columns = task_ids, cells = state color.
// Equivalent to Airflow's Grid view.
function GridView({ dagId }) {
  const [rows, setRows] = useState([]);
  const [taskIds, setTaskIds] = useState([]);

  const load = useCallback(() => {
    apiFetch(`${BASE}/dags/${dagId}/runs?limit=25`).then(r=>r.json()).then(async data => {
      const runs = data.items ?? [];
      if (!runs.length) { setRows([]); return; }

      // Collect all task_ids from all runs
      const allTaskIds = new Set();
      const runsWithTasks = await Promise.all(runs.map(async run => {
        try {
          const r = await apiFetch(`${BASE}/dag-runs/${run.run_id}/tasks`);
          const tasks = await r.json();
          const arr = Array.isArray(tasks) ? tasks : (tasks.items ?? []);
          arr.forEach(t => allTaskIds.add(t.task_id));
          return { run, tasks: arr };
        } catch { return { run, tasks: [] }; }
      }));

      const sorted = [...allTaskIds].sort();
      setTaskIds(sorted);
      setRows(runsWithTasks);
    }).catch(()=>{});
  }, [dagId]);

  useInterval(load, 5000);

  const STATE_COLORS = {
    success:'#48bb78', failed:'#fc8181', running:'#63b3ed',
    queued:'#a0aec0', skipped:'#667eea', deferred:'#9f7aea', cancelled:'#718096',
    up_for_retry:'#d69e2e', up_for_reschedule:'#38b2ac', upstream_failed:'#ed8936', removed:'#2d3748',
  };

  if (!rows.length) return <div className="empty">No runs yet</div>;

  return (
    <div style={{overflowX:'auto'}}>
      <table style={{borderCollapse:'collapse',fontSize:11,minWidth:'100%'}}>
        <thead>
          <tr>
            <th style={{color:'#a0aec0',padding:'4px 8px',textAlign:'left',minWidth:90,borderBottom:'1px solid #2d3748'}}>Run</th>
            <th style={{color:'#a0aec0',padding:'4px 4px',textAlign:'center',borderBottom:'1px solid #2d3748'}}>State</th>
            {taskIds.map(id => (
              <th key={id} style={{color:'#a0aec0',padding:'4px 2px',maxWidth:60,borderBottom:'1px solid #2d3748',writingMode:'vertical-rl',transform:'rotate(180deg)',fontSize:10}}>
                {id.length>12 ? id.slice(0,10)+'…' : id}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ run, tasks }) => {
            const taskMap = {};
            tasks.forEach(t => { taskMap[t.task_id] = t.state; });
            return (
              <tr key={run.run_id} style={{borderBottom:'1px solid #1a1f2e'}}>
                <td style={{padding:'3px 8px',fontFamily:'monospace',color:'#718096',fontSize:10}}>
                  {run.run_id.slice(-8)}
                  <div style={{fontSize:9,color:'#4a5568'}}>{new Date(run.created_at).toISOString().slice(5,16).replace('T',' ')}</div>
                </td>
                <td style={{padding:'3px 4px',textAlign:'center'}}>
                  <span style={{display:'inline-block',width:10,height:10,borderRadius:3,background:STATE_COLORS[run.state]||'#4a5568'}} title={run.state} />
                </td>
                {taskIds.map(id => {
                  const state = taskMap[id];
                  const color = state ? (STATE_COLORS[state]||'#4a5568') : '#2d3748';
                  return (
                    <td key={id} style={{padding:'3px 2px',textAlign:'center'}}>
                      <span style={{display:'inline-block',width:14,height:14,borderRadius:2,background:color,opacity:state?0.85:0.2}}
                        title={state ? `${id}: ${state}` : `${id}: —`} />
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
      <div style={{marginTop:8,display:'flex',gap:12,flexWrap:'wrap'}}>
        {Object.entries(STATE_COLORS).map(([s,c]) => (
          <span key={s} style={{display:'flex',alignItems:'center',gap:4,fontSize:10,color:'#a0aec0'}}>
            <span style={{display:'inline-block',width:10,height:10,borderRadius:2,background:c,opacity:0.85}} />{s}
          </span>
        ))}
      </div>
    </div>
  );
}

// ── Calendar View ─────────────────────────────────────────────────────
// Month heatmap of run counts + success rate per day for a DAG.
function CalendarView({ dagId }) {
  const [runsByDay, setRunsByDay] = useState({});
  const [month, setMonth] = useState(() => {
    const now = new Date(); return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  });

  useEffect(() => {
    apiFetch(`${BASE}/dags/${dagId}/runs?limit=200`).then(r=>r.json()).then(data => {
      const byDay = {};
      (data.items ?? []).forEach(run => {
        const day = run.created_at.slice(0, 10);
        if (!byDay[day]) byDay[day] = { total: 0, success: 0, failed: 0 };
        byDay[day].total++;
        if (run.state === 'success') byDay[day].success++;
        else if (run.state === 'failed') byDay[day].failed++;
      });
      setRunsByDay(byDay);
    }).catch(()=>{});
  }, [dagId, month]);

  const [year, mon] = month.split('-').map(Number);
  const firstDay = new Date(year, mon-1, 1);
  const lastDay = new Date(year, mon, 0);
  const startDow = firstDay.getDay(); // 0=Sun
  const daysInMonth = lastDay.getDate();

  const prevMonth = () => {
    const d = new Date(year, mon-2, 1);
    setMonth(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`);
  };
  const nextMonth = () => {
    const d = new Date(year, mon, 1);
    setMonth(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`);
  };

  const cellColor = (dayData) => {
    if (!dayData || !dayData.total) return '#1a1f2e';
    const rate = dayData.success / dayData.total;
    if (dayData.total === 0) return '#1a1f2e';
    if (rate === 1) return '#276749';
    if (rate >= 0.7) return '#2f855a';
    if (rate >= 0.4) return '#d69e2e';
    return '#9b2c2c';
  };

  // Build grid: 7 cols, up to 6 rows
  const cells = [];
  for (let i = 0; i < startDow; i++) cells.push(null);
  for (let d = 1; d <= daysInMonth; d++) cells.push(d);

  const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];

  return (
    <div>
      <div style={{display:'flex',alignItems:'center',gap:12,marginBottom:8}}>
        <button className="refresh-btn" onClick={prevMonth}>‹</button>
        <span style={{color:'#e2e8f0',fontWeight:'bold',minWidth:130,textAlign:'center'}}>
          {MONTHS[mon-1]} {year}
        </span>
        <button className="refresh-btn" onClick={nextMonth}>›</button>
      </div>
      <div style={{display:'grid',gridTemplateColumns:'repeat(7,1fr)',gap:3,maxWidth:280}}>
        {['Su','Mo','Tu','We','Th','Fr','Sa'].map(d => (
          <div key={d} style={{textAlign:'center',fontSize:10,color:'#718096',paddingBottom:2}}>{d}</div>
        ))}
        {cells.map((d, i) => {
          if (!d) return <div key={`e${i}`} />;
          const dayStr = `${month}-${String(d).padStart(2,'0')}`;
          const data = runsByDay[dayStr];
          const isToday = dayStr === new Date().toISOString().slice(0,10);
          return (
            <div key={d} title={data ? `${dayStr}: ${data.total} run(s), ${data.success} success, ${data.failed} failed` : dayStr}
              style={{
                width:'100%', aspectRatio:'1', borderRadius:3,
                background: cellColor(data),
                border: isToday ? '1px solid #63b3ed' : '1px solid transparent',
                display:'flex', alignItems:'center', justifyContent:'center',
                fontSize: 9, color: data?.total ? '#e2e8f0' : '#4a5568',
                cursor: data?.total ? 'default' : 'default',
              }}>
              {d}
              {data?.total ? <span style={{fontSize:7,marginLeft:1,color:'#a0aec0'}}>·{data.total}</span> : null}
            </div>
          );
        })}
      </div>
      <div style={{marginTop:8,display:'flex',gap:10,flexWrap:'wrap',fontSize:10,color:'#a0aec0'}}>
        {[['#276749','100% success'],['#2f855a','≥70%'],['#d69e2e','≥40%'],['#9b2c2c','<40%'],['#1a1f2e','no runs']].map(([c,l]) => (
          <span key={l} style={{display:'flex',alignItems:'center',gap:3}}>
            <span style={{display:'inline-block',width:10,height:10,borderRadius:2,background:c}} />{l}
          </span>
        ))}
      </div>
    </div>
  );
}

// ── Task / run actions ───────────────────────────────────────────────
// Every action asks the API for a dry run first, so the confirmation lists what will change.
async function confirmAndApply(url, method, body, verb) {
  const dry = await apiFetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, dry_run: true }) });
  const preview = await dry.json();
  if (!dry.ok) { alert(preview.error ?? `${verb} failed`); return false; }
  const list = preview.task_instances ?? [];
  if (list.length === 0) { alert(`${verb}: nothing to change — no task instance is in a state this action applies to.`); return false; }
  const lines = list.slice(0, 15).map(t => `  ${t.task_id}${t.map_index !== null ? `[${t.map_index}]` : ''} (${t.state})`);
  if (list.length > 15) lines.push(`  … and ${list.length - 15} more`);
  if (!confirm(`${verb}: ${list.length} task instance(s) will change\n\n${lines.join('\n')}`)) return false;
  const res = await apiFetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) { alert((await res.json().catch(() => ({}))).error ?? `${verb} failed`); return false; }
  return true;
}

function TaskActions({ runId, taskId, onDone }) {
  const [downstream, setDownstream] = useState(true);
  const [upstream, setUpstream] = useState(false);
  const [onlyFailed, setOnlyFailed] = useState(false);
  const [busy, setBusy] = useState(false);
  const run = async (fn) => { setBusy(true); try { if (await fn()) onDone(); } finally { setBusy(false); } };
  const rel = { include_downstream: downstream, include_upstream: upstream };
  const taskUrl = `${BASE}/dag-runs/${runId}/tasks/${encodeURIComponent(taskId)}`;
  const mark = (state) => run(() => confirmAndApply(taskUrl, 'PATCH', { state, ...rel }, `Mark ${state}`));
  return (
    <div className="task-actions">
      <strong style={{color:'#e2e8f0',fontFamily:'monospace'}}>{taskId}</strong>
      <label><input type="checkbox" checked={downstream} onChange={e => setDownstream(e.target.checked)} />downstream</label>
      <label><input type="checkbox" checked={upstream} onChange={e => setUpstream(e.target.checked)} />upstream</label>
      <label title="Clear only failed / upstream_failed instances"><input type="checkbox" checked={onlyFailed} onChange={e => setOnlyFailed(e.target.checked)} />failed only</label>
      <button className="log-btn" disabled={busy}
        onClick={() => run(() => confirmAndApply(`${taskUrl}/clear`, 'POST', { ...rel, only_failed: onlyFailed }, 'Clear'))}>↺ Clear</button>
      <button className="log-btn" disabled={busy} style={{color:'#68d391'}} onClick={() => mark('success')}>✓ Mark success</button>
      <button className="log-btn" disabled={busy} style={{color:'#fc8181'}} onClick={() => mark('failed')}>✗ Mark failed</button>
      <button className="log-btn" disabled={busy} style={{color:'#a3bffa'}} onClick={() => mark('skipped')}>↷ Mark skipped</button>
    </div>
  );
}

function RunActions({ runId, isActive, onDone }) {
  const [busy, setBusy] = useState(false);
  const act = async (url, method, body, verb) => {
    setBusy(true);
    try { if (await confirmAndApply(url, method, body, verb)) onDone(); } finally { setBusy(false); }
  };
  const runUrl = `${BASE}/dag-runs/${runId}`;
  return (
    <>
      {!isActive && <button className="btn btn-sm" disabled={busy} title="Clear every task and run again"
        onClick={() => act(`${runUrl}/clear`, 'POST', {}, 'Re-run')}>↺ Re-run</button>}
      {!isActive && <button className="btn btn-sm" disabled={busy} title="Clear only failed / upstream_failed tasks"
        onClick={() => act(`${runUrl}/clear`, 'POST', { only_failed: true }, 'Clear failed')}>↺ Clear failed</button>}
      <button className="btn btn-sm" disabled={busy} style={{background:'#1c4532',color:'#68d391'}}
        onClick={() => act(runUrl, 'PATCH', { state: 'success' }, 'Mark run success')}>✓ Mark success</button>
      <button className="btn btn-sm" disabled={busy} style={{background:'#4a1515',color:'#fc8181'}}
        onClick={() => act(runUrl, 'PATCH', { state: 'failed' }, 'Mark run failed')}>✗ Mark failed</button>
    </>
  );
}

// ── Run Detail ────────────────────────────────────────────────────────
function RunDetail({ runId, onClose }) {
  const [run, setRun] = useState(null);
  const [logTask, setLogTask] = useState(null);
  const [cancelling, setCancelling] = useState(false);
  const [view, setView] = useState('graph');  // 'graph' | 'list'

  const fetch_ = useCallback(() =>
    apiFetch(`${BASE}/dag-runs/${runId}`).then(r=>r.json()).then(setRun).catch(()=>{}), [runId]);
  useInterval(fetch_, 2000);

  const cancel = async () => {
    setCancelling(true);
    await apiFetch(`${BASE}/dag-runs/${runId}/cancel`, { method: 'POST' });
    setCancelling(false);
    fetch_();
  };

  const isActive = run && (run.state === 'queued' || run.state === 'running');

  if (!run) return <div className="run-detail"><div className="empty">Loading…</div></div>;
  return (
    <div className="run-detail">
      <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:12,flexWrap:'wrap'}}>
        <h3 style={{flex:1,marginBottom:0}}>Run: <code style={{fontSize:12}}>{runId.slice(-8)}</code></h3>
        {statusBadge(run.state)}
        {run.dag_version && (
          <span title={`Dag version: ${run.dag_version}`}
            style={{fontSize:10,fontFamily:'monospace',color:'#718096',background:'#1e2130',padding:'2px 6px',borderRadius:4}}>
            v{run.dag_version}
          </span>
        )}
        {run.logical_date && (
          <span title="Backfill logical date"
            style={{fontSize:10,fontFamily:'monospace',color:'#90cdf4',background:'#1a2744',padding:'2px 6px',borderRadius:4}}>
            ⏮ {new Date(run.logical_date).toISOString().slice(0,10)}
          </span>
        )}
        {(run.tags ?? []).map(tag => (
          <span key={tag} style={{fontSize:10,background:'#2d3748',color:'#a0aec0',padding:'2px 6px',borderRadius:10,border:'1px solid #4a5568'}}>
            #{tag}
          </span>
        ))}
        <div className="view-toggle">
          <button className={view==='graph'?'active':''} onClick={() => setView('graph')}>⬡ Graph</button>
          <button className={view==='list' ?'active':''} onClick={() => setView('list')}>≡ List</button>
          <button className={view==='gantt'?'active':''} onClick={() => setView('gantt')}>▬ Gantt</button>
          <button className={view==='grid' ?'active':''} onClick={() => setView('grid')}>⊞ Grid</button>
          <button className={view==='xcom' ?'active':''} onClick={() => setView('xcom')}>⇄ XCom</button>
        </div>
        {isActive && (
          <button className="btn btn-sm" disabled={cancelling}
            style={{background:'#4a1515',color:'#fc8181'}} onClick={cancel}>
            {cancelling ? '…' : '⏹ Cancel'}
          </button>
        )}
        <RunActions runId={runId} isActive={isActive} onDone={fetch_} />
        <button className="refresh-btn" onClick={fetch_}>↻</button>
        <button className="refresh-btn" onClick={onClose}>✕</button>
      </div>

      {logTask && <TaskActions runId={runId} taskId={logTask} onDone={fetch_} />}

      {view === 'graph' && (
        <>
          <DagGraph tasks={run.tasks}
            onClickTask={id => setLogTask(prev => prev === id ? null : id)} />
          {logTask && (
            <LogPanel runId={runId} taskId={logTask} onClose={() => setLogTask(null)} />
          )}
          {run.tasks.find(t => t.task_id === logTask)?.error && (
            <div className="error-msg" style={{marginTop:4}}>
              {run.tasks.find(t => t.task_id === logTask).error}
            </div>
          )}
        </>
      )}

      {/* Conf panel — shown when conf is non-empty */}
      {run.conf && Object.keys(run.conf).length > 0 && (
        <details style={{marginBottom:8}}>
          <summary style={{fontSize:11,color:'#a0aec0',cursor:'pointer',userSelect:'none'}}>⚙ conf ({Object.keys(run.conf).length} keys)</summary>
          <pre style={{fontSize:11,background:'#1e2130',padding:8,borderRadius:4,overflowX:'auto',marginTop:4,color:'#e2e8f0'}}>
            {JSON.stringify(run.conf, null, 2)}
          </pre>
        </details>
      )}
      {/* Note panel — inline editable */}
      <NoteEditor runId={runId} initialNote={run.note} />

      {view === 'gantt' && (
        <div style={{marginTop:8}}>
          <GanttChart tasks={run.tasks} />
        </div>
      )}

      {view === 'grid' && (
        <div style={{marginTop:8}}>
          <GridView dagId={run.dag_id} />
        </div>
      )}

      {view === 'xcom' && <XComPanel runId={runId} />}

      {view === 'list' && (() => {
        // Group tasks by group_id for display; ungrouped tasks shown inline
        const grouped = {};
        const ungrouped = [];
        run.tasks.forEach(t => {
          if (t.group_id) { (grouped[t.group_id] = grouped[t.group_id] || []).push(t); }
          else ungrouped.push(t);
        });
        const renderTask = (t) => (
          <div key={t.task_id}>
            <div className="task-row">
              <span className="task-name">{t.task_id}</span>
              {statusBadge(t.state)}
              {t.depends_on?.length > 0 &&
                <span className="task-deps">← {t.depends_on.join(', ')}</span>}
              <span className="duration">{duration(t.started_at, t.ended_at)}</span>
              <button className="log-btn"
                onClick={() => setLogTask(prev => prev === t.task_id ? null : t.task_id)}>
                {logTask === t.task_id ? 'hide logs' : 'logs'}
              </button>
            </div>
            {t.error && <div className="error-msg" style={{padding:'4px 0 4px 140px'}}>{t.error}</div>}
            {logTask === t.task_id &&
              <LogPanel runId={runId} taskId={t.task_id} onClose={() => setLogTask(null)} />}
          </div>
        );
        return (
          <>
            {Object.entries(grouped).map(([gid, gtasks]) => (
              <div key={gid} style={{border:'1px solid #2d3748',borderRadius:6,marginBottom:8,padding:'4px 8px'}}>
                <div style={{fontSize:10,fontFamily:'monospace',color:'#90cdf4',marginBottom:4,paddingBottom:4,borderBottom:'1px solid #2d3748'}}>
                  ▤ {gid}
                </div>
                {gtasks.map(renderTask)}
              </div>
            ))}
            {ungrouped.map(renderTask)}
          </>
        );
      })()}
    </div>
  );
}

// ── Runs Table ────────────────────────────────────────────────────────
function RunsTable({ dagId, onSelectRun }) {
  const [runs, setRuns] = useState([]);
  const [stateFilter, setStateFilter] = useState('');
  const [nextCursor, setNextCursor] = useState(null);
  const [loadingMore, setLoadingMore] = useState(false);

  const fetchPage = useCallback((cursor = null, replace = true) => {
    const url = `${BASE}/dags/${dagId}/runs?limit=20${stateFilter ? `&state=${stateFilter}` : ''}${cursor ? `&cursor=${cursor}` : ''}`;
    return apiFetch(url).then(r => r.json()).then(data => {
      setRuns(prev => replace ? (data.items ?? []) : [...prev, ...(data.items ?? [])]);
      setNextCursor(data.next_cursor ?? null);
    }).catch(() => {});
  }, [dagId, stateFilter]);

  // Poll first page every 3s (replaces current view to show new runs)
  useInterval(() => fetchPage(null, true), 3000);

  const loadMore = async () => {
    if (!nextCursor) return;
    setLoadingMore(true);
    await fetchPage(nextCursor, false);
    setLoadingMore(false);
  };

  const filter = (
    <select value={stateFilter} onChange={e => setStateFilter(e.target.value)} aria-label="Run state" style={{...FILTER_INPUT,marginBottom:8}}>
      <option value="">All states</option>
      {LAST_RUN_OPTIONS.filter(s => s !== 'none').map(s => <option key={s} value={s}>{s}</option>)}
    </select>
  );
  if (!runs.length) return <>{filter}<div className="empty">{stateFilter ? `No ${stateFilter} runs` : 'No runs yet'}</div></>;
  return (
    <>
      {filter}
      <table className="runs-table">
        <thead><tr>
          <th>Run ID</th><th>State</th><th>Tags</th><th>Version</th><th>Logical Date</th><th>Created</th><th></th>
        </tr></thead>
        <tbody>
          {runs.map(r => (
            <tr key={r.run_id}>
              <td><code style={{fontSize:12}}>{r.run_id.slice(-8)}</code></td>
              <td>{statusBadge(r.state)}</td>
              <td style={{fontSize:10}}>
                {(r.tags ?? []).map(tag => (
                  <span key={tag} style={{background:'#2d3748',color:'#a0aec0',padding:'1px 5px',borderRadius:8,marginRight:3,border:'1px solid #4a5568'}}>#{tag}</span>
                ))}
                {r.note && <span title={r.note} style={{color:'#718096'}}>📝</span>}
              </td>
              <td><code style={{fontSize:10,color:'#718096'}}>{r.dag_version ?? '—'}</code></td>
              <td style={{color:'#90cdf4',fontSize:11}}>
                {r.logical_date ? new Date(r.logical_date).toISOString().slice(0,10) : '—'}
              </td>
              <td style={{color:'#718096'}}>{new Date(r.created_at).toLocaleTimeString()}</td>
              <td><button className="refresh-btn" onClick={() => onSelectRun(r.run_id)}>detail →</button></td>
            </tr>
          ))}
        </tbody>
      </table>
      {nextCursor && (
        <div style={{textAlign:'center', marginTop:8}}>
          <button className="refresh-btn" onClick={loadMore} disabled={loadingMore}>
            {loadingMore ? '…' : 'Load more ↓'}
          </button>
        </div>
      )}
    </>
  );
}

// ── Stats Panel ───────────────────────────────────────────────────────
function fmtMs(ms) {
  if (ms == null) return '—';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms/1000).toFixed(1)}s`;
  return `${Math.floor(ms/60000)}m ${Math.round((ms%60000)/1000)}s`;
}

/**
 * DurationHistogram — renders a bar chart from the histogram buckets returned
 * by GET /dags/:id/stats. No external library; pure CSS flex bars.
 *
 * Each bar height is proportional to its count relative to the bucket with
 * the most runs. Tooltip shows [lo–hi) range and count on hover.
 */
function DurationHistogram({ buckets }) {
  if (!buckets || buckets.length === 0) return null;

  const maxCount = Math.max(...buckets.map(b => b.count));
  if (maxCount === 0) return null;

  return (
    <div className="hist-wrap">
      <div className="hist-label">Duration distribution</div>
      <div className="hist-bars">
        {buckets.map((b, i) => {
          const heightPct = maxCount > 0 ? (b.count / maxCount) * 100 : 0;
          const tip = `${fmtMs(b.lo)}–${fmtMs(b.hi)}: ${b.count} run${b.count !== 1 ? 's' : ''}`;
          return (
            <div key={i} className="hist-bar" style={{height:`${Math.max(heightPct, b.count > 0 ? 4 : 0)}%`}}>
              <span className="hist-tip">{tip}</span>
            </div>
          );
        })}
      </div>
      <div className="hist-axis">
        <span>{fmtMs(buckets[0].lo)}</span>
        <span>{fmtMs(buckets[Math.floor(buckets.length / 2)].lo)}</span>
        <span>{fmtMs(buckets[buckets.length - 1].hi)}</span>
      </div>
    </div>
  );
}

function StatsPanel({ dagId }) {
  const [stats, setStats] = useState(null);
  const fetch_ = useCallback(() =>
    apiFetch(`${BASE}/dags/${dagId}/stats?limit=50`)
      .then(r => r.json()).then(setStats).catch(() => {}),
    [dagId]);

  useEffect(() => { fetch_(); }, [fetch_]);

  if (!stats) return null;

  const terminal = stats.success_count + stats.failed_count + stats.cancelled_count;
  const running  = stats.total_runs - terminal;
  const rate     = stats.success_rate;

  // Bar segments: success=green, failed=red, cancelled=grey, running=blue
  const seg = (count, color, title) => {
    if (!count) return null;
    const pct = (count / stats.total_runs * 100).toFixed(1);
    return <div key={color} className="stats-bar-seg" title={`${title}: ${count}`}
      style={{width:`${pct}%`, background:color}} />;
  };

  return (
    <div className="stats-panel">
      <h4>📊 Last {stats.total_runs} runs</h4>

      {stats.total_runs > 0 && (
        <div className="stats-bar-wrap">
          {seg(stats.success_count,   '#276749', 'Success')}
          {seg(stats.failed_count,    '#9b1c1c', 'Failed')}
          {seg(stats.cancelled_count, '#4a5568', 'Cancelled')}
          {seg(running,               '#1a365d', 'Active')}
        </div>
      )}

      <div className="stats-grid">
        <div className="stats-kv">
          <span className="stats-label">Success rate</span>
          <span className="stats-value"
            style={{color: rate == null ? '#718096' : rate >= .9 ? '#68d391' : rate >= .5 ? '#f6ad55' : '#fc8181'}}>
            {rate == null ? '—' : `${(rate*100).toFixed(0)}%`}
          </span>
          <span className="stats-sub">{stats.success_count}✓ {stats.failed_count}✗ {stats.cancelled_count}⊘</span>
        </div>
        <div className="stats-kv">
          <span className="stats-label">Avg duration</span>
          <span className="stats-value">{fmtMs(stats.avg_duration_ms)}</span>
        </div>
        <div className="stats-kv">
          <span className="stats-label">p95 duration</span>
          <span className="stats-value">{fmtMs(stats.p95_duration_ms)}</span>
        </div>
        <div className="stats-kv">
          <span className="stats-label">Min / Max</span>
          <span className="stats-value" style={{fontSize:13}}>{fmtMs(stats.min_duration_ms)} / {fmtMs(stats.max_duration_ms)}</span>
        </div>
      </div>

      <DurationHistogram buckets={stats.histogram} />
    </div>
  );
}

// ── Backfill Panel ────────────────────────────────────────────────────
function BackfillPanel({ dagId, hasSchedule }) {
  const [open, setOpen] = useState(false);
  const [start, setStart] = useState('');
  const [end, setEnd] = useState('');
  const [result, setResult] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  if (!hasSchedule) return null;

  const submit = async () => {
    setError(''); setResult(null); setLoading(true);
    try {
      const res = await apiFetch(`${BASE}/dags/${dagId}/backfill`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ start, end }),
      });
      const body = await res.json();
      if (!res.ok) { setError(body.error ?? 'Request failed'); }
      else { setResult(body); }
    } catch (e) { setError(String(e)); }
    finally { setLoading(false); }
  };

  return (
    <div style={{marginBottom:12}}>
      <button className="btn btn-sm" onClick={() => { setOpen(v => !v); setResult(null); setError(''); }}
        style={{background:'#1a2744',color:'#90cdf4',border:'1px solid #2d4a8a'}}>
        ⏮ Backfill
      </button>
      {open && (
        <div style={{marginTop:8,padding:12,background:'#1a2130',borderRadius:6,border:'1px solid #2d3748',display:'flex',flexWrap:'wrap',gap:8,alignItems:'flex-end'}}>
          <label style={{fontSize:12,color:'#a0aec0'}}>Start
            <input type="date" value={start} onChange={e => setStart(e.target.value)}
              style={{display:'block',marginTop:2,background:'#2d3748',color:'#e2e8f0',border:'none',borderRadius:4,padding:'4px 8px',fontSize:12}} />
          </label>
          <label style={{fontSize:12,color:'#a0aec0'}}>End
            <input type="date" value={end} onChange={e => setEnd(e.target.value)}
              style={{display:'block',marginTop:2,background:'#2d3748',color:'#e2e8f0',border:'none',borderRadius:4,padding:'4px 8px',fontSize:12}} />
          </label>
          <button className="btn btn-sm" disabled={!start || !end || loading} onClick={submit}>
            {loading ? '…' : 'Run backfill'}
          </button>
          {error && <span style={{fontSize:12,color:'#fc8181'}}>{error}</span>}
          {result && (
            <span style={{fontSize:12,color:'#68d391'}}>
              ✓ {result.created_count} runs created, {result.skipped} skipped ({result.total_dates} dates)
            </span>
          )}
        </div>
      )}
    </div>
  );
}

// ── Variables Panel ───────────────────────────────────────────────────
function VariablesPanel({ onClose }) {
  const [vars, setVars] = useState([]);
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [isSecret, setIsSecret] = useState(false);
  const [desc, setDesc] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  const fetchVars = useCallback(() =>
    apiFetch(`${BASE}/variables`).then(r => r.json()).then(data => {
      if (Array.isArray(data)) setVars(data);
    }).catch(() => {}), []);

  useEffect(() => { fetchVars(); }, []);

  const save = async (e) => {
    e.preventDefault();
    if (!key.trim()) return;
    setError(''); setSaving(true);
    const res = await apiFetch(`${BASE}/variables`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: key.trim(), value, is_secret: isSecret, description: desc || undefined }),
    });
    setSaving(false);
    if (res.ok) {
      setKey(''); setValue(''); setIsSecret(false); setDesc('');
      fetchVars();
    } else {
      setError((await res.json()).error ?? 'Failed');
    }
  };

  const del = async (k) => {
    if (!confirm(`Delete variable "${k}"?`)) return;
    await apiFetch(`${BASE}/variables/${encodeURIComponent(k)}`, { method: 'DELETE' });
    fetchVars();
  };

  return (
    <div className="keys-panel">
      <h4>📦 Variables <span style={{flex:1}} /> <button className="log-btn" onClick={onClose}>✕</button></h4>
      <form onSubmit={save} style={{display:'grid',gap:6,marginBottom:16}}>
        <div style={{display:'flex',gap:6}}>
          <input value={key} onChange={e => setKey(e.target.value)} placeholder="key"
            style={{flex:1,background:'#0f1117',border:'1px solid #4a5568',borderRadius:4,padding:'5px 8px',color:'#e2e8f0',fontSize:12}} />
          <input value={value} onChange={e => setValue(e.target.value)} placeholder="value"
            type={isSecret ? 'password' : 'text'}
            style={{flex:2,background:'#0f1117',border:'1px solid #4a5568',borderRadius:4,padding:'5px 8px',color:'#e2e8f0',fontSize:12}} />
        </div>
        <div style={{display:'flex',gap:12,alignItems:'center',fontSize:12}}>
          <label style={{display:'flex',gap:4,alignItems:'center',color:'#a0aec0',cursor:'pointer'}}>
            <input type="checkbox" checked={isSecret} onChange={e => setIsSecret(e.target.checked)} />
            secret (masked in UI)
          </label>
          <input value={desc} onChange={e => setDesc(e.target.value)} placeholder="description (optional)"
            style={{flex:1,background:'#0f1117',border:'1px solid #4a5568',borderRadius:4,padding:'4px 8px',color:'#e2e8f0',fontSize:12}} />
          <button type="submit" className="btn btn-primary btn-sm" disabled={saving || !key.trim()}>{saving ? '…' : '+ Set'}</button>
        </div>
        {error && <div style={{fontSize:11,color:'#fc8181'}}>{error}</div>}
      </form>
      {vars.length === 0 && <div className="empty" style={{padding:'12px 0'}}>No variables yet</div>}
      {vars.map(v => (
        <div key={v.key} className="keys-row">
          <span className="key-name">{v.key}</span>
          {v.is_secret
            ? <span style={{fontSize:11,color:'#718096',fontStyle:'italic'}}>••••• (secret)</span>
            : <span style={{fontSize:12,color:'#e2e8f0',fontFamily:'monospace',maxWidth:200,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{v.value}</span>
          }
          {v.description && <span className="key-meta">{v.description}</span>}
          <button className="log-btn" style={{color:'#fc8181',borderColor:'#4a1515',marginLeft:'auto'}}
            onClick={() => del(v.key)}>Delete</button>
        </div>
      ))}
    </div>
  );
}

// ── Connections Panel ──────────────────────────────────────────────────
function ConnectionsPanel({ onClose }) {
  const [conns, setConns] = useState([]);
  const [form, setForm] = useState({ conn_id:'', conn_type:'postgres', host:'', port:'', login:'', password:'', schema:'', extra:'', description:'' });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [showForm, setShowForm] = useState(false);

  const fetchConns = useCallback(() =>
    apiFetch(`${BASE}/connections`).then(r => r.json()).then(data => {
      if (Array.isArray(data)) setConns(data);
    }).catch(() => {}), []);

  useEffect(() => { fetchConns(); }, []);

  const save = async (e) => {
    e.preventDefault();
    if (!form.conn_id.trim() || !form.conn_type.trim()) return;
    setError(''); setSaving(true);
    const body = {
      conn_id: form.conn_id.trim(),
      conn_type: form.conn_type.trim(),
      ...(form.host && { host: form.host }),
      ...(form.port && { port: parseInt(form.port, 10) }),
      ...(form.login && { login: form.login }),
      ...(form.password && { password: form.password }),
      ...(form.schema && { schema: form.schema }),
      ...(form.extra && { extra: form.extra }),
      ...(form.description && { description: form.description }),
    };
    const res = await apiFetch(`${BASE}/connections`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    setSaving(false);
    if (res.ok) {
      setForm({ conn_id:'', conn_type:'postgres', host:'', port:'', login:'', password:'', schema:'', extra:'', description:'' });
      setShowForm(false);
      fetchConns();
    } else {
      setError((await res.json()).error ?? 'Failed');
    }
  };

  const del = async (id) => {
    if (!confirm(`Delete connection "${id}"?`)) return;
    await apiFetch(`${BASE}/connections/${encodeURIComponent(id)}`, { method: 'DELETE' });
    fetchConns();
  };

  const inp = (field, placeholder, opts = {}) => (
    <input value={form[field]} onChange={e => setForm(f => ({...f, [field]: e.target.value}))}
      placeholder={placeholder} {...opts}
      style={{background:'#0f1117',border:'1px solid #4a5568',borderRadius:4,padding:'5px 8px',color:'#e2e8f0',fontSize:12,width:'100%'}} />
  );

  return (
    <div className="keys-panel">
      <h4>🔌 Connections <span style={{flex:1}} />
        <button className="log-btn" onClick={() => setShowForm(v => !v)} style={{marginRight:8}}>+ Add</button>
        <button className="log-btn" onClick={onClose}>✕</button>
      </h4>
      {showForm && (
        <form onSubmit={save} style={{display:'grid',gap:6,marginBottom:16,padding:10,background:'#1a2130',borderRadius:6,border:'1px solid #2d3748'}}>
          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:6}}>
            {inp('conn_id', 'conn_id *')}
            {inp('conn_type', 'type (postgres, http, s3…) *')}
            {inp('host', 'host')}
            {inp('port', 'port', {type:'number'})}
            {inp('login', 'login/username')}
            {inp('password', 'password', {type:'password'})}
            {inp('schema', 'schema/database')}
            {inp('extra', 'extra (JSON string)')}
          </div>
          {inp('description', 'description (optional)')}
          {error && <div style={{fontSize:11,color:'#fc8181'}}>{error}</div>}
          <button type="submit" className="btn btn-primary btn-sm" disabled={saving || !form.conn_id.trim()}>{saving ? '…' : '💾 Save connection'}</button>
        </form>
      )}
      {conns.length === 0 && !showForm && <div className="empty" style={{padding:'12px 0'}}>No connections yet</div>}
      {conns.map(c => (
        <div key={c.conn_id} className="keys-row">
          <span className="key-name">{c.conn_id}</span>
          <span className="key-meta">{c.conn_type}{c.host ? ` · ${c.host}${c.port ? ':'+c.port : ''}` : ''}</span>
          {c.has_password && <span style={{fontSize:10,color:'#718096'}}>🔒 pwd</span>}
          {c.description && <span className="key-meta">{c.description}</span>}
          <button className="log-btn" style={{color:'#fc8181',borderColor:'#4a1515',marginLeft:'auto'}}
            onClick={() => del(c.conn_id)}>Delete</button>
        </div>
      ))}
    </div>
  );
}

// ── Relative time ─────────────────────────────────────────────────────
function timeAgo(date) {
  const s = Math.round((Date.now() - new Date(date).getTime()) / 1000);
  if (s < 60) return `${Math.max(s, 0)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

/** `value`, updated only after it stopped changing for `ms` (typing in a filter box). */
function useDebounced(value, ms) {
  const [v, setV] = useState(value);
  useEffect(() => { const id = setTimeout(() => setV(value), ms); return () => clearTimeout(id); }, [value, ms]);
  return v;
}

// ── Dag filters (kept in the URL so a filtered view can be shared or reloaded) ──
const FILTER_KEYS = ['search', 'paused', 'last_run_state', 'tag'];
const LAST_RUN_OPTIONS = ['success', 'failed', 'running', 'queued', 'cancelled', 'none'];

function readFilters() {
  const q = new URLSearchParams(window.location.search);
  return Object.fromEntries(FILTER_KEYS.map(k => [k, q.get(k) ?? '']));
}

function writeFilters(filters) {
  const q = new URLSearchParams(window.location.search);
  FILTER_KEYS.forEach(k => { if (filters[k]) q.set(k, filters[k]); else q.delete(k); });
  const qs = q.toString();
  window.history.replaceState(null, '', `${window.location.pathname}${qs ? `?${qs}` : ''}`);
}

function dagsQuery(filters) {
  const q = new URLSearchParams();
  FILTER_KEYS.forEach(k => { if (filters[k]) q.set(k, k === 'search' ? filters[k].trim() : filters[k]); });
  const qs = q.toString();
  return qs ? `?${qs}` : '';
}

const FILTER_INPUT = {background:'#0f1117',border:'1px solid #2d3748',borderRadius:4,padding:'5px 8px',color:'#e2e8f0',fontSize:12};

function DagFilters({ filters, onChange }) {
  const set = (k, v) => onChange({ ...filters, [k]: v });
  const active = FILTER_KEYS.some(k => filters[k]);
  return (
    <div style={{display:'flex',gap:8,alignItems:'center',flexWrap:'wrap',marginBottom:12}}>
      <input type="search" value={filters.search} onChange={e => set('search', e.target.value)}
        placeholder="Search Dag id, owner or tag…" aria-label="Search Dags" style={{...FILTER_INPUT,minWidth:240}} />
      <select value={filters.paused} onChange={e => set('paused', e.target.value)} aria-label="Pause state" style={FILTER_INPUT}>
        <option value="">All Dags</option>
        <option value="false">Active</option>
        <option value="true">Paused</option>
      </select>
      <select value={filters.last_run_state} onChange={e => set('last_run_state', e.target.value)} aria-label="Last run state" style={FILTER_INPUT}>
        <option value="">Any last run</option>
        {LAST_RUN_OPTIONS.map(s => <option key={s} value={s}>{s === 'none' ? 'never run' : `last run ${s}`}</option>)}
      </select>
      {filters.tag && (
        <span className="tag-chip active" title="Remove tag filter" onClick={() => set('tag', '')}>#{filters.tag} ✕</span>
      )}
      {active && (
        <button className="refresh-btn" onClick={() => onChange(Object.fromEntries(FILTER_KEYS.map(k => [k, ''])))}>Clear filters</button>
      )}
    </div>
  );
}

// ── Dataset lineage ───────────────────────────────────────────────────
const LINEAGE_STYLE = {
  dag:     { fill: '#1a2744', stroke: '#3182ce', text: '#e2e8f0', icon: '◆', rx: 6 },
  dataset: { fill: '#1a2f1a', stroke: '#68d391', text: '#c6f6d5', icon: '▤', rx: 17 },
  alias:   { fill: '#2f1a2f', stroke: '#d6bcfa', text: '#e9d8fd', icon: '⇢', rx: 17, dash: '4 3' },
};
const RUN_STATE_COLOR = { success: '#68d391', failed: '#fc8181', running: '#63b3ed', queued: '#a0aec0', cancelled: '#718096' };

function LineageGraph({ lineage, lastRuns, selected, onSelect }) {
  const W = 180, H = 34, GAP_X = 80, GAP_Y = 14, PAD = 16;
  const pos = {};
  const perLayer = {};
  lineage.nodes.forEach(n => {
    const row = (perLayer[n.layer] = (perLayer[n.layer] ?? 0) + 1) - 1;
    pos[n.id] = { x: PAD + n.layer * (W + GAP_X), y: PAD + row * (H + GAP_Y) };
  });
  const layers = Math.max(0, ...lineage.nodes.map(n => n.layer)) + 1;
  const rows = Math.max(1, ...Object.values(perLayer));
  const width = PAD * 2 + layers * W + (layers - 1) * GAP_X;
  const height = PAD * 2 + rows * H + (rows - 1) * GAP_Y;
  const short = (s) => s.length > 24 ? `${s.slice(0, 23)}…` : s;

  return (
    <div style={{overflowX:'auto',background:'#0f1117',border:'1px solid #2d3748',borderRadius:6}}>
      <svg width={width} height={height} role="img" aria-label="Dataset lineage">
        <defs>
          <marker id="lineage-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
            <path d="M0,0 L10,5 L0,10 z" fill="#4a5568" />
          </marker>
        </defs>
        {lineage.edges.map(e => {
          const a = pos[e.from], b = pos[e.to];
          if (!a || !b) return null;
          const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x, y2 = b.y + H / 2, mid = (x1 + x2) / 2;
          const hot = selected && (e.from === selected || e.to === selected);
          return (
            <path key={`${e.from}>${e.to}`} d={`M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}`}
              fill="none" stroke={hot ? '#90cdf4' : '#4a5568'} strokeWidth={hot ? 2 : 1.2}
              strokeDasharray={e.runtime ? '5 4' : undefined} markerEnd="url(#lineage-arrow)">
              <title>{`${e.kind}${e.runtime ? ' (seen at runtime)' : ''}`}</title>
            </path>
          );
        })}
        {lineage.nodes.map(n => {
          const st = LINEAGE_STYLE[n.kind], p = pos[n.id];
          const run = n.kind === 'dag' ? lastRuns[n.name] : null;
          return (
            <g key={n.id} transform={`translate(${p.x},${p.y})`} style={{cursor:'pointer'}} onClick={() => onSelect(n)}>
              <title>{n.name}</title>
              <rect width={W} height={H} rx={st.rx} fill={st.fill} stroke={selected === n.id ? '#f6e05e' : st.stroke}
                strokeWidth={selected === n.id ? 2 : 1} strokeDasharray={st.dash} />
              <text x={12} y={H / 2 + 4} fill={st.text} fontSize="11" fontFamily="monospace">{st.icon} {short(n.name)}</text>
              {run && <circle cx={W - 12} cy={H / 2} r={4} fill={RUN_STATE_COLOR[run.state] ?? '#a0aec0'}><title>{`last run ${run.state}`}</title></circle>}
              {n.kind === 'dataset' && !n.last_event && <text x={W - 16} y={H / 2 + 4} fill="#718096" fontSize="10"><title>never produced</title>○</text>}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function LineagePanel({ dags, focusDagId, onFocus, onSelectDag, onClose }) {
  const [lineage, setLineage] = useState(null);
  const [error, setError] = useState('');
  const [selected, setSelected] = useState(null);

  const load = useCallback(() => {
    const url = `${BASE}/lineage${focusDagId ? `?dag_id=${encodeURIComponent(focusDagId)}` : ''}`;
    return apiFetch(url).then(async r => {
      const body = await r.json();
      if (!r.ok) { setError(body.error ?? 'Failed to load lineage'); return; }
      setError(''); setLineage(body);
    }).catch(() => setError('Failed to load lineage'));
  }, [focusDagId]);
  useInterval(load, 15000);

  const lastRuns = Object.fromEntries(dags.filter(d => d.last_run).map(d => [d.id, d.last_run]));
  const sel = lineage?.nodes.find(n => n.id === selected?.id) ?? null;
  const neighbours = (dir) => !sel ? [] : lineage.edges
    .filter(e => (dir === 'in' ? e.to : e.from) === sel.id)
    .map(e => lineage.nodes.find(n => n.id === (dir === 'in' ? e.from : e.to)))
    .filter(Boolean);

  return (
    <div className="keys-panel">
      <h4>
        🧬 Dataset lineage {focusDagId && <span className="tag-chip active" onClick={() => onFocus(null)} title="Show all Dags">{focusDagId} ✕</span>}
        <span style={{flex:1}} />
        <span style={{fontSize:10,color:'#718096',fontWeight:400}}>◆ Dag · ▤ dataset · ⇢ alias · dashed = seen at runtime</span>
        <button className="log-btn" onClick={load}>↻</button>
        <button className="log-btn" onClick={onClose}>✕</button>
      </h4>
      {error && <div className="error-msg">{error}</div>}
      {!lineage && !error && <div className="empty">Loading…</div>}
      {lineage && lineage.edges.length === 0 && <div className="empty">No Dag produces or consumes datasets{focusDagId ? ` connected to ${focusDagId}` : ''}.</div>}
      {lineage && lineage.edges.length > 0 && (
        <LineageGraph lineage={lineage} lastRuns={lastRuns} selected={sel?.id} onSelect={n => setSelected(prev => prev?.id === n.id ? null : n)} />
      )}
      {sel && (
        <div style={{marginTop:10,fontSize:12,color:'#a0aec0',display:'grid',gap:4}}>
          <div><strong style={{color:'#e2e8f0',fontFamily:'monospace'}}>{sel.name}</strong> <span style={{color:'#718096'}}>({sel.kind})</span></div>
          {sel.kind === 'dataset' && (
            <div>Last event: {sel.last_event
              ? <>{new Date(sel.last_event.emitted_at).toLocaleString()} by <code>{sel.last_event.produced_by}</code> ({timeAgo(sel.last_event.emitted_at)})</>
              : 'never produced'}</div>
          )}
          {neighbours('in').length > 0 && <div>← {neighbours('in').map(n => n.name).join(', ')}</div>}
          {neighbours('out').length > 0 && <div>→ {neighbours('out').map(n => n.name).join(', ')}</div>}
          {sel.kind === 'dag' && (
            <div style={{display:'flex',gap:6}}>
              <button className="log-btn" onClick={() => onSelectDag(sel.name)}>View runs</button>
              {focusDagId !== sel.name && <button className="log-btn" onClick={() => onFocus(sel.name)}>Focus lineage</button>}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Event log ─────────────────────────────────────────────────────────
function EventLogPanel({ dags, onOpenRun, onClose }) {
  const [types, setTypes] = useState([]);
  const [filters, setFilters] = useState({ dag_id: '', event_type: '', task_id: '' });
  const [items, setItems] = useState([]);
  const [nextCursor, setNextCursor] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    apiFetch(`${BASE}/event-logs/types`).then(r => r.json()).then(b => setTypes(b.items ?? [])).catch(() => {});
  }, []);

  const query = useDebounced(filters, 300);
  const fetchPage = useCallback(async (cursor) => {
    const q = new URLSearchParams({ limit: '50' });
    Object.entries(query).forEach(([k, v]) => { if (v.trim()) q.set(k, v.trim()); });
    if (cursor) q.set('cursor', cursor);
    setLoading(true);
    try {
      const r = await apiFetch(`${BASE}/event-logs?${q}`);
      const body = await r.json();
      if (!r.ok) { setError(body.error ?? 'Failed to load events'); return; }
      setError('');
      setItems(prev => cursor ? [...prev, ...body.items] : body.items);
      setNextCursor(body.next_cursor);
    } catch { setError('Failed to load events'); } finally { setLoading(false); }
  }, [query]);

  useEffect(() => { fetchPage(null); }, [fetchPage]);
  const set = (k, v) => setFilters(f => ({ ...f, [k]: v }));

  return (
    <div className="keys-panel">
      <h4>📜 Event log <span style={{flex:1}} />
        <button className="log-btn" onClick={() => fetchPage(null)} disabled={loading}>↻</button>
        <button className="log-btn" onClick={onClose}>✕</button>
      </h4>
      <div style={{display:'flex',gap:8,flexWrap:'wrap',marginBottom:10}}>
        <select value={filters.dag_id} onChange={e => set('dag_id', e.target.value)} aria-label="Dag" style={FILTER_INPUT}>
          <option value="">All Dags</option>
          {dags.map(d => <option key={d.id} value={d.id}>{d.id}</option>)}
        </select>
        <select value={filters.event_type} onChange={e => set('event_type', e.target.value)} aria-label="Event type" style={FILTER_INPUT}>
          <option value="">All events</option>
          {types.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
        <input value={filters.task_id} onChange={e => set('task_id', e.target.value)} placeholder="task id" aria-label="Task id" style={FILTER_INPUT} />
      </div>
      {error && <div className="error-msg">{error}</div>}
      {!error && items.length === 0 && <div className="empty">{loading ? 'Loading…' : 'No events'}</div>}
      {items.length > 0 && (
        <table className="runs-table">
          <thead><tr><th>When</th><th>Event</th><th>Dag</th><th>Run</th><th>Task</th><th>Details</th></tr></thead>
          <tbody>
            {items.map(e => {
              const details = Object.keys(e.metadata ?? {}).length ? JSON.stringify(e.metadata) : '';
              return (
                <tr key={e.id}>
                  <td style={{color:'#718096',whiteSpace:'nowrap'}} title={new Date(e.created_at).toLocaleString()}>{timeAgo(e.created_at)}</td>
                  <td><code style={{fontSize:11}}>{e.event_type}</code></td>
                  <td style={{fontFamily:'monospace',fontSize:11}}>{e.dag_id ?? '—'}</td>
                  <td>{e.dag_run_id && e.dag_id
                    ? <button className="log-btn" onClick={() => onOpenRun(e.dag_id, e.dag_run_id)}>{e.dag_run_id.slice(-8)} →</button>
                    : '—'}</td>
                  <td style={{fontFamily:'monospace',fontSize:11}}>{e.task_id ? `${e.task_id}${e.map_index != null && e.map_index >= 0 ? `[${e.map_index}]` : ''}` : '—'}</td>
                  <td style={{fontSize:11,color:'#a0aec0',maxWidth:320,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}} title={details}>{details}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      {nextCursor && (
        <div style={{textAlign:'center',marginTop:8}}>
          <button className="refresh-btn" onClick={() => fetchPage(nextCursor)} disabled={loading}>{loading ? '…' : 'Load more ↓'}</button>
        </div>
      )}
    </div>
  );
}

// ── App ───────────────────────────────────────────────────────────────
function App() {
  const [dags, setDags] = useState([]);
  const [selectedDag, setSelectedDag] = useState(null);
  const [selectedRun, setSelectedRun] = useState(null);
  const [showAlerts, setShowAlerts] = useState(false);
  const [showKeys, setShowKeys] = useState(false);
  const [showVars, setShowVars] = useState(false);
  const [showConns, setShowConns] = useState(false);
  const [showImages, setShowImages] = useState(false);
  const [showEvents, setShowEvents] = useState(false);
  const [lineageFocus, setLineageFocus] = useState(undefined);   // undefined = hidden, null = all Dags, id = one Dag
  const [filters, setFilters] = useState(readFilters);
  const query = useDebounced(filters, 300);
  // null = not checked yet, true = authenticated, false = needs login
  const [authed, setAuthed] = useState(null);
  const [session, setSession] = useState(null);

  const { alerts, refresh: refreshAlerts } = useSlaAlerts();
  const importErrors = useImportErrors();

  // Ask the server who we are; the CSRF token lives only in memory
  const loadSession = useCallback(() =>
    fetch(`${BASE}/auth/session`, { credentials: 'same-origin' }).then(r => r.json()).then(info => {
      csrfToken = info.csrf_token ?? null;
      setSession(info);
      setAuthed(Boolean(info.authenticated));
    }).catch(() => setAuthed(false)), []);

  useEffect(() => {
    localStorage.removeItem('airflow_api_key');   // older versions kept the API key here
    onSessionLost = () => { csrfToken = null; setAuthed(false); };
    loadSession();
  }, [loadSession]);

  const fetchDags = useCallback(() =>
    apiFetch(`${BASE}/dags${dagsQuery(query)}`).then(r=>r.json()).then(d=>setDags(Array.isArray(d)?d:(d.items??[]))).catch(()=>{}), [query]);
  useEffect(() => { writeFilters(query); }, [query]);

  useInterval(fetchDags, 5000);

  const handleLogin = () => { loadSession(); };
  const handleLogout = async () => {
    const res = await apiFetch(`${BASE}/auth/logout`, { method: 'POST' }).catch(() => null);
    const body = res?.ok ? await res.json().catch(() => ({})) : {};
    csrfToken = null;
    setAuthed(false);
    if (body.logout_url) window.location.href = body.logout_url;   // end the identity-provider session too
  };

  // Fetch DAGs immediately when auth state becomes true (after login or on startup)
  useEffect(() => { if (authed === true) fetchDags(); }, [authed]);
  const filtered = FILTER_KEYS.some(k => query[k]);

  const handleTrigger = () => { if (selectedDag) fetchDags(); };
  const handleTogglePause = () => fetchDags();
  const selectDag = (id) => { setSelectedDag(prev => prev === id ? null : id); setSelectedRun(null); };
  const openDag = (id) => { setSelectedDag(id); setSelectedRun(null); };
  const openRun = (dagId, runId) => { setSelectedDag(dagId); setSelectedRun(runId); };

  const ackAlert = async (id) => {
    await apiFetch(`${BASE}/sla-alerts/${id}/ack`, { method: 'POST' });
    refreshAlerts();
  };

  if (authed === null) return null;
  if (authed === false) return <LoginScreen methods={session?.methods} onLogin={handleLogin} />;

  return (
    <>
      <header>
        <h1>✈ Airflow Node</h1>
        <Health />
        <button className="refresh-btn" style={{marginLeft:'auto'}} onClick={() => setLineageFocus(f => f === undefined ? null : undefined)}
          title="Dataset lineage">🧬</button>
        <button className="refresh-btn" onClick={() => setShowEvents(v => !v)}
          title="Event log">📜</button>
        <button className="refresh-btn" onClick={() => setShowVars(v => !v)}
          title="Variables">📦</button>
        <button className="refresh-btn" onClick={() => setShowConns(v => !v)}
          title="Connections">🔌</button>
        <button className="refresh-btn" onClick={() => setShowKeys(v => !v)}
          title="API Keys">🔑</button>
        <button className="refresh-btn" onClick={() => setShowImages(v => !v)}
          title="Docker Images">🐳</button>
        {session?.auth_enabled && (
          <>
            {session.user && <span className="user-badge" title={`signed in via ${session.user.method === 'oidc' ? 'SSO' : 'API key'}`}>{session.user.name} · {session.role}</span>}
            <button className="refresh-btn" onClick={handleLogout} title="Sign out">⏻</button>
          </>
        )}
      </header>
      <ImportErrorBanner errors={importErrors} />
      <SlaBanner alerts={alerts} onViewAlerts={() => setShowAlerts(true)} />
      <div className="container">
        {showVars && <VariablesPanel onClose={() => setShowVars(false)} />}
        {showConns && <ConnectionsPanel onClose={() => setShowConns(false)} />}
        {showKeys && <ApiKeysPanel onClose={() => setShowKeys(false)} />}
        {showImages && <ImagesPanel onClose={() => setShowImages(false)} />}
        {showAlerts && (
          <SlaPanel alerts={alerts} onAck={ackAlert} onClose={() => setShowAlerts(false)} />
        )}
        {lineageFocus !== undefined && (
          <LineagePanel dags={dags} focusDagId={lineageFocus} onFocus={setLineageFocus}
            onSelectDag={openDag} onClose={() => setLineageFocus(undefined)} />
        )}
        {showEvents && <EventLogPanel dags={dags} onOpenRun={openRun} onClose={() => setShowEvents(false)} />}
        <div className="section-title">Dags ({dags.length}{filtered ? ' matching' : ''})</div>
        <DagFilters filters={filters} onChange={setFilters} />
        <div className="dag-grid">
          {dags.map(d => (
            <DagCard key={d.id} dag={d}
              selected={selectedDag === d.id}
              onTrigger={handleTrigger}
              onTogglePause={handleTogglePause}
              onTagClick={tag => setFilters(f => ({ ...f, tag }))}
              onLineage={setLineageFocus}
              onSelect={selectDag} />
          ))}
          {!dags.length && <div className="empty">{filtered ? 'No Dags match these filters' : 'No Dags loaded'}</div>}
        </div>

        {selectedDag && (
          <>
            <StatsPanel dagId={selectedDag} />
            <BackfillPanel dagId={selectedDag}
              hasSchedule={!!(dags.find(d => d.id === selectedDag)?.schedule)} />
            <div className="split">
              <div>
                <div className="section-title" style={{marginBottom:8}}>
                  Runs — {selectedDag}
                </div>
                <RunsTable dagId={selectedDag} onSelectRun={setSelectedRun} />
                <details style={{marginTop:12}}>
                  <summary style={{fontSize:11,color:'#a0aec0',cursor:'pointer',userSelect:'none',padding:'4px 0'}}>
                    📅 Calendar View
                  </summary>
                  <div style={{marginTop:8}}>
                    <CalendarView dagId={selectedDag} />
                  </div>
                </details>
              </div>
              {selectedRun && (
                <RunDetail runId={selectedRun} onClose={() => setSelectedRun(null)} />
              )}
            </div>
          </>
        )}
      </div>
    </>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
