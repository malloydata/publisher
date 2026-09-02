/* Eval run — every case on one row, everything behind it one click down.
 *
 * Rows are cases; each arm of the run gets a verdict pill and a row of dots
 * for the entities the golden answer depends on (did get_context deliver them).
 * Opening a row shows the reference answer as a table, then per arm: the
 * judge's reasoning, an effort line (turns, calls, errors, seconds, dollars),
 * and the attempt as a TIMELINE -- every get_context and execute_query with
 * its input and what came back, the prose between them, the final answer.
 *
 * The timeline is the point. A verdict of no_match cannot tell you whether the
 * query was wrong or a right query was read wrongly; the sequence can, and
 * every earlier viewer flattened it into one blob of prose.
 *
 * Aggregate tables live in eval_run.malloynb, which Publisher renders. This
 * file holds no scoring logic; everything comes from eval_run.malloy.
 */

const MODEL = 'eval_run.malloy';

const MODES = [
  ['all',       'All',           () => true],
  ['failures',  'Failures',      r => r.arms.some(a => a.verdict === 'no_match')],
  ['undecided', 'Partly / human',r => r.arms.some(a => undecided(a.verdict))],
  ['different', 'Disagreements', r => r.arms.length > 1 &&
      new Set(r.arms.filter(a => !undecided(a.verdict))
                    .map(a => a.verdict === 'match')).size > 1],
  ['refusal',   'Refusal',       r => r.coverage === 'absent' ||
      (r.tags || '').includes('answerable-sounds-unanswerable')],
  ['retrieval', 'Retrieval gaps',r => r.arms.some(a =>
      (a.required || []).some(e => e.status === 'missing'))],
];

const undecided = v => v === 'near_match' || v === 'needs_human';
const esc = s => String(s ?? '').replace(/[&<>"]/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const pct = x => x == null ? '—' : (100 * x).toFixed(0) + '%';
const num = (x, d = 0) => x == null ? '—' : Number(x).toLocaleString(undefined,
  { maximumFractionDigits: d });
const verdictLabel = { match: 'correct', near_match: 'partly', no_match: 'wrong',
                       needs_human: 'needs human' };

/* Publisher.query(modelPath, malloy) takes POSITIONAL arguments and returns
 * plain row objects, nesting included. */
const query = malloy => Publisher.query(MODEL, malloy);

const state = { mode: 'all', q: '', wtf: '', rows: [], arms: [], required: {} };

function readUrl() {
  const p = new URLSearchParams(location.hash.slice(1));
  state.mode = p.get('mode') || 'all';
  state.q = p.get('q') || '';
  state.wtf = p.get('wtf') || '';
  state.open = p.get('case') || null;
}
function writeUrl() {
  const p = new URLSearchParams();
  if (state.mode !== 'all') p.set('mode', state.mode);
  if (state.q) p.set('q', state.q);
  if (state.wtf) p.set('wtf', state.wtf);
  if (state.open) p.set('case', state.open);
  const h = p.toString();
  history.replaceState(null, '', h ? '#' + h : location.pathname);
}

/* ---------------------------------------------------------------- render bits */

function pill(v) {
  if (!v) return '<span class="pill none">—</span>';
  return `<span class="pill ${esc(v)}">${esc(verdictLabel[v] || v.replace('_', ' '))}</span>`;
}

function dots(required) {
  if (!required || !required.length) return '';
  return '<span class="dots">' + required.map(e =>
    `<i class="dot ${esc(e.status)}" title="${esc(e.entity_id)} — ${esc(e.status)}"></i>`)
    .join('') + '</span>';
}

/* A table from an array of flat objects. Rows past `max` are summarised. */
function rowsTable(rows, max = 12) {
  if (!Array.isArray(rows) || !rows.length) return '';
  const cols = [...new Set(rows.flatMap(r => Object.keys(r || {})))];
  const isNum = c => rows.every(r => r[c] == null || typeof r[c] === 'number');
  const fmt = v => v == null ? '<span class="mute">null</span>'
    : typeof v === 'number' ? num(v, Number.isInteger(v) ? 0 : 3)
    : typeof v === 'object' ? esc(JSON.stringify(v)) : esc(v);
  let h = '<div class="tbl"><table><thead><tr>' +
    cols.map(c => `<th${isNum(c) ? ' class="num"' : ''}>${esc(c)}</th>`).join('') +
    '</tr></thead><tbody>';
  for (const r of rows.slice(0, max))
    h += '<tr>' + cols.map(c =>
      `<td${isNum(c) ? ' class="num"' : ''}>${fmt(r[c])}</td>`).join('') + '</tr>';
  h += '</tbody></table>';
  if (rows.length > max) h += `<div class="more">+ ${rows.length - max} more rows</div>`;
  return h + '</div>';
}

/* The golden, whatever shape it was stored in. */
function goldenBlock(d) {
  if (d.golden_kind === 'unanswerable' || !d.golden_value)
    return `<div class="box"><b>Reference.</b> ${esc(d.golden || 'The model cannot answer this; declining is the pass.')}</div>`;
  let v; try { v = JSON.parse(d.golden_value); } catch { v = null; }
  if (Array.isArray(v)) return rowsTable(v);
  if (v && typeof v === 'object')
    return rowsTable([v]);
  return `<div class="box">${esc(d.golden)}</div>`;
}

/* The re-executed rows: JSON, a pipe-separated text table, or a note. */
function predictionBlock(p) {
  if (!p) return '';
  const t = p.trim();
  if (t.startsWith('[') || t.startsWith('{')) {
    try { const v = JSON.parse(t); return rowsTable(Array.isArray(v) ? v : [v]); }
    catch { /* fall through */ }
  }
  const lines = t.split('\n').filter(l => l.includes('|'));
  if (lines.length >= 2 && /^[\s|:-]+$/.test(lines[1])) {
    const cells = l => l.split('|').map(s => s.trim());
    const cols = cells(lines[0]);
    const rows = lines.slice(2).map(l => Object.fromEntries(
      cells(l).map((c, i) => [cols[i] || `c${i}`, isFinite(c) && c !== '' ? Number(c) : c])));
    return rowsTable(rows);
  }
  return `<div class="box mute">${esc(t)}</div>`;
}

/* Enough markdown for an agent's answer: fences, tables, bold, code, headings, bullets. */
function md(text) {
  if (!text) return '';
  const out = [];
  const lines = String(text).split('\n');
  let i = 0, para = [];
  const flush = () => { if (para.length) { out.push(`<p>${inline(para.join(' '))}</p>`); para = []; } };
  const inline = s => esc(s)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<b>$1</b>');
  while (i < lines.length) {
    const l = lines[i];
    if (/^```/.test(l)) {
      flush(); const buf = []; i++;
      while (i < lines.length && !/^```/.test(lines[i])) buf.push(lines[i++]);
      out.push(`<pre>${esc(buf.join('\n'))}</pre>`); i++; continue;
    }
    if (/^\s*\|/.test(l) && i + 1 < lines.length && /^\s*\|?[\s|:-]+\|?\s*$/.test(lines[i + 1])) {
      flush();
      const cells = s => s.replace(/^\s*\||\|\s*$/g, '').split('|').map(x => x.trim());
      const cols = cells(l); i += 2; const rows = [];
      while (i < lines.length && /^\s*\|/.test(lines[i])) {
        const c = cells(lines[i++]);
        rows.push(Object.fromEntries(cols.map((k, j) => [k, c[j] ?? ''])));
      }
      out.push(rowsTable(rows, 30)); continue;
    }
    if (/^#{1,6}\s/.test(l)) { flush(); out.push(`<h4>${inline(l.replace(/^#+\s*/, ''))}</h4>`); i++; continue; }
    if (/^\s*[-*]\s+/.test(l)) {
      flush(); const items = [];
      while (i < lines.length && /^\s*[-*]\s+/.test(lines[i]))
        items.push(`<li>${inline(lines[i++].replace(/^\s*[-*]\s+/, ''))}</li>`);
      out.push(`<ul>${items.join('')}</ul>`); continue;
    }
    if (!l.trim()) { flush(); i++; continue; }
    para.push(l); i++;
  }
  flush();
  return `<div class="md">${out.join('')}</div>`;
}

function stepHtml(s) {
  const k = s.kind, err = s.is_error === 'true' || s.is_error === true;
  if (k === 'text') return `<div class="step text"><div class="prose">${md(s.label)}</div></div>`;
  if (k === 'skill') return `<div class="step"><span class="k">skill</span><span class="mono">${esc(s.label)}</span></div>`;
  if (k === 'get_context') {
    const r = err ? `<span class="r" style="color:var(--fail)">${esc(s.detail)}</span>`
      : `<span class="r">→ ${s.n_results ?? '?'} entities</span>`;
    return `<div class="step get_context${err ? ' error' : ''}"><span class="k get_context">get_context</span><span class="mono">${esc(s.label)}</span>${r}</div>`;
  }
  if (k === 'execute_query') {
    let r;
    if (err) r = `<div class="r" style="color:var(--fail);margin:4px 0 0">${esc(s.detail)}</div>`;
    else r = `<span class="r">→ ${s.n_results ?? '?'} rows</span>`;
    return `<div class="step execute_query${err ? ' error' : ''}"><span class="k${err ? ' error' : ''}">execute_query${err ? ' · error' : ''}</span>${r}<pre>${esc(s.label)}</pre></div>`;
  }
  return `<div class="step${err ? ' error' : ''}"><span class="k${err ? ' error' : ''}">${esc(k)}</span><span class="mono">${esc(s.label).slice(0, 200)}</span>${err ? `<span class="r" style="color:var(--fail)">${esc(s.detail)}</span>` : ''}</div>`;
}

/* ---------------------------------------------------------------- list */

function visible() {
  const mode = MODES.find(m => m[0] === state.mode) || MODES[0];
  const q = state.q.toLowerCase();
  return state.rows.filter(r =>
    mode[2](r) &&
    (!state.wtf || r.arms.some(a => a.where_to_fix === state.wtf)) &&
    (!q || (r.question + ' ' + r.qid + ' ' + (r.tags || '')).toLowerCase().includes(q)));
}

function renderList() {
  const rows = visible();
  document.getElementById('count').textContent = `${rows.length} of ${state.rows.length} cases`;
  const el = document.getElementById('cases');
  if (!rows.length) { el.innerHTML = '<div class="empty">No cases match.</div>'; return; }
  el.style.setProperty('--armcols', state.arms.map(() => 'minmax(120px,auto)').join(' '));
  el.innerHTML = rows.map(r => {
    const cells = state.arms.map(arm => {
      const a = r.arms.find(x => x.arm === arm) || {};
      return `<span class="armcell" title="${esc(arm)}">${dots(a.required)}${pill(a.verdict)}</span>`;
    }).join('');
    return `<details class="case" data-case="${esc(r.qid)}"${state.open === r.qid ? ' open' : ''}>
      <summary>
        <span><div class="q">${esc(r.question)}</div>
          <div class="qid">${esc(r.qid)} · ${esc(r.coverage)}${r.split ? ' · ' + esc(r.split) : ''}${
            r.tags ? ' · ' + esc(r.tags) : ''}</div></span>
        <span class="kind">${esc(r.golden_kind || '')}</span>
        ${cells}
      </summary>
      <div class="body" data-body="${esc(r.qid)}"><div class="empty">Loading…</div></div>
    </details>`;
  }).join('');
  el.querySelectorAll('details.case').forEach(d => {
    d.addEventListener('toggle', () => {
      if (d.open) { state.open = d.dataset.case; writeUrl(); loadCase(d.dataset.case); }
      else if (state.open === d.dataset.case) { state.open = null; writeUrl(); }
    });
    if (d.open) loadCase(d.dataset.case);
  });
}

const caseCache = {};
async function loadCase(qid) {
  const body = document.querySelector(`[data-body="${CSS.escape(qid)}"]`);
  if (!body) return;
  if (caseCache[qid]) { body.innerHTML = caseCache[qid]; return; }
  const lit = qid.replace(/'/g, "\\'");
  let detail, steps;
  try {
    [detail, steps] = await Promise.all([
      query(`run: case_drawer + { where: qid = '${lit}' }`),
      query(`run: case_steps + { where: qid = '${lit}' }`),
    ]);
  } catch (e) {
    body.innerHTML = `<div class="err">${esc(e.message || e)}</div>`; return;
  }
  if (!detail.length) { body.innerHTML = '<div class="empty">No attempt recorded.</div>'; return; }
  const d0 = detail[0];
  let html = `<div><h3>Reference answer</h3>${goldenBlock(d0)}`;
  if (d0.rubric || d0.must_state)
    html += `<details class="sub" style="margin-top:8px"><summary>Rubric</summary>
      <div class="box" style="margin-top:6px">${d0.rubric ? esc(d0.rubric) : ''}${
        d0.must_state ? `<br><br><b>Must state.</b> ${esc(d0.must_state)}` : ''}</div></details>`;
  html += '</div>';

  html += `<div class="${detail.length === 2 ? 'two' : ''}">`;
  for (const d of detail) {
    const mine = steps.filter(s => s.arm === d.arm);
    const req = ((state.required[qid] || {})[d.arm]) || [];
    html += `<div class="armblock">
      <div class="armhead"><span class="name">${esc(d.arm)}</span>${pill(d.verdict)}
        ${d.confidence != null ? `<span class="chip">confidence ${d.confidence}/10</span>` : ''}
        ${d.gold_status && d.gold_status !== 'verified' ? `<span class="chip warn">golden ${esc(d.gold_status)}</span>` : ''}
        ${d.contaminated === 'true' ? '<span class="chip fail">contaminated</span>' : ''}
        ${dots(req)}</div>
      <div class="meta">
        <span><b>${num(d.num_turns)}</b> turns</span>
        <span><b>${num(d.n_get_context)}</b> get_context</span>
        <span><b>${num(d.n_execute)}</b> execute_query${d.n_execute_errors ? ` <span style="color:var(--fail)">(${d.n_execute_errors} errored)</span>` : ''}</span>
        <span><b>${num(d.wall_seconds)}</b> s</span>
        <span><b>$${num(d.cost_usd, 2)}</b></span>
      </div>
      <div class="judge"><b>Judge.</b> ${esc(d.judge_reasoning)}${
        d.where_to_fix ? ` <span class="mute">· where to fix: ${esc(d.where_to_fix)}</span>` : ''}</div>
      ${d.prediction ? `<h3>Re-executed rows</h3>${predictionBlock(d.prediction)}` : ''}
      <h3>The attempt, step by step</h3>
      <div class="timeline">${mine.length ? mine.map(stepHtml).join('')
        : `<div class="step text"><div class="prose">${md(d.answer)}</div></div>${
            d.final_query ? `<div class="step execute_query"><span class="k">final query</span><pre>${esc(d.final_query)}</pre></div>` : ''}`}</div>
      ${d.transcript ? `<div class="qid" style="margin-top:10px">transcript: ${esc(d.transcript)}</div>` : ''}
    </div>`;
  }
  html += '</div>';
  caseCache[qid] = html;
  body.innerHTML = html;
}

/* ---------------------------------------------------------------- header */

function renderArms(summary, effort, retrieval) {
  document.getElementById('arms').innerHTML = summary.map(s => {
    const e = effort.find(x => x.arm === s.arm) || {};
    const r = retrieval.find(x => x.arm === s.arm) || {};
    const attempts = e.attempt_count ?? s.confident_count;
    return `<div class="arm">
      <div class="name">${esc(s.arm)}</div>
      <div class="big">
        <div><div class="n">${s.passed}<small>/ ${attempts}</small></div><div class="l">correct</div></div>
        <div><div class="n acc">${r.required_count ? pct(r.delivered_rate) : '—'}</div>
          <div class="l">retrieval recall · ${r.delivered_count ?? '—'} of ${r.required_count ?? '—'} needed entities delivered${
            r.ranked_count != null ? `, ${r.ranked_count} as ranked` : ''}</div></div>
      </div>
      <div class="chips">
        ${s.near_matches ? `<span class="chip warn">${s.near_matches} partly</span>` : ''}
        ${s.failed ? `<span class="chip fail">${s.failed} wrong</span>` : ''}
        ${s.needs_human ? `<span class="chip warn">${s.needs_human} needs human</span>` : ''}
        ${e.contaminated_count ? `<span class="chip fail">${e.contaminated_count} contaminated</span>` : ''}
        ${e.execute_errors ? `<span class="chip">${e.execute_errors} query errors</span>` : ''}
      </div>
      <div class="effort mono">${num(e.avg_turns, 1)} turns · ${num(e.avg_get_context_per_attempt, 1)} get_context · ${
        num(e.avg_execute_per_attempt, 1)} execute_query · ${num(e.avg_wall_seconds)} s · $${num(e.avg_cost_usd, 2)} per question · $${num(e.total_cost_usd, 2)} total</div>
    </div>`;
  }).join('');
}

async function main() {
  readUrl();
  document.getElementById('modes').innerHTML = MODES.map(
    ([id, label]) => `<button data-mode="${id}" aria-pressed="${state.mode === id}">${label}</button>`).join('');
  document.getElementById('q').value = state.q;

  let summary, effort, retrieval, matrix, required;
  try {
    [summary, effort, retrieval, matrix, required] = await Promise.all([
      query('run: run_summary'), query('run: run_effort'), query('run: run_retrieval'),
      query('run: case_matrix'), query('run: case_required'),
    ]);
  } catch (e) {
    document.getElementById('cases').innerHTML =
      `<div class="err"><b>Could not load the run.</b><br>${esc(e.message || e)}
       <br><br>Serve this package from Publisher; opening index.html from disk has no
       <code>/sdk/publisher.js</code> and no model to query.</div>`;
    return;
  }

  for (const r of required)
    ((state.required[r.qid] ??= {})[r.arm] ??= []).push(r);
  state.arms = summary.map(s => s.arm);
  state.rows = matrix.map(r => ({
    ...r, arms: (r.by_arm || []).map(a => ({ ...a, required: (state.required[r.qid] || {})[a.arm] || [] })),
  }));

  document.getElementById('eyebrow').textContent =
    `${state.arms.length === 1 ? 'eval run' : state.arms.length + ' arms'} · ${state.rows.length} cases`;
  document.getElementById('title').textContent = state.arms.length === 1 ? state.arms[0] : 'Eval runs';
  document.getElementById('runline').innerHTML =
    esc(summary.map(s => `${s.arm}: ${s.passed}/${s.confident_count} decided`).join('  ·  ')) +
    '  ·  <a href="../eval_run.malloynb">analytical tables →</a>';

  const fixes = [...new Set(state.rows.flatMap(r => r.arms.map(a => a.where_to_fix).filter(Boolean)))].sort();
  document.getElementById('wtf').innerHTML = '<option value="">Any where-to-fix</option>' +
    fixes.map(f => `<option${f === state.wtf ? ' selected' : ''}>${esc(f)}</option>`).join('');

  renderArms(summary, effort, retrieval);
  renderList();

  document.getElementById('modes').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    state.mode = b.dataset.mode; writeUrl();
    document.querySelectorAll('#modes button').forEach(x => x.setAttribute('aria-pressed', x.dataset.mode === state.mode));
    renderList();
  });
  document.getElementById('q').addEventListener('input', e => { state.q = e.target.value; writeUrl(); renderList(); });
  document.getElementById('wtf').addEventListener('change', e => { state.wtf = e.target.value; writeUrl(); renderList(); });
}

main();
