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
 * file holds no scoring logic; everything comes from eval_run.malloy. That
 * includes classifying a verdict: the `outcome` column is decided once, in
 * flip_table.outcome, and travels through the CSV. This file used to keep its
 * own `undecided()` and the Malloy kept a third copy, which is how the package
 * and the acceptance check came to disagree about what a flip was.
 */

const MODEL = 'eval_run.malloy';

const MODES = [
  ['all',       'All',           () => true],
  ['failures',  'Failures',      r => r.arms.some(a => a.outcome === 'fail')],
  // "Undecided" is the honest name: near_match means the judge found the
  // answer defensibly different, needs_human means it would not commit. Neither
  // is a pass or a fail, and neither moves an acceptance check.
  ['undecided', 'Undecided',     r => r.arms.some(a => a.outcome === 'neither')],
  ['different', 'Disagreements', r => r.arms.length > 1 &&
      new Set(r.arms.filter(a => a.outcome !== 'neither')
                    .map(a => a.outcome === 'pass')).size > 1],
  ['refusal',   'Refusal',       r => r.coverage === 'absent' ||
      (r.tags || '').includes('answerable-sounds-unanswerable')],
  ['retrieval', 'Retrieval gaps',r => r.arms.some(a =>
      (a.required || []).some(e => e.status === 'missing'))],
];

const esc = s => String(s ?? '').replace(/[&<>"]/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
/* Escape a value for a single-quoted Malloy string literal. Backslashes FIRST,
 * then quotes: the other order re-escapes the backslash the quote escape just
 * introduced, so a qid ending in a backslash closes the literal early and the
 * rest of it parses as Malloy. This is the form the malloy-html-data-app-runtime
 * skill prescribes for in-package apps. */
const lit = s => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
const pct = x => x == null ? '—' : (100 * x).toFixed(0) + '%';
const num = (x, d = 0) => x == null ? '—' : Number(x).toLocaleString(undefined,
  { maximumFractionDigits: d });
const usd = x => x == null ? '—' : '$' + Number(x).toFixed(2);
const verdictLabel = { match: 'correct', near_match: 'partly', no_match: 'wrong',
                       needs_human: 'needs human' };
/* Two per-case labels nothing on screen explained. Both come from the SET, not
   from the run: they describe the question, not how it went. */
const COVERAGE_HELP = {
  covered: 'covered: the model can answer this directly.',
  derivable: 'derivable: no single field answers it, but the model has the parts.',
  absent: 'absent: the model cannot answer this. Declining IS the correct answer.',
};
const SPLIT_HELP = {
  dev: 'dev: diagnose and improve may look at this case.',
  holdout: 'holdout: kept back from diagnose and improve, so a gain measured here is evidence rather than memorisation.',
};

/* Publisher.query(modelPath, malloy) takes POSITIONAL arguments and returns
 * plain row objects, nesting included. */
const query = malloy => Publisher.query(MODEL, malloy);

const state = { mode: 'all', q: '', wtf: '', group: null, measure: 'turns',
                rows: [], arms: [], required: {}, backlog: [], effort: [] };
const MEASURES = [['turns', 'turns', 'num_turns', 0], ['cost', 'dollars', 'cost_usd', 2],
                  ['seconds', 'seconds', 'wall_seconds', 0]];

function readUrl() {
  const p = new URLSearchParams(location.hash.slice(1));
  state.mode = p.get('mode') || 'all';
  state.q = p.get('q') || '';
  state.wtf = p.get('wtf') || '';
  state.open = p.get('case') || null;
  // The hash is user-editable: an unknown mode or measure falls back rather
  // than driving the page, and a group index is range-checked once the backlog loads.
  if (!MODES.some(m => m[0] === state.mode)) state.mode = 'all';
  state.measure = MEASURES.some(m => m[0] === p.get('m')) ? p.get('m') : 'turns';
  const g = Number.parseInt(p.get('g'), 10);
  state.group = Number.isInteger(g) && g >= 0 ? g : null;
}
function writeUrl() {
  const p = new URLSearchParams();
  if (state.mode !== 'all') p.set('mode', state.mode);
  if (state.q) p.set('q', state.q);
  if (state.wtf) p.set('wtf', state.wtf);
  if (state.group != null) p.set('g', String(state.group));
  if (state.measure !== 'turns') p.set('m', state.measure);
  if (state.open) p.set('case', state.open);
  const h = p.toString();
  history.replaceState(null, '', h ? '#' + h : location.pathname);
}

/* ---------------------------------------------------------------- render bits */

function pill(v) {
  if (!v) return '<span class="pill none">—</span>';
  return `<span class="pill ${esc(v)}">${esc(verdictLabel[v] || v.replace('_', ' '))}</span>`;
}

/* The entities this answer needed, one dot each, and the count in front.
   The dots alone made a reader count them to learn the one thing that
   matters -- how many of the needed entities came back -- and the four
   shades read as four verdicts rather than as one binary with three ways
   of arriving at it. The fraction says it outright; the shades stay for
   HOW each one was delivered, which the legend explains. */
function dots(required) {
  if (!required || !required.length) return '';
  const got = required.filter(e => e.status !== 'missing').length;
  const all = got === required.length;
  return `<span class="dots"><span class="recall${all ? '' : ' short'}"`
    + ` title="${got} of ${required.length} needed entities came back">`
    + `${got}/${required.length}</span>` + required.map(e =>
    `<i class="dot ${esc(e.status)}" title="${esc(e.entity_id)} — ${esc(e.status)}"></i>`)
    .join('') + '</span>';
}

/* A table from an array of flat objects. Rows past `max` are summarised. */
function rowsTable(rows, max = 12) {
  if (!Array.isArray(rows) || !rows.length) return '';
  const cols = [...new Set(rows.flatMap(r => Object.keys(r || {})))];
  const isNum = c => rows.every(r => r[c] == null || typeof r[c] === 'number');
  // A timestamp at midnight is a date; printing the time made every result
  // with a month or day column twice as wide as it needed to be.
  const day = v => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T00:00:00(\.0+)?Z$/.test(v) ? v.slice(0, 10) : v;
  const fmt = v => (v = day(v)) == null ? '<span class="mute">null</span>'
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
  // Keep the separator line too: Malloy's text tables draw it with `+` at
  // column breaks and no `|`, so filtering on `|` alone dropped it. The
  // header must still be the first line, so a table with a `+---+` top
  // border falls through to raw; format_rows() never draws one.
  const lines = t.split('\n').filter(l => l.includes('|') || /^[\s:+-]{3,}$/.test(l));
  if (lines.length >= 2 && lines[0].includes('|') && /^[\s|:+-]+$/.test(lines[1])) {
    const cells = l => l.split('|').map(s => s.trim());
    const cols = cells(lines[0]);
    const rows = lines.slice(2).map(l => Object.fromEntries(
      cells(l).map((c, i) => [cols[i] || `c${i}`, isFinite(c) && c !== '' ? Number(c) : c])));
    return rowsTable(rows);
  }
  // Malloy renders a result with no grouping as `name ------ value`, one line
  // per field and not a pipe in sight, so it used to miss both branches above
  // and land in the raw fallback: a lone number shown as
  // `check_sum --------- 213939.33999999976`, full width, dashes and all.
  // Through rowsTable it is a one-row table with the column named and the
  // number formatted, which is what every other result here looks like.
  const scalar = t.split('\n').map(l => l.match(/^\s*([^\s|]+)\s+-{3,}\s+(.*\S)\s*$/))
    .filter(Boolean);
  if (scalar.length && scalar.length === t.split('\n').filter(l => l.trim()).length)
    return rowsTable([Object.fromEntries(scalar.map(([, k, v]) =>
      [k, isFinite(v) && v !== '' ? Number(v) : v]))]);
  // A one-column result prints as a header, a rule of dashes, then values.
  const all = t.split('\n').map(l => l.trim()).filter(Boolean);
  if (all.length >= 3 && /^\S+$/.test(all[0]) && /^-{3,}$/.test(all[1]))
    return rowsTable(all.slice(2).map(v => ({ [all[0]]: isFinite(v) ? Number(v) : v })));
  return `<div class="box mute rawtext">${esc(t)}</div>`;
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

// A flag that survives the CSV round trip either way it is typed. The build
// writes booleans as lowercase `true`/`false`, DuckDB sniffs a populated column
// as BOOLEAN and a header-only one as VARCHAR, so the same field arrives as a
// JSON boolean on one run and as a string on another. `=== 'true'` alone
// silently stopped rendering the contamination chip the moment the column
// sniffed boolean, which is the one flag that must never fail quietly.
function truthy(v) {
  return v === true || v === 'true';
}

/* ---------------------------------------------------------------- list */

function visible() {
  const mode = MODES.find(m => m[0] === state.mode) || MODES[0];
  const q = state.q.toLowerCase();
  return state.rows.filter(r =>
    mode[2](r) &&
    (!state.wtf || r.arms.some(a => a.where_to_fix === state.wtf)) &&
    (state.group == null || groupQids().has(r.qid)) &&
    (!q || (r.question + ' ' + r.qid + ' ' + (r.tags || '')).toLowerCase().includes(q)));
}

function renderList() {
  const rows = visible();
  document.getElementById('count').textContent = `${rows.length} of ${state.rows.length} cases`;
  renderChips();
  const el = document.getElementById('cases');
  if (!rows.length) { el.innerHTML = '<div class="empty">No cases match.</div>'; return; }
  el.style.setProperty('--armcols', state.arms.map(() => 'minmax(120px,auto)').join(' '));
  el.innerHTML = rows.map(r => {
    const cells = state.arms.map(arm => {
      const a = r.arms.find(x => x.arm === arm) || {};
      return `<span class="armcell" title="${esc(arm)}">${dots(a.required)}${pill(a.verdict)}</span>`;
    }).join('');
    return `<button type="button" class="case${state.open === r.qid ? ' on' : ''}" data-case="${esc(r.qid)}"
        aria-haspopup="dialog" title="Open this case">
        <span><span class="q">${esc(r.question)}</span>
          <span class="qid">${esc(r.qid)}${r.coverage ? ` · <span title="${esc(COVERAGE_HELP[r.coverage] || 'How far the model can answer this question.')}">${esc(r.coverage)}</span>` : ''}${
            r.split ? ' · <span title="' + esc(SPLIT_HELP[r.split] || '') + '">' + esc(r.split) + '</span>' : ''}${
            r.tags ? ' · ' + esc(r.tags) : ''}</span></span>
        ${cells}
        <span class="open-hint" aria-hidden="true">&rsaquo;</span>
      </button>`;
  }).join('');
  el.querySelectorAll('button.case').forEach(b =>
    b.addEventListener('click', () => openDrawer(b.dataset.case, b)));
}

/* ---------------------------------------------------------------- the case panel

   One case at a time, in a panel over the page. It opens on a SUMMARY that
   reads top to bottom as an argument: the verdict, the two answers side by
   side, what the agent said, what search had to find, what it cost. The
   attempt's steps are a second tab, one line each, opened one at a time: the
   trace is what you read to find out WHY, and only after the summary has told
   you WHAT. Every section carries one line saying what it is, because the
   harness's own vocabulary (re-executed rows, required entities, where to fix)
   means nothing to a first-time reader. */

const drawer = { qid: null, arm: null, tab: 'summary', data: null, returnFocus: null };
const caseCache = {};

async function caseData(qid) {
  if (caseCache[qid]) return caseCache[qid];
  const q = lit(qid);
  const [detail, steps] = await Promise.all([
    query(`run: case_drawer + { where: qid = '${q}' }`),
    query(`run: case_steps + { where: qid = '${q}' }`),
  ]);
  return (caseCache[qid] = { detail, steps });
}

function openDrawer(qid, from) {
  drawer.qid = qid; drawer.tab = 'summary'; drawer.arm = null;
  drawer.returnFocus = from || document.activeElement;
  state.open = qid; writeUrl();
  document.querySelectorAll('button.case').forEach(b => b.classList.toggle('on', b.dataset.case === qid));
  const panel = document.getElementById('panel');
  document.getElementById('scrim').hidden = false;
  panel.hidden = false;
  document.body.classList.add('locked');
  requestAnimationFrame(() => panel.classList.add('is-open'));
  document.addEventListener('keydown', onPanelKey);
  paintDrawerShell();
  panel.querySelector('.p-close').focus();
  caseData(qid).then(d => {
    if (drawer.qid !== qid) return;   // a slower response for a case already left
    drawer.data = d;
    drawer.arm = drawer.arm || (d.detail[0] && d.detail[0].arm);
    paintDrawer();
  }).catch(e => {
    if (drawer.qid !== qid) return;
    document.getElementById('p-body').innerHTML = `<div class="err">${esc(e.message || e)}</div>`;
  });
}

function closeDrawer() {
  if (!drawer.qid) return;
  drawer.qid = null; drawer.data = null; state.open = null; writeUrl();
  const panel = document.getElementById('panel');
  panel.classList.remove('is-open');
  document.getElementById('scrim').hidden = true;
  document.body.classList.remove('locked');
  document.removeEventListener('keydown', onPanelKey);
  setTimeout(() => { if (!drawer.qid) panel.hidden = true; }, 220);
  document.querySelectorAll('button.case.on').forEach(b => b.classList.remove('on'));
  const back = drawer.returnFocus && document.contains(drawer.returnFocus) ? drawer.returnFocus
    : document.querySelector('button.case');
  back && back.focus();
}

function stepCase(delta) {
  const rows = visible(); const i = rows.findIndex(r => r.qid === drawer.qid);
  const next = rows[i + delta];
  if (next) openDrawer(next.qid, document.querySelector(`button.case[data-case="${CSS.escape(next.qid)}"]`));
}

function onPanelKey(e) {
  if (e.key === 'Escape') { e.preventDefault(); closeDrawer(); return; }
  if ((e.key === 'ArrowDown' || e.key === 'j') && !e.target.closest('input,textarea,select')) { e.preventDefault(); stepCase(1); return; }
  if ((e.key === 'ArrowUp' || e.key === 'k') && !e.target.closest('input,textarea,select')) { e.preventDefault(); stepCase(-1); return; }
  if (e.key !== 'Tab') return;
  // Keep Tab inside the open panel, so a keyboard user cannot walk into the page behind it.
  const f = [...document.getElementById('panel').querySelectorAll(
    'button:not([disabled]),a[href],summary,[tabindex="0"]')].filter(x => x.offsetParent !== null);
  if (!f.length) return;
  const first = f[0], last = f[f.length - 1];
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
}

function paintDrawerShell() {
  const r = state.rows.find(x => x.qid === drawer.qid) || { question: drawer.qid };
  const rows = visible(); const i = rows.findIndex(x => x.qid === drawer.qid);
  document.getElementById('p-head').innerHTML = `
    <div class="p-nav">
      <span class="eyebrow">${i >= 0 ? `case ${i + 1} of ${rows.length}` : 'case'} · ${esc(short(drawer.qid))}</span>
      <span class="p-btns">
        <button class="iconbtn" data-step="-1" ${i <= 0 ? 'disabled' : ''} title="Previous case (k or ↑)" aria-label="Previous case">&uarr;</button>
        <button class="iconbtn" data-step="1" ${i < 0 || i >= rows.length - 1 ? 'disabled' : ''} title="Next case (j or ↓)" aria-label="Next case">&darr;</button>
        <button class="iconbtn p-close" title="Close (Esc)" aria-label="Close">&times;</button>
      </span>
    </div>
    <h2 id="p-title">${esc(r.question)}</h2>
    <div id="p-verdict"></div>
    <div id="p-tabs" class="tabs" role="tablist"></div>`;
  document.getElementById('p-body').innerHTML = '<div class="empty">Loading…</div>';
  document.querySelectorAll('#p-head [data-step]').forEach(b => b.addEventListener('click', () => stepCase(+b.dataset.step)));
  document.querySelector('#p-head .p-close').addEventListener('click', closeDrawer);
}

const VERDICT_SENTENCE = {
  match: 'The judge marked this answer <b>correct</b>',
  near_match: 'The judge marked this answer <b>partly right</b>: defensibly different from the key, so it counts as neither a pass nor a fail',
  no_match: 'The judge marked this answer <b>wrong</b>',
  needs_human: 'The judge <b>would not decide</b> this one; a person needs to',
};

function paintDrawer() {
  const { detail, steps } = drawer.data;
  if (!detail.length) { document.getElementById('p-body').innerHTML = '<div class="empty">No attempt recorded for this case.</div>'; return; }
  const d = detail.find(x => x.arm === drawer.arm) || detail[0];
  const mine = steps.filter(s => s.arm === d.arm);
  document.getElementById('p-verdict').innerHTML = `<div class="verdict ${esc(d.verdict || 'none')}">
      ${pill(d.verdict)}<span>${VERDICT_SENTENCE[d.verdict] || 'No verdict recorded'}${
        d.confidence != null ? `, ${esc(d.confidence)}/10 confident.` : '.'}</span></div>`;
  document.getElementById('p-tabs').innerHTML =
    (detail.length > 1 ? `<span class="armtabs">${detail.map(x =>
      `<button role="tab" data-arm="${esc(x.arm)}" aria-selected="${x.arm === d.arm}">${esc(x.arm)} ${pill(x.verdict)}</button>`).join('')}</span>` : '') +
    `<button role="tab" data-tab="summary" aria-selected="${drawer.tab === 'summary'}">Summary</button>
     <button role="tab" data-tab="steps" aria-selected="${drawer.tab === 'steps'}">Step by step <span class="mute">${mine.length}</span></button>`;
  document.querySelectorAll('#p-tabs [data-tab]').forEach(b => b.addEventListener('click', () => { drawer.tab = b.dataset.tab; paintDrawer(); }));
  document.querySelectorAll('#p-tabs [data-arm]').forEach(b => b.addEventListener('click', () => { drawer.arm = b.dataset.arm; paintDrawer(); }));
  const body = document.getElementById('p-body');
  body.innerHTML = drawer.tab === 'steps' ? stepsTab(d, mine) : summaryTab(d, mine);
  body.scrollTop = 0;
  body.querySelectorAll('[data-goto-steps]').forEach(b => b.addEventListener('click', () => { drawer.tab = 'steps'; paintDrawer(); }));
  body.querySelectorAll('.st-toggle').forEach(b => b.addEventListener('click', () => {
    const li = b.closest('li'); const open = li.classList.toggle('open'); b.setAttribute('aria-expanded', open);
  }));
  const all = body.querySelector('[data-expand-all]');
  if (all) all.addEventListener('click', () => {
    const lis = body.querySelectorAll('.steps > li'); const open = [...lis].some(l => !l.classList.contains('open'));
    lis.forEach(l => { l.classList.toggle('open', open); l.querySelector('.st-toggle')?.setAttribute('aria-expanded', open); });
    all.textContent = open ? 'Collapse all' : 'Expand all';
  });
}

function sec(title, explain, inner, cls = '') {
  return `<section class="psec ${cls}"><h3>${title}</h3>${explain ? `<p class="explain">${explain}</p>` : ''}${inner}</section>`;
}

const ENTITY_STATUS = {
  exact: ['came back', 'ranked directly by the search'],
  alias: ['came back', 'under a sibling source'],
  in_docs: ['came back', "named in a returned source's docs"],
  missing: ['did not come back', 'the agent never saw it'],
};

function summaryTab(d, mine) {
  let h = '';
  // Flags first: each one changes what the rest of the panel means.
  const flags = [];
  if (truthy(d.contaminated)) flags.push(`<b>Contaminated.</b> The agent saw something it should not have (such as the answer key), so this attempt is not a measurement.`);
  if (d.gold_status && d.gold_status !== 'verified') flags.push(`<b>The answer key is ${esc(d.gold_status)}.</b> It has not been confirmed two ways, so a mismatch may be the key's fault.`);
  if (d.must_not_use_hits) flags.push(`<b>Vetoed by a script.</b> The final query used <span class="mono">${esc(d.must_not_use_hits)}</span>, which this question forbids, so it scored wrong whatever the judge thought. The judge itself said <b>${esc(d.judge_verdict || 'nothing')}</b>. If the field is fine and only its use is wrong, the rule should be prose instead, and this is a false failure.`);
  if (flags.length) h += `<div class="flags">${flags.map(f => `<div class="flag">${f}</div>`).join('')}</div>`;

  // 1. Why.
  const groups = state.backlog.filter(g => (g.cases || []).some(c => c.qid === d.qid));
  // "coverage not measured" only says the harness had no coverage label to
  // place the miss by. Beside a diagnosis that did place it, it reads as a
  // contradiction, so the diagnosis wins.
  const wtf = groups.length && d.where_to_fix === 'coverage not measured' ? '' : d.where_to_fix;
  h += sec('Why the judge decided this', 'The judge compared the agent\'s answer with the answer key, using the rubric at the bottom of this panel.',
    `<p class="judge">${esc(d.judge_reasoning || 'No reasoning recorded.')}</p>${
      wtf || groups.length ? `<div class="fixline">${wtf ? `<div><span class="k">Where the failure lives</span><span class="chip">${esc(wtf)}</span>${
        d.why ? ` <span class="mute">${esc(d.why)}</span>` : ''}</div>` : ''}${
        groups.map(g => `<div><span class="k">Diagnosis</span><span class="chip acc">fix in: ${esc(g.lever || '?')}</span> ${esc(g.cluster)}</div>`).join('')}</div>` : ''}`);

  // 2. The two answers, side by side.
  const agentRows = predictionBlock(d.prediction);
  h += sec('The answer key, and what the agent\'s query returns',
    'Left: the correct answer, worked out from the raw data. Right: the agent\'s final query, <b>run again by the harness</b>, so you see what the query really returns rather than what the agent wrote about it. The judge saw both.',
    `<div class="vs"><div><div class="k">Answer key</div>${goldenBlock(d)}</div>
      <div><div class="k">Agent's query, re-run</div>${agentRows || '<div class="box mute">Not re-run: the agent submitted no final query, so the judge scored its prose alone.</div>'}</div></div>${
      d.final_query ? `<details class="sub"><summary><span class="caret">&#9654;</span>The agent's final query</summary><pre>${esc(d.final_query)}</pre></details>` : ''}`);

  // 3. What the agent said.
  const lastText = [...mine].reverse().find(s => s.kind === 'text');
  const said = lastText ? lastText.label : d.answer;
  h += sec('What the agent told the user', 'Its final message. Earlier reasoning is under Step by step.',
    `<div class="said">${md(said || 'No answer recorded.')}</div>`);

  // 4. Search.
  const req = ((state.required[d.qid] || {})[d.arm]) || [];
  if (req.length) {
    const got = req.filter(e => e.status !== 'missing').length;
    h += sec(`What search had to find <span class="count-inline ${got < req.length ? 'short' : ''}">${got} of ${req.length}</span>`,
      'The fields the correct answer depends on. The agent can only use a field that its <span class="mono">get_context</span> search returned.',
      `<ul class="ents">${req.map(e => { const [a, b] = ENTITY_STATUS[e.status] || [e.status, '']; return `<li class="${esc(e.status)}">
        <i class="dot ${esc(e.status)}"></i><span class="mono">${esc(e.entity_id)}</span><span>${esc(a)}<span class="mute"> · ${esc(b)}</span></span></li>`; }).join('')}</ul>`);
  }

  // 5. Effort.
  h += sec('What this attempt cost', 'Totals for this one attempt.',
    `<div class="effortgrid">
      <div><b>${num(d.num_turns)}</b>turns</div>
      <div><b>${num(d.n_get_context)}</b>searches</div>
      <div><b>${num(d.n_execute)}</b>queries${d.n_execute_errors ? `<span class="err-n">${d.n_execute_errors} failed</span>` : ''}</div>
      <div><b>${num(d.wall_seconds)}s</b>elapsed</div>
      <div><b>${usd(d.cost_usd)}</b>spent</div>
    </div><button class="linkbtn" data-goto-steps>See every step &rarr;</button>`);

  // 6. Rubric.
  if (d.rubric || d.must_state)
    h += `<details class="sub rubric"><summary><span class="caret">&#9654;</span>How this answer was judged: the rubric</summary>
      <div class="box">${d.rubric ? esc(d.rubric) : ''}${d.must_state ? `<br><br><b>Must state.</b> ${esc(d.must_state)}` : ''}</div></details>`;
  if (d.transcript) h += `<div class="transcript">transcript: ${esc(d.transcript)}</div>`;
  return h;
}

/* One line per step, in words a reader would use; the detail opens in place. */
const STEP_KIND = {
  text: ['Thought', 'think'], skill: ['Skill', 'skill'],
  get_context: ['Search', 'search'], execute_query: ['Query', 'query'],
};

function stepSummary(s, err) {
  // One line, markdown markers dropped: a summary line is not rendered as markdown.
  const one = t => { const x = String(t || '').replace(/\*\*|`/g, '').replace(/\s+/g, ' ');
    return esc(x.slice(0, 140)) + (x.length > 140 ? '…' : ''); };
  if (s.kind === 'get_context') return one(s.label) + (err ? '' : `<span class="st-r">${s.n_results ?? '?'} results</span>`);
  if (s.kind === 'execute_query') return `<span class="mono">${one(s.label)}</span>` +
    `<span class="st-r${err ? ' bad' : ''}">${err ? 'failed' : `${s.n_results ?? '?'} row${s.n_results === 1 ? '' : 's'}`}</span>`;
  if (s.kind === 'skill') return `<span class="mono">${esc(s.label)}</span>`;
  return one(s.label);
}

function stepDetail(s, err) {
  if (s.kind === 'text') return md(s.label);
  if (s.kind === 'skill') return `<p class="mute">${esc(s.detail || '')}</p>`;
  if (s.kind === 'get_context') {
    const targets = String(s.label || '').split(' · ').filter(Boolean);
    return `<div class="k">What it searched for</div><ul class="targets">${targets.map(t => {
      const m = t.match(/^(\w+):\s*(.*)$/); return m ? `<li><span class="chip">${esc(m[1])}</span> ${esc(m[2])}</li>` : `<li>${esc(t)}</li>`; }).join('')}</ul>${
      err ? `<div class="err">${esc(s.detail)}</div>` : `<p class="mute">${s.n_results ?? '?'} entities came back.</p>`}`;
  }
  if (s.kind === 'execute_query') {
    let res = '';
    if (err) res = `<div class="k">The error</div><div class="err rawtext">${esc(s.detail)}</div>`;
    else if (s.detail) res = `<div class="k">What it returned</div>${predictionBlock(s.detail)}`;
    return `<div class="k">The query</div><pre>${esc(s.label)}</pre>${res}`;
  }
  return `<pre>${esc(s.label)}</pre>${s.detail ? `<p class="mute">${esc(s.detail)}</p>` : ''}`;
}

function stepsTab(d, mine) {
  if (!mine.length)
    return `<p class="explain">No step-by-step record for this attempt; here is the final answer and query.</p>
      <div class="said">${md(d.answer)}</div>${d.final_query ? `<pre>${esc(d.final_query)}</pre>` : ''}`;
  const q = mine.filter(s => s.kind === 'execute_query');
  const failed = q.filter(s => truthy(s.is_error)).length;
  return `<div class="st-head"><p class="explain">Everything the agent did, in order: ${
      mine.filter(s => s.kind === 'get_context').length} search${mine.filter(s => s.kind === 'get_context').length === 1 ? '' : 'es'}, ${
      q.length} quer${q.length === 1 ? 'y' : 'ies'}${failed ? ` (${failed} failed)` : ''}, and its reasoning between them.
      Open a step to see the full query and what came back.</p>
      <button class="linkbtn" data-expand-all>Expand all</button></div>
    <ol class="steps">${mine.map((s, i) => {
      const err = truthy(s.is_error);
      const [label, cls] = STEP_KIND[s.kind] || [s.kind, 'other'];
      return `<li class="st ${cls}${err ? ' error' : ''}">
        <button class="st-toggle" aria-expanded="false">
          <span class="st-n">${i + 1}</span><span class="st-k">${err ? (s.kind === 'execute_query' ? 'Query failed' : label + ' · failed') : label}</span>
          <span class="st-s">${stepSummary(s, err)}</span><span class="caret">&#9654;</span></button>
        <div class="st-d">${stepDetail(s, err)}</div></li>`;
    }).join('')}</ol>`;
}

/* ---------------------------------------------------------------- header */

/* The part of every qid the set shares ("storefront-tour-") says nothing on a
   label, so direct labels and backlog links drop it. */
function shortener(qids) {
  if (qids.length < 2) return q => q;
  let p = qids[0];
  for (const q of qids) while (!q.startsWith(p)) p = p.slice(0, -1);
  p = p.slice(0, p.lastIndexOf('-') + 1);
  return q => (p && q.length > p.length ? q.slice(p.length) : q);
}
let short = q => q;

/* The split under each score: every case once, as correct, undecided, or a
   failure under the place it would have to be fixed. A failure the scorer
   could not place is still counted, so the parts always sum to the whole. */
function splitFor(arm) {
  const parts = new Map();
  const add = (key, cls, label, filter) => {
    const p = parts.get(key) || { key, cls, label, filter, n: 0, qids: [] };
    p.n++; parts.set(key, p); return p;
  };
  for (const r of state.rows) {
    const a = r.arms.find(x => x.arm === arm);
    if (!a) continue;
    // A score against a key established wrong is in the attempts but not in
    // `passed`, so it gets its own part rather than hiding under correct/wrong.
    const p = a.counts === 'false' ? add('unscored', 'unscored', 'not scored · answer key is wrong', null)
      : a.outcome === 'pass' ? add('pass', 'pass', 'correct', null)
      : a.outcome === 'fail' ? add('fail:' + (a.where_to_fix || ''), 'fail',
          'wrong · ' + (a.where_to_fix || 'not attributed'),
          a.where_to_fix ? { wtf: a.where_to_fix } : { mode: 'failures' })
      : add('neither', 'neither', 'undecided (partly or needs human)', { mode: 'undecided' });
    p.qids.push(r.qid);
  }
  const order = { pass: 0, fail: 1, neither: 2, unscored: 3 };
  return [...parts.values()].sort((x, y) => order[x.cls] - order[y.cls] || y.n - x.n);
}

function applyFilter(f) {
  if (!f) return;
  if (f.wtf != null) { state.wtf = state.wtf === f.wtf ? '' : f.wtf; }
  if (f.mode) { state.mode = state.mode === f.mode ? 'all' : f.mode; }
  syncControls(); writeUrl(); renderHeroes(); renderList();
  document.getElementById('cases').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function segActive(f) {
  return f && ((f.wtf != null && state.wtf === f.wtf) || (f.mode && state.mode === f.mode));
}

function renderHeroes() {
  const { summary, effort, retrieval } = state.header;
  document.getElementById('heroes').innerHTML = summary.map((s, si) => {
    const e = effort.find(x => x.arm === s.arm) || {};
    const r = retrieval.find(x => x.arm === s.arm) || {};
    const attempts = e.attempt_count ?? s.confident_count;
    const parts = splitFor(s.arm);
    const total = parts.reduce((t, p) => t + p.n, 0) || 1;
    const bar = parts.map((p, i) => p.filter
      ? `<button class="${p.cls}${segActive(p.filter) ? ' on' : ''}" style="flex:${p.n}" data-arm="${si}" data-part="${i}"
          title="${esc(p.label)}: ${p.n} of ${total}. Click to list them." aria-label="${esc(p.label)}, ${p.n} cases"></button>`
      : `<button class="${p.cls}" style="flex:${p.n}" tabindex="-1" title="${esc(p.label)}: ${p.n} of ${total}" aria-hidden="true"></button>`).join('');
    const legend = parts.map((p, i) => `<li><i style="background:var(--${p.cls === 'neither' ? 'near' : p.cls === 'unscored' ? 'soft' : p.cls})"></i><b>${p.n}</b><span>${
      p.filter ? `<button data-arm="${si}" data-part="${i}">${esc(p.label)}</button>` : esc(p.label)}${
      p.cls === 'fail' ? ` <span class="why mono">${p.qids.map(q => esc(short(q))).join(', ')}</span>` : ''}</span></li>`).join('');
    return `<div class="hero">
      <div>
        <div class="arm">${esc(s.arm)}</div>
        <div class="score">${num(s.passed)}<small>/ ${num(attempts)}</small></div>
        <div class="score-l">answered correctly${s.confident_count !== attempts
          ? ` · ${num(s.confident_count)} decided` : ''}</div>
        <div class="kv">
          <div><b>${r.required_count ? pct(r.delivered_rate) : '—'}</b>
            retrieval recall<br>${r.delivered_count ?? '—'} of ${r.required_count ?? '—'} needed entities</div>
          <div><b>${usd(e.total_cost_usd)}</b>answerer spend<br>${usd(e.avg_cost_usd)} per question</div>
          ${e.contaminated_count ? `<div><b class="warn">${e.contaminated_count}</b>contaminated<br>not a measurement</div>` : ''}
        </div>
      </div>
      <div>
        <div class="split-h">Where every question landed</div>
        <div class="bar" role="group" aria-label="Outcome split">${bar}</div>
        <ul class="segs">${legend}</ul>
        <!-- Every figure but the last is a MEAN PER QUESTION; the qualifier leads
             so the line does not read as totals describing one attempt. -->
        <div class="effort" title="Mean per question across this arm's attempts, except the final figure, which is the arm's total spend.">
          <span class="qual">mean per question:</span> ${num(e.avg_turns, 1)} turns · ${
          num(e.avg_get_context_per_attempt, 1)} get_context · ${
          num(e.avg_execute_per_attempt, 1)} execute_query${e.execute_errors ? ` (${e.execute_errors} errored in all)` : ''} · ${
          num(e.avg_wall_seconds)} s</div>
      </div>
    </div>`;
  }).join('');
  document.querySelectorAll('#heroes [data-part]').forEach(b => b.addEventListener('click', () => {
    const parts = splitFor(state.header.summary[+b.dataset.arm].arm);
    applyFilter(parts[+b.dataset.part].filter);
  }));
}

/* ---------------------------------------------------------------- backlog */

function groupQids() {
  const g = state.backlog[state.group];
  return new Set(g ? (g.cases || []).map(c => c.qid) : []);
}

function openCase(qid) {
  if (!visible().some(r => r.qid === qid)) {
    state.mode = 'all'; state.q = ''; state.wtf = ''; state.group = null; syncControls();
    renderBacklog(); renderList();
  }
  openDrawer(qid, document.querySelector(`button.case[data-case="${CSS.escape(qid)}"]`));
}

function renderBacklog() {
  const sec = document.getElementById('backlog-sec');
  sec.hidden = !state.backlog.length;
  if (!state.backlog.length) return;
  document.getElementById('backlog').innerHTML = state.backlog.map((g, i) => `
    <div class="cluster${state.group === i ? ' on' : ''}" data-group="${i}" tabindex="0" role="button"
         aria-pressed="${state.group === i}" title="Show only this group's questions">
      <div class="n">${num(g.cases_fixed)}</div>
      <div>
        <div class="label">${esc(g.cluster)}</div>
        <div class="prop${g.proposal ? '' : ' none'}">${g.proposal
          ? `<b>Proposed edit.</b> ${esc(g.proposal)}`
          : 'No edit proposed yet. eval-improve decides what to change.'}</div>
        <div class="qs">${(g.cases || []).map(c =>
          `<button class="qlink" data-qid="${esc(c.qid)}" title="${esc(c.question)}">${esc(short(c.qid))}</button>`).join('')}</div>
      </div>
      <div class="tags tags-r">${g.lever ? `<span class="chip acc">fix in: ${esc(g.lever)}</span>` : ''}${
        g.where_to_fix ? `<span class="chip">${esc(g.where_to_fix)}</span>` : ''}</div>
    </div>`).join('');
  document.querySelectorAll('#backlog .cluster').forEach(el => {
    const toggle = () => {
      const i = +el.dataset.group;
      state.group = state.group === i ? null : i; writeUrl(); renderBacklog(); renderList();
    };
    el.addEventListener('click', e => { if (!e.target.closest('.qlink')) toggle(); });
    el.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); } });
  });
  document.querySelectorAll('#backlog .qlink').forEach(b =>
    b.addEventListener('click', () => openCase(b.dataset.qid)));
}

/* ---------------------------------------------------------------- effort strip */

/* One dot per question per arm on a shared axis. A mean hides the thing worth
   seeing: whether the failures are the expensive attempts (the agent flailed)
   or the cheap ones (it stopped early, sure of a wrong reading). */
function renderStrip() {
  const sec = document.getElementById('effort-sec');
  if (!state.effort.length) { sec.hidden = true; return; }
  sec.hidden = false;
  const [id, unit, col, dp] = MEASURES.find(m => m[0] === state.measure);
  document.getElementById('measure').innerHTML = MEASURES.map(([m, u]) =>
    `<button data-m="${m}" aria-pressed="${m === state.measure}">${u}</button>`).join('');
  const outcomeOf = {};
  for (const r of state.rows) for (const a of r.arms) outcomeOf[r.qid + '\u0000' + a.arm] = a.outcome;
  const pts = state.effort.filter(e => e[col] != null).map(e => ({
    qid: e.qid, arm: e.arm, v: Number(e[col]), o: outcomeOf[e.qid + '\u0000' + e.arm] || 'none' }));
  const el = document.getElementById('strip');
  const W = Math.max(320, el.clientWidth || 800), padL = state.arms.length > 1 ? 120 : 8, padR = 24;
  const laneH = 84, H = 28 + laneH * state.arms.length;
  const max = Math.max(...pts.map(p => p.v), 0) || 1;
  const x = v => padL + (W - padL - padR) * (v / max);
  // A step from the axis's own magnitude, so a dollar axis under $1 still gets
  // ticks. A whole-number measure never steps below 1.
  const mag = Math.pow(10, Math.floor(Math.log10(max)));
  const step = Math.max(mag * (max / mag > 4 ? 1 : .5), dp ? 0 : 1);
  let svg = '';
  for (let t = 0; t <= max + 1e-9; t += step)
    svg += `<text class="tick" x="${x(t)}" y="${H - 6}" text-anchor="middle">${unit === 'dollars' ? '$' : ''}${num(t, dp)}</text>`;
  state.arms.forEach((arm, li) => {
    const cy = 20 + laneH * li + laneH / 2;
    svg += `<line class="axis" x1="${padL}" x2="${W - padR}" y1="${cy}" y2="${cy}"/>`;
    if (state.arms.length > 1) svg += `<text class="lane" x="0" y="${cy + 4}">${esc(arm.slice(0, 16))}</text>`;
    const mine = pts.filter(p => p.arm === arm).sort((a, b) => a.v - b.v);
    const top = mine[mine.length - 1];
    // Dots that would overlap stack upward, so every question stays countable.
    const placed = [], labels = [];
    for (const p of mine) {
      let dy = 0; while (placed.some(q => Math.abs(q.px - x(p.v)) < 11 && q.dy === dy)) dy -= 11;
      placed.push({ px: x(p.v), dy });
      svg += `<circle class="${esc(p.o)}" cx="${x(p.v)}" cy="${cy + dy}" r="5" data-qid="${esc(p.qid)}">
        <title>${esc(p.qid)} · ${unit === 'dollars' ? '$' : ''}${num(p.v, dp)} ${unit === 'dollars' ? '' : unit} · ${esc(p.o)}</title></circle>`;
      // Label the failures and the costliest question directly; the rest are hover.
      if (p.o === 'fail' || p === top) {
        const below = p.o === 'fail', text = short(p.qid);
        const anchor = x(p.v) > W - 120 ? 'end' : x(p.v) < padL + 60 ? 'start' : 'middle';
        const w = text.length * 6.6, x0 = anchor === 'end' ? x(p.v) - w : anchor === 'start' ? x(p.v) : x(p.v) - w / 2;
        let y = below ? cy + 20 : cy + dy - 10;
        // Labels that would print over each other step down a line instead.
        while (labels.some(l => l.y === y && x0 < l.x1 + 6 && x0 + w > l.x0 - 6)) y += below ? 13 : -13;
        labels.push({ x0, x1: x0 + w, y });
        svg += `<text class="dl${below ? ' fail' : ''}" x="${x(p.v)}" y="${y}" text-anchor="${anchor}">${esc(text)}</text>`;
      }
    }
  });
  el.innerHTML = `<svg class="strip" viewBox="0 0 ${W} ${H}" style="height:${H}px" role="img"
    aria-label="Effort per question in ${unit}">${svg}</svg>`;
  el.querySelectorAll('circle').forEach(c => c.addEventListener('click', () => openCase(c.dataset.qid)));
  const mean = xs => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
  const f = mean(pts.filter(p => p.o === 'fail').map(p => p.v));
  const ok = mean(pts.filter(p => p.o === 'pass').map(p => p.v));
  const money = v => (unit === 'dollars' ? '$' : '') + num(v, unit === 'dollars' ? 2 : 1) + (unit === 'dollars' ? '' : ' ' + unit);
  document.getElementById('effort-cap').textContent =
    `One dot per question; red is wrong, green correct. Click a dot to open it.` +
    (f != null && ok != null ? ` Wrong answers averaged ${money(f)}, correct ones ${money(ok)}${
      f < ok ? ': the failures stopped early rather than struggling.' : '.'}` : '');
}

/* ---------------------------------------------------------------- chips */

/* Every active filter is on screen and removable. A filtered list with no
   visible filter reads as the whole run. */
function renderChips() {
  const chips = [];
  const mode = MODES.find(m => m[0] === state.mode);
  if (state.mode !== 'all') chips.push(['mode', `show: ${mode[1]}`]);
  if (state.wtf) chips.push(['wtf', `where to fix: ${state.wtf}`]);
  if (state.group != null && state.backlog[state.group])
    chips.push(['group', `group ${state.group + 1}: ${state.backlog[state.group].cases_fixed} question(s)`]);
  if (state.q) chips.push(['q', `search: “${state.q}”`]);
  const el = document.getElementById('chips');
  el.innerHTML = chips.map(([k, l]) =>
    `<span class="fchip">${esc(l)}<button data-k="${k}" aria-label="Remove filter ${esc(l)}">×</button></span>`).join('') +
    (chips.length > 1 ? '<button class="clearall" data-k="all">clear all</button>' : '');
  el.querySelectorAll('[data-k]').forEach(b => b.addEventListener('click', () => {
    const k = b.dataset.k;
    if (k === 'mode' || k === 'all') state.mode = 'all';
    if (k === 'wtf' || k === 'all') state.wtf = '';
    if (k === 'group' || k === 'all') state.group = null;
    if (k === 'q' || k === 'all') state.q = '';
    syncControls(); writeUrl(); renderHeroes(); renderBacklog(); renderList();
  }));
}

function syncControls() {
  document.querySelectorAll('#modes button').forEach(x => x.setAttribute('aria-pressed', x.dataset.mode === state.mode));
  document.getElementById('q').value = state.q;
  document.getElementById('wtf').value = state.wtf;
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
    document.getElementById('heroes').innerHTML =
      `<div class="err"><b>Could not load the run.</b><br>${esc(e.message || e)}
       <br><br>Serve this package from Publisher; opening index.html from disk has no
       <code>/sdk/publisher.js</code> and no model to query.</div>`;
    return;
  }
  // The backlog and the effort strip are optional: a run that was never
  // diagnosed has no clusters, and the rest of the page must still load.
  const [backlog, perCase] = await Promise.all([
    query('run: backlog').catch(() => []),
    query('run: case_drawer -> { select: qid, arm, num_turns, cost_usd, wall_seconds }').catch(() => []),
  ]);

  for (const r of required)
    ((state.required[r.qid] ??= {})[r.arm] ??= []).push(r);
  state.arms = summary.map(s => s.arm);
  state.rows = matrix.map(r => ({
    ...r, arms: (r.by_arm || []).map(a => ({ ...a, required: (state.required[r.qid] || {})[a.arm] || [] })),
  }));
  // `backlog` orders by size only, so equal-sized groups come back in any
  // order, and the group index in the URL would point at a different group on
  // the next load. Break ties by label so the index is stable.
  state.backlog = [...backlog].sort((x, y) =>
    (y.cases_fixed || 0) - (x.cases_fixed || 0) || String(x.cluster).localeCompare(String(y.cluster)));
  state.effort = perCase;
  if (state.group != null && !state.backlog[state.group]) state.group = null;
  state.header = { summary, effort, retrieval };
  short = shortener(state.rows.map(r => r.qid));

  document.getElementById('eyebrow').textContent =
    `${state.arms.length === 1 ? 'eval run' : state.arms.length + ' arms'} · ${state.rows.length} cases`;
  document.title = state.arms.length === 1 ? `Eval run · ${state.arms[0]}` : 'Eval runs';

  const fixes = [...new Set(state.rows.flatMap(r => r.arms.map(a => a.where_to_fix).filter(Boolean)))].sort();
  document.getElementById('wtf').title =
    // No label is split across a concatenation: `score_retrieval_test.py`
    // checks each one appears here verbatim, and a label broken over two
    // string literals reads as absent.
    'Where a failure would have to be fixed. '
    + '"model coverage": nothing in the model answers this. '
    + '"not retrieved": a search of the right kind went out and the entity '
    + 'did not come back. '
    + '"never asked": no search of that kind went out at all. '
    + '"delivered, wrong": everything arrived; the agent or the docs, '
    + 'diagnose decides. '
    + '"refusal behaviour": the model cannot answer and the agent did. '
    + '"coverage not measured": a miss with no measured coverage label.';
  document.getElementById('wtf').innerHTML = '<option value="">Where to fix: any</option>' +
    fixes.map(f => `<option${f === state.wtf ? ' selected' : ''}>${esc(f)}</option>`).join('');

  renderHeroes();
  renderBacklog();
  renderStrip();
  renderList();
  if (state.open && state.rows.some(r => r.qid === state.open)) openDrawer(state.open);

  document.getElementById('modes').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    state.mode = b.dataset.mode; writeUrl(); syncControls(); renderHeroes(); renderList();
  });
  document.getElementById('q').addEventListener('input', e => { state.q = e.target.value; writeUrl(); renderList(); });
  document.getElementById('wtf').addEventListener('change', e => { state.wtf = e.target.value; writeUrl(); renderHeroes(); renderList(); });
  document.getElementById('measure').addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    state.measure = b.dataset.m; writeUrl(); renderStrip();
  });
  let rt; addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(renderStrip, 150); });
  document.getElementById('scrim').addEventListener('click', closeDrawer);
}

main();
