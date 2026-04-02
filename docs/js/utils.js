// ── Constants ────────────────────────────────────────────────────────────────
const ALL_SEASONS = [2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025];
const PRE_MODERN_CUTOFF = 2016; // current ownership established this year

// SEASONS respects the Pre-Modern Era toggle (default: modern only, 2016+)
let SEASONS = localStorage.getItem('includePreModern') === 'true'
  ? ALL_SEASONS
  : ALL_SEASONS.filter(y => y >= PRE_MODERN_CUTOFF);

// Normalize owner names to canonical display names
function normOwner(o) {
  if (o === 'Steven')  return 'Steve';
  if (o === 'Patrick' || o === 'patrick') return 'Pat';
  return o;
}

// ── Data loading ─────────────────────────────────────────────────────────────
async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${url}`);
  return r.json();
}

function loadFile(year, file) {
  return fetchJSON(`data/${year}/${file}`);
}

// Load a file for every season; returns {year: data} (null on failure)
async function loadAllSeasons(file) {
  const pairs = await Promise.all(
    SEASONS.map(y => loadFile(y, file).then(d => [y, d]).catch(() => [y, null]))
  );
  return Object.fromEntries(pairs);
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function buildTeamOwnerMap(standings) {
  const map = {};
  for (const t of standings) map[t.team] = normOwner(t.owner);
  return map;
}

function fmt2(n) { return typeof n === 'number' ? n.toFixed(2) : parseFloat(n).toFixed(2); }
function pctStr(n) { return (n * 100).toFixed(1) + '%'; }

// ── Aggregated data ───────────────────────────────────────────────────────────

// Returns [{year, team, owner, score, opponent, oppScore}] for each season champ
async function getChampions() {
  const [allMatchups, allStandings] = await Promise.all([
    loadAllSeasons('matchups.json'),
    loadAllSeasons('standings.json'),
  ]);
  return SEASONS.flatMap(year => {
    const matchups = allMatchups[year] || [];
    const standings = allStandings[year] || [];
    const teamOwner = buildTeamOwnerMap(standings);
    const champ = matchups.find(m => m.round === 'Championship');
    if (!champ) return [];
    const s1 = parseFloat(champ.score1), s2 = parseFloat(champ.score2);
    const [winner, winScore, loser, loseScore] =
      s1 > s2 ? [champ.team1, s1, champ.team2, s2] : [champ.team2, s2, champ.team1, s1];
    return [{ year, team: winner, owner: teamOwner[winner] || '?', score: winScore, opponent: loser, oppOwner: teamOwner[loser] || '?', oppScore: loseScore }];
  });
}

// Returns owner stats array sorted by championships desc, wins desc
async function getOwnerStats() {
  const [allStandings, champions] = await Promise.all([
    loadAllSeasons('standings.json'),
    getChampions(),
  ]);
  const champCounts = {};
  champions.forEach(c => { champCounts[c.owner] = (champCounts[c.owner] || 0) + 1; });

  const stats = {};
  for (const year of SEASONS) {
    for (const t of (allStandings[year] || [])) {
      const owner = normOwner(t.owner);
      if (!stats[owner]) stats[owner] = { owner, wins:0, losses:0, ties:0, pf:0, pa:0, seasons:0 };
      stats[owner].wins   += parseInt(t.wins)   || 0;
      stats[owner].losses += parseInt(t.losses) || 0;
      stats[owner].ties   += parseInt(t.ties)   || 0;
      stats[owner].pf     += parseFloat(t.points_for)     || 0;
      stats[owner].pa     += parseFloat(t.points_against) || 0;
      stats[owner].seasons += 1;
    }
  }
  for (const s of Object.values(stats)) {
    s.championships = champCounts[s.owner] || 0;
    const g = s.wins + s.losses + s.ties;
    s.winPct = g ? s.wins / g : 0;
    s.avgPF  = s.seasons ? s.pf / s.seasons : 0;
  }
  return Object.values(stats).sort((a, b) =>
    b.championships - a.championships || b.wins - a.wins
  );
}

// Returns all matchups enriched with owner names across every season
async function getAllMatchupsWithOwners() {
  const [allMatchups, allStandings] = await Promise.all([
    loadAllSeasons('matchups.json'),
    loadAllSeasons('standings.json'),
  ]);
  const result = [];
  for (const year of SEASONS) {
    const standings = allStandings[year] || [];
    const teamOwner = buildTeamOwnerMap(standings);
    for (const m of (allMatchups[year] || [])) {
      result.push({
        year, week: m.week,
        team1: m.team1, owner1: teamOwner[m.team1] || m.team1, score1: parseFloat(m.score1),
        team2: m.team2, owner2: teamOwner[m.team2] || m.team2, score2: parseFloat(m.score2),
        playoff: m.playoff, round: m.round,
      });
    }
  }
  return result;
}

// Returns sorted list of all unique normalized owner names
async function getOwnerList() {
  const allStandings = await loadAllSeasons('standings.json');
  const set = new Set();
  for (const s of Object.values(allStandings)) {
    if (s) s.forEach(t => set.add(normOwner(t.owner)));
  }
  return [...set].sort();
}

// ── DOM helpers ───────────────────────────────────────────────────────────────
function el(tag, attrs = {}, ...children) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') e.className = v;
    else if (k === 'html') e.innerHTML = v;
    else e.setAttribute(k, v);
  }
  for (const c of children) {
    if (c != null) e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  }
  return e;
}

function makeSortableTable(tableEl) {
  const ths = tableEl.querySelectorAll('th.sortable');
  ths.forEach((th, idx) => {
    th.addEventListener('click', () => {
      const asc = th.classList.contains('sort-asc');
      ths.forEach(t => t.classList.remove('sort-asc', 'sort-desc'));
      th.classList.add(asc ? 'sort-desc' : 'sort-asc');
      const tbody = tableEl.querySelector('tbody');
      const rows = [...tbody.querySelectorAll('tr')];
      const dir = asc ? -1 : 1;
      rows.sort((a, b) => {
        const av = a.cells[idx]?.dataset.val ?? a.cells[idx]?.textContent ?? '';
        const bv = b.cells[idx]?.dataset.val ?? b.cells[idx]?.textContent ?? '';
        const an = parseFloat(av), bn = parseFloat(bv);
        if (!isNaN(an) && !isNaN(bn)) return dir * (an - bn);
        return dir * av.localeCompare(bv);
      });
      rows.forEach(r => tbody.appendChild(r));
    });
  });
}

// ── Pre-Modern Era toggle ─────────────────────────────────────────────────────
// Injected into every page's nav automatically.
document.addEventListener('DOMContentLoaded', () => {
  const nav = document.querySelector('.nav-inner');
  if (!nav) return;

  const isOn = localStorage.getItem('includePreModern') === 'true';

  const btn = document.createElement('button');
  btn.id        = 'pre-modern-toggle';
  btn.title     = 'Toggle whether pre-2016 seasons (2011–2015) are included in stats';
  btn.innerHTML = isOn
    ? '<span style="color:var(--gold)">&#9679;</span> Pre-Modern'
    : '<span style="color:var(--border)">&#9675;</span> Pre-Modern';
  btn.style.cssText = [
    'background:none',
    'border:1px solid var(--border)',
    'border-radius:20px',
    'color:var(--muted)',
    'cursor:pointer',
    'font-size:11px',
    'font-weight:600',
    'letter-spacing:0.03em',
    'margin-left:auto',
    'padding:4px 10px',
    'white-space:nowrap',
    'transition:border-color 0.15s, color 0.15s',
  ].join(';');

  btn.addEventListener('mouseenter', () => { btn.style.borderColor = 'var(--muted)'; btn.style.color = 'var(--text)'; });
  btn.addEventListener('mouseleave', () => { btn.style.borderColor = 'var(--border)'; btn.style.color = 'var(--muted)'; });

  btn.addEventListener('click', () => {
    const current = localStorage.getItem('includePreModern') === 'true';
    localStorage.setItem('includePreModern', String(!current));
    location.reload();
  });

  nav.appendChild(btn);
});
