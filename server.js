/**
 * Volleyball League Scheduler - Server
 *
 * Express server that:
 *  - Serves the public/ directory as static files
 *  - Exposes POST /api/generate to build a full league schedule
 *  - Exposes POST /api/state/save, GET /api/state/load for persistence
 *  - Exposes POST /api/scores to record match results
 *  - Exposes GET /api/standings for computed division standings
 *  - Exposes POST /api/finalize-week for relegation
 *  - Exposes POST /api/reschedule-night to reduce courts for a game night
 *  - Exposes PATCH /api/edit-match to edit a single match
 *
 * Run with: node server.js
 */

const express = require('express');
const path = require('path');
const fs = require('fs');
const { Redis } = require('@upstash/redis');

const app = express();
const PORT = process.env.PORT || 3000;

// Parse JSON request bodies
app.use(express.json());

// Serve frontend from public/
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// Data Persistence (Upstash Redis when env vars present, file-based fallback)
// ---------------------------------------------------------------------------

const STATE_KEY = 'league-state';

const redis = (process.env.KV_REST_API_URL && process.env.KV_REST_API_TOKEN)
  ? new Redis({ url: process.env.KV_REST_API_URL, token: process.env.KV_REST_API_TOKEN })
  : null;

// File-based fallback for local dev
const DATA_DIR = path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'league-state.json');

async function loadStateFromFile() {
  if (redis) {
    try {
      const data = await redis.get(STATE_KEY);
      return data || null;
    } catch (e) {
      console.error('Redis load error:', e);
      return null;
    }
  }
  try {
    if (fs.existsSync(STATE_FILE)) {
      return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    }
  } catch (e) {
    console.error('Error loading state:', e);
  }
  return null;
}

async function persistState(state) {
  if (redis) {
    try {
      await redis.set(STATE_KEY, state);
      return;
    } catch (e) {
      console.error('Redis save error:', e);
    }
  }
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
}

// ---------------------------------------------------------------------------
// Standings Computation
// ---------------------------------------------------------------------------

/**
 * Compute standings per division from schedule and recorded scores.
 * Win = 2 pts, Loss = 0 pts. Sorted by Pts desc → W desc → (PF-PA) desc.
 */
function computeStandings(schedule, scores) {
  if (!schedule || !schedule.divisions || !schedule.weeks) return {};

  // Accumulate stats per team (keyed by team name, division-label-agnostic)
  const teamStats = {};
  schedule.divisions.forEach(div => {
    div.teams.forEach(team => {
      teamStats[team] = { team, GP: 0, W: 0, L: 0, PF: 0, PA: 0, Pts: 0 };
    });
  });

  schedule.weeks.forEach(week => {
    week.matches.forEach((match, matchIndex) => {
      const key = `week_${week.weekNum}_match_${matchIndex}`;
      const score = (scores || {})[key];
      if (!score || score.scoreA === '' || score.scoreA == null) return;
      if (score.scoreB === '' || score.scoreB == null) return;

      const { teamA, teamB } = match;
      const stA = teamStats[teamA];
      const stB = teamStats[teamB];
      if (!stA || !stB) return;

      const sA = Number(score.scoreA);
      const sB = Number(score.scoreB);

      stA.GP++; stB.GP++;
      stA.PF += sA; stA.PA += sB;
      stB.PF += sB; stB.PA += sA;

      if (sA > sB) {
        stA.W++; stA.Pts += 2;
        stB.L++;
      } else if (sB > sA) {
        stB.W++; stB.Pts += 2;
        stA.L++;
      }
    });
  });

  // Group by CURRENT division membership (post-relegation)
  const result = {};
  schedule.divisions.forEach(div => {
    result[div.name] = div.teams
      .map(team => teamStats[team] || { team, GP: 0, W: 0, L: 0, PF: 0, PA: 0, Pts: 0 })
      .sort((a, b) => {
        if (b.Pts !== a.Pts) return b.Pts - a.Pts;
        if (b.W !== a.W) return b.W - a.W;
        return (b.PF - b.PA) - (a.PF - a.PA);
      });
  });

  return result;
}

/**
 * Build a seed order from standings.
 * 1st from each div → 2nd from each div → etc. Within same rank, sort by Pts desc.
 */
function seedTeamsFromStandings(standings) {
  const divNames = Object.keys(standings).sort((a, b) => {
    const numA = parseInt(a.replace(/\D/g, ''), 10) || 0;
    const numB = parseInt(b.replace(/\D/g, ''), 10) || 0;
    return numA - numB;
  });
  if (divNames.length === 0) return [];

  const maxSize = Math.max(...divNames.map(d => standings[d].length));
  const seeds = [];
  for (let pos = 0; pos < maxSize; pos++) {
    const atRank = divNames
      .filter(d => pos < standings[d].length)
      .map(d => standings[d][pos])
      .sort((a, b) => b.Pts - a.Pts);
    atRank.forEach(e => seeds.push(e.team));
  }
  return seeds;
}

// ---------------------------------------------------------------------------
// Division Building
// ---------------------------------------------------------------------------

/**
 * Divide an array of team names into divisions of 3 or 4.
 */
function buildDivisions(teams) {
  const n = teams.length;
  const base = Math.floor(n / 3);
  const rem = n % 3;

  const numDivs = base;

  if (numDivs === 0) {
    return [teams.slice()];
  }

  const divisions = [];
  for (let i = 0; i < numDivs; i++) {
    divisions.push(teams.slice(i * 3, i * 3 + 3));
  }

  if (rem === 1) {
    divisions[numDivs - 1].push(teams[n - 1]);
  } else if (rem === 2) {
    divisions[numDivs - 2].push(teams[n - 2]);
    divisions[numDivs - 1].push(teams[n - 1]);
  }

  return divisions;
}

// ---------------------------------------------------------------------------
// Match Generation per Division
// ---------------------------------------------------------------------------

/**
 * Generate all round-robin match pairs for a set of teams.
 */
function roundRobinPairs(teams) {
  const pairs = [];
  for (let i = 0; i < teams.length; i++) {
    for (let j = i + 1; j < teams.length; j++) {
      pairs.push([teams[i], teams[j]]);
    }
  }
  return pairs;
}

// ---------------------------------------------------------------------------
// Time Slot Utilities
// ---------------------------------------------------------------------------

function parseTime(timeStr) {
  const [h, m] = timeStr.split(':').map(Number);
  return h * 60 + m;
}

function formatTime(totalMinutes) {
  const h = Math.floor(totalMinutes / 60) % 24;
  const m = totalMinutes % 60;
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
}

// ---------------------------------------------------------------------------
// Court Assignment
// ---------------------------------------------------------------------------

/**
 * Assign time slots, venues, and courts to a list of unscheduled matches.
 */
function assignCourts(matches, venues, startTime, slotDuration) {
  const courtSlots = [];
  for (const venue of venues) {
    for (let c = 1; c <= venue.courts; c++) {
      courtSlots.push({ venue: venue.name, court: c });
    }
  }
  const totalCourts = courtSlots.length;

  const startMinutes = parseTime(startTime);
  const scheduled = [];

  for (let i = 0; i < matches.length; i++) {
    const slotIndex = Math.floor(i / totalCourts);
    const courtIndex = i % totalCourts;
    const timeMinutes = startMinutes + slotIndex * slotDuration;

    scheduled.push({
      ...matches[i],
      time: formatTime(timeMinutes),
      venue: courtSlots[courtIndex].venue,
      court: courtSlots[courtIndex].court,
    });
  }

  return scheduled;
}

// ---------------------------------------------------------------------------
// Weekly Schedule Builder (Divisional mode)
// ---------------------------------------------------------------------------

function div3MatchOrder(teams, weekNum) {
  const [A, B, C] = teams;
  const orderings = [
    [[A, B], [B, C], [A, C]],
    [[A, C], [A, B], [B, C]],
    [[B, C], [A, C], [A, B]],
  ];
  return orderings[(weekNum - 1) % 3];
}

function buildWeekSchedule(divisions, weekNum, date, venues, startTime, slotDuration) {
  const divMatchLists = divisions.map((teams, divIdx) => {
    const divName = `Division ${divIdx + 1}`;
    if (teams.length === 3) {
      return div3MatchOrder(teams, weekNum).map(([tA, tB]) => ({
        division: divName,
        teamA: tA,
        teamB: tB,
      }));
    } else {
      return roundRobinPairs(teams).map(([tA, tB]) => ({
        division: divName,
        teamA: tA,
        teamB: tB,
      }));
    }
  });

  const totalCourts = venues.reduce((sum, v) => sum + v.courts, 0);
  const divChunks = [];
  for (let i = 0; i < divMatchLists.length; i += totalCourts) {
    divChunks.push(divMatchLists.slice(i, i + totalCourts));
  }

  const interleaved = [];
  for (const chunk of divChunks) {
    const maxLen = Math.max(...chunk.map(l => l.length));
    for (let i = 0; i < maxLen; i++) {
      for (const list of chunk) {
        if (i < list.length) {
          interleaved.push(list[i]);
        }
      }
    }
  }

  const scheduled = assignCourts(interleaved, venues, startTime, slotDuration);
  return scheduled.map(m => ({ ...m, date }));
}

// ---------------------------------------------------------------------------
// Playoff Builder
// ---------------------------------------------------------------------------

function seedTeams(divisions) {
  const seeds = [];
  const maxSize = Math.max(...divisions.map(d => d.length));
  for (let pos = 0; pos < maxSize; pos++) {
    for (const div of divisions) {
      if (pos < div.length) {
        seeds.push(div[pos]);
      }
    }
  }
  return seeds;
}

function buildBracket(seeds, bracketNum) {
  const label = `Bracket ${bracketNum}`;

  const qf1 = { round: 'QF', division: label, teamA: seeds[2], teamB: seeds[5], note: '' };
  const qf2 = { round: 'QF', division: label, teamA: seeds[3], teamB: seeds[4], note: '' };

  const sf1 = {
    round: 'SF',
    division: label,
    teamA: seeds[0],
    teamB: `Winner of ${seeds[2]} vs ${seeds[5]}`,
    note: 'Seed 1 vs QF1 winner',
  };
  const sf2 = {
    round: 'SF',
    division: label,
    teamA: seeds[1],
    teamB: `Winner of ${seeds[3]} vs ${seeds[4]}`,
    note: 'Seed 2 vs QF2 winner',
  };

  const final = {
    round: 'Final',
    division: label,
    teamA: `Winner SF1 (${seeds[0]} side)`,
    teamB: `Winner SF2 (${seeds[1]} side)`,
    note: 'Championship',
  };

  const thirdPlace = {
    round: '3rd Place',
    division: label,
    teamA: `Loser SF1 (${seeds[0]} side)`,
    teamB: `Loser SF2 (${seeds[1]} side)`,
    note: '3rd place match',
  };

  const fifthPlace = {
    round: '5th Place',
    division: label,
    teamA: seeds[2],
    teamB: seeds[3],
    note: '5th place match (QF losers)',
  };

  return {
    qf: [qf1, qf2],
    sf: [sf1, sf2],
    final: [final],
    consolation: [thirdPlace, fifthPlace],
  };
}

function buildPlayoffs(divisions, date1, date2, venues, startTime, slotDuration, overrideSeeds = null) {
  const seeds = overrideSeeds || seedTeams(divisions);
  const n = seeds.length;

  const brackets = [];
  let i = 0;
  while (i < n) {
    const remaining = n - i;
    if (remaining >= 6) {
      brackets.push(seeds.slice(i, i + 6));
      i += 6;
    } else {
      if (brackets.length > 0) {
        brackets[brackets.length - 1].push(...seeds.slice(i));
      } else {
        brackets.push(seeds.slice(i));
      }
      break;
    }
  }

  const builtBrackets = brackets.map((bracketSeeds, idx) =>
    buildBracket(bracketSeeds, idx + 1)
  );

  const totalCourts = venues.reduce((sum, v) => sum + v.courts, 0);
  const allQFs = builtBrackets.flatMap(b => b.qf);
  const allSFs = builtBrackets.flatMap(b => b.sf);

  const qfSlots = Math.ceil(allQFs.length / totalCourts);
  const startMinutes = parseTime(startTime);
  const sfStartTime = formatTime(startMinutes + qfSlots * slotDuration);

  const scheduledQFs = assignCourts(allQFs, venues, startTime, slotDuration);
  const scheduledSFs = assignCourts(allSFs, venues, sfStartTime, slotDuration);

  const night1Matches = [
    ...scheduledQFs.map(m => ({ ...m, date: date1 })),
    ...scheduledSFs.map(m => ({ ...m, date: date1 })),
  ];

  const finalMatches = builtBrackets.flatMap(b => [...b.final, ...b.consolation]);
  const night2Scheduled = assignCourts(finalMatches, venues, startTime, slotDuration);
  const night2Matches = night2Scheduled.map(m => ({ ...m, date: date2 }));

  return {
    night1: { date: date1, matches: night1Matches },
    night2: { date: date2, matches: night2Matches },
  };
}

// ---------------------------------------------------------------------------
// Date Utilities
// ---------------------------------------------------------------------------

function addWeeks(dateStr, weeks) {
  const d = new Date(dateStr + 'T00:00:00');
  d.setDate(d.getDate() + weeks * 7);
  return d.toISOString().slice(0, 10);
}

/**
 * Compute all game night dates for a season, skipping blackout dates.
 * Advances week-by-week from seasonStartDate, skipping any date in blackoutDates.
 *
 * @param {string}   seasonStartDate - "YYYY-MM-DD"
 * @param {number}   numWeeks
 * @param {string[]} blackoutDates   - ["YYYY-MM-DD", ...]
 * @returns {string[]} ordered list of numWeeks game night dates
 */
function computeGameNightDates(seasonStartDate, numWeeks, blackoutDates = []) {
  const dates = [];
  let offset = 0;
  while (dates.length < numWeeks) {
    const candidate = addWeeks(seasonStartDate, offset);
    offset++;
    if (!blackoutDates.includes(candidate)) {
      dates.push(candidate);
    }
  }
  return dates;
}

// ---------------------------------------------------------------------------
// All-vs-All Round Robin Scheduling
// ---------------------------------------------------------------------------

/**
 * Generate all rounds of a round-robin tournament using the circle method.
 * For N teams (N even): N-1 rounds, each with N/2 pairs.
 * For N teams (N odd): N rounds, each with (N-1)/2 pairs (one team gets a bye per round).
 *
 * @param {string[]} teams
 * @returns {[string, string][][]} array of rounds, each round = array of [teamA, teamB] pairs
 */
function generateCircleRounds(teams) {
  let list = [...teams];
  const hasBye = list.length % 2 === 1;
  if (hasBye) list.push(null); // null = bye slot
  const n = list.length;
  const numRounds = n - 1;
  const rotating = list.slice(1);
  const rounds = [];

  for (let r = 0; r < numRounds; r++) {
    const round = [];
    const fixed = list[0];
    // Rotate the non-fixed teams by r positions
    const rot = [
      ...rotating.slice(r % rotating.length),
      ...rotating.slice(0, r % rotating.length),
    ];
    const half = n / 2;

    // Pair fixed with rot[half-1]
    const partner = rot[half - 1];
    if (fixed !== null && partner !== null) {
      round.push([fixed, partner]);
    }

    // Pair rot[i] with rot[n-2-i] for i in 0..half-2
    for (let i = 0; i < half - 1; i++) {
      const a = rot[i];
      const b = rot[n - 2 - i];
      if (a !== null && b !== null) {
        round.push([a, b]);
      }
    }
    rounds.push(round);
  }
  return rounds;
}

/**
 * Check whether a team has a conflict on a given date at a given time.
 *
 * @param {string} team
 * @param {string} date  - "YYYY-MM-DD"
 * @param {string} time  - "HH:MM"
 * @param {object} teamConflicts - { "Team": { global: ["HH:MM"], dates: { "YYYY-MM-DD": ["HH:MM"] } } }
 */
function hasTeamConflict(team, date, time, teamConflicts) {
  if (!teamConflicts || !teamConflicts[team]) return false;
  const tc = teamConflicts[team];
  if (tc.global && tc.global.includes(time)) return true;
  if (tc.dates && tc.dates[date] && tc.dates[date].includes(time)) return true;
  return false;
}

/**
 * Score a round (array of pairs) by the sum of existing matchCounts for those pairs.
 * Lower score = less-played pairs = higher priority.
 */
function scoreRound(round, matchCounts) {
  return round.reduce((sum, [a, b]) => {
    const key = [a, b].sort().join('|');
    return sum + (matchCounts[key] || 0);
  }, 0);
}

/**
 * Build the All-vs-All schedule for a single game night.
 *
 * Strategy:
 * - Select 2 * totalCourts active teams (those with the most rest time get priority)
 * - Generate 2 disjoint perfect matchings for those teams using the circle method
 * - Assign slot 1 = first matching, slot 2 = second matching (back-to-back)
 * - Teams not selected rest for this night
 *
 * @param {string[]} teams
 * @param {string}   date
 * @param {object[]} venues
 * @param {string}   startTime
 * @param {number}   slotDuration
 * @param {object}   matchCounts  - mutable: { "A|B": count }
 * @param {object}   restCounts   - mutable: { "Team": count }
 * @param {object}   teamConflicts
 * @returns {object[]} scheduled matches for this night
 */
function buildAllVsAllNight(teams, date, venues, startTime, slotDuration, matchCounts, restCounts, teamConflicts) {
  const totalCourts = venues.reduce((s, v) => s + v.courts, 0);
  const N = teams.length;

  const slot1Time = startTime;
  const slot2Time = formatTime(parseTime(startTime) + slotDuration);

  // Separate teams that are fully blocked (conflict in both slots) from those that can play
  const mustRest = [];
  const canPlay = [];
  for (const team of teams) {
    const c1 = hasTeamConflict(team, date, slot1Time, teamConflicts);
    const c2 = hasTeamConflict(team, date, slot2Time, teamConflicts);
    if (c1 && c2) {
      mustRest.push(team);
    } else {
      canPlay.push(team);
    }
  }

  // Sort canPlay: teams with more rest time play first (fairness)
  canPlay.sort((a, b) => {
    const rd = (restCounts[b] || 0) - (restCounts[a] || 0);
    if (rd !== 0) return rd;
    // Tiebreak: fewer total matches played
    const aMatches = Object.entries(matchCounts)
      .filter(([k]) => k.split('|').includes(a))
      .reduce((s, [, v]) => s + v, 0);
    const bMatches = Object.entries(matchCounts)
      .filter(([k]) => k.split('|').includes(b))
      .reduce((s, [, v]) => s + v, 0);
    return aMatches - bMatches;
  });

  // Active teams: need an even number, at most 2 * totalCourts
  let numActive = Math.min(canPlay.length, totalCourts * 2);
  if (numActive % 2 !== 0) numActive--; // keep it even for perfect matchings

  const activeTeams = canPlay.slice(0, numActive);
  const restingFromCap = canPlay.slice(numActive);

  // Update rest counts
  [...mustRest, ...restingFromCap].forEach(t => {
    restCounts[t] = (restCounts[t] || 0) + 1;
  });

  if (activeTeams.length < 2) {
    // Degenerate case: not enough teams to play
    return [];
  }

  // Generate all rounds for the active teams (their own sub-tournament)
  const allRounds = generateCircleRounds(activeTeams);

  if (allRounds.length < 2) {
    // Only 2 active teams: one match total per night
    const round = allRounds[0] || [];
    const [a, b] = round[0] || [activeTeams[0], activeTeams[1]];
    const key = [a, b].sort().join('|');
    matchCounts[key] = (matchCounts[key] || 0) + 1;
    const assigned = assignCourts(
      [{ division: 'All vs All', teamA: a, teamB: b }],
      venues, startTime, slotDuration
    );
    return assigned.map(m => ({ ...m, date }));
  }

  // Pick best round for slot 1: fewest already-played pairs
  let bestIdx1 = 0;
  let bestScore1 = scoreRound(allRounds[0], matchCounts);
  for (let i = 1; i < allRounds.length; i++) {
    const s = scoreRound(allRounds[i], matchCounts);
    if (s < bestScore1) { bestScore1 = s; bestIdx1 = i; }
  }

  // Pick best round for slot 2: different from slot 1, fewest already-played
  let bestIdx2 = bestIdx1 === 0 ? 1 : 0;
  let bestScore2 = scoreRound(allRounds[bestIdx2], matchCounts);
  for (let i = 0; i < allRounds.length; i++) {
    if (i === bestIdx1) continue;
    const s = scoreRound(allRounds[i], matchCounts);
    if (s < bestScore2) { bestScore2 = s; bestIdx2 = i; }
  }

  const slot1Pairs = allRounds[bestIdx1];
  const slot2Pairs = allRounds[bestIdx2];

  // Update matchCounts
  [...slot1Pairs, ...slot2Pairs].forEach(([a, b]) => {
    const key = [a, b].sort().join('|');
    matchCounts[key] = (matchCounts[key] || 0) + 1;
  });

  // Build match objects: slot 1 first, then slot 2
  const courtSlots = [];
  for (const v of venues) {
    for (let c = 1; c <= v.courts; c++) {
      courtSlots.push({ venue: v.name, court: c });
    }
  }

  const matches = [];

  // Slot 1 matches
  slot1Pairs.slice(0, totalCourts).forEach(([a, b], i) => {
    matches.push({
      division: 'All vs All',
      teamA: a,
      teamB: b,
      time: slot1Time,
      venue: courtSlots[i % courtSlots.length].venue,
      court: courtSlots[i % courtSlots.length].court,
      date,
    });
  });

  // Slot 2 matches
  slot2Pairs.slice(0, totalCourts).forEach(([a, b], i) => {
    matches.push({
      division: 'All vs All',
      teamA: a,
      teamB: b,
      time: slot2Time,
      venue: courtSlots[i % courtSlots.length].venue,
      court: courtSlots[i % courtSlots.length].court,
      date,
    });
  });

  return matches;
}

/**
 * Build the full All-vs-All season schedule.
 */
function buildAllVsAllSchedule(teams, numWeeks, gameNightDates, venues, startTime, slotDuration, teamConflicts) {
  const matchCounts = {};
  const restCounts = {};
  teams.forEach(t => { restCounts[t] = 0; });

  const weeks = [];
  for (let w = 1; w <= numWeeks; w++) {
    const date = gameNightDates[w - 1];
    const matches = buildAllVsAllNight(
      teams, date, venues, startTime, slotDuration,
      matchCounts, restCounts, teamConflicts
    );

    // Build rest note
    const restingTeams = teams.filter(t => !matches.some(m => m.teamA === t || m.teamB === t));
    const note = restingTeams.length > 0
      ? `Resting this week: ${restingTeams.join(', ')}`
      : 'All teams active this week';

    weeks.push({
      weekNum: w,
      date,
      type: 'regular',
      note,
      matches,
    });
  }

  return { weeks, matchCounts, restCounts };
}

// ---------------------------------------------------------------------------
// API: Generate Schedule
// ---------------------------------------------------------------------------

/**
 * POST /api/generate
 * Accepts a league configuration object and returns a fully generated schedule.
 */
app.post('/api/generate', (req, res) => {
  try {
    const {
      teams,
      seasonStartDate,
      numWeeks = 4,
      playoffDate1,
      playoffDate2,
      venues = [
        { name: 'Venue 1', courts: 2 },
        { name: 'Venue 2', courts: 2 },
      ],
      slotDuration = 35,
      startTime = '18:30',
      leagueName = 'Volleyball League',
      scheduleFormat = 'Divisional with Relegation',
      teamConflicts = {},
      blackoutDates = [],
    } = req.body;

    if (!teams || !Array.isArray(teams) || teams.length < 3) {
      return res.status(400).json({ error: 'At least 3 teams are required.' });
    }
    if (!seasonStartDate) {
      return res.status(400).json({ error: 'seasonStartDate is required.' });
    }

    // Compute game night dates (skip blackout dates)
    const gameNightDates = computeGameNightDates(seasonStartDate, numWeeks, blackoutDates);

    if (scheduleFormat === 'All-vs-All Round Robin') {
      // All-vs-All mode: one flat division, no relegation
      const { weeks, matchCounts, restCounts } = buildAllVsAllSchedule(
        teams, numWeeks, gameNightDates, venues, startTime, slotDuration, teamConflicts
      );

      return res.json({
        leagueName,
        scheduleFormat,
        divisions: [{ name: 'All Teams', teams }],
        weeks,
        playoffs: null,
        matchCounts,
        restCounts,
      });
    }

    // --- Divisional with Relegation (existing logic) ---
    const divisions = buildDivisions(teams);

    const weeks = [];
    for (let w = 1; w <= numWeeks; w++) {
      const date = gameNightDates[w - 1];
      const matches = buildWeekSchedule(divisions, w, date, venues, startTime, slotDuration);
      weeks.push({
        weekNum: w,
        date,
        type: 'regular',
        note:
          w === 1
            ? 'Initial division assignments used. Week 2+ divisions update after results are entered.'
            : `Week ${w} — divisions based on initial seeding (update after results)`,
        matches,
      });
    }

    let playoffs = null;
    if (playoffDate1 && playoffDate2) {
      playoffs = buildPlayoffs(divisions, playoffDate1, playoffDate2, venues, startTime, slotDuration);
    }

    res.json({
      leagueName,
      scheduleFormat,
      divisions: divisions.map((teams, i) => ({
        name: `Division ${i + 1}`,
        teams,
      })),
      weeks,
      playoffs,
    });
  } catch (err) {
    console.error('Error generating schedule:', err);
    res.status(500).json({ error: 'Failed to generate schedule: ' + err.message });
  }
});

// ---------------------------------------------------------------------------
// API: State Persistence
// ---------------------------------------------------------------------------

app.post('/api/state/save', async (req, res) => {
  try {
    const state = req.body;
    if (!state || !state.schedule) {
      if (redis) {
        await redis.del(STATE_KEY);
      } else if (fs.existsSync(STATE_FILE)) {
        fs.unlinkSync(STATE_FILE);
      }
      return res.json({ ok: true, cleared: true });
    }
    await persistState(state);
    res.json({ ok: true });
  } catch (err) {
    console.error('Error saving state:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/state/load', async (req, res) => {
  const state = await loadStateFromFile();
  if (!state || !state.schedule) return res.json(null);
  state.standings = computeStandings(state.schedule, state.scores || {});
  res.json(state);
});

/**
 * POST /api/scores
 */
app.post('/api/scores', async (req, res) => {
  try {
    const { weekNum, matchIndex, scoreA, scoreB, updates } = req.body;

    let state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found. Generate a schedule first.' });
    }

    if (!state.scores) state.scores = {};

    const batch = updates && Array.isArray(updates)
      ? updates
      : [{ matchIndex, scoreA, scoreB }];

    for (const upd of batch) {
      const key = `week_${weekNum}_match_${upd.matchIndex}`;
      state.scores[key] = { scoreA: Number(upd.scoreA), scoreB: Number(upd.scoreB) };
    }

    const standings = computeStandings(state.schedule, state.scores);

    if (state.generateConfig) {
      const seeds = seedTeamsFromStandings(standings);
      const { playoffDate1, playoffDate2, venues, startTime, slotDuration } = state.generateConfig;
      if (playoffDate1 && playoffDate2 && seeds.length >= 3) {
        state.schedule.playoffs = buildPlayoffs(
          state.schedule.divisions.map(d => d.teams),
          playoffDate1, playoffDate2,
          venues || [{ name: 'Venue 1', courts: 2 }, { name: 'Venue 2', courts: 2 }],
          startTime || '18:30',
          slotDuration || 35,
          seeds
        );
      }
    }

    await persistState(state);
    res.json({ ok: true, standings, playoffs: state.schedule.playoffs || null });
  } catch (err) {
    console.error('Error saving score:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/standings', async (req, res) => {
  const state = await loadStateFromFile();
  if (!state || !state.schedule) return res.json({});
  res.json(computeStandings(state.schedule, state.scores || {}));
});

// ---------------------------------------------------------------------------
// API: Finalize Week + Relegation
// ---------------------------------------------------------------------------

app.post('/api/finalize-week', async (req, res) => {
  try {
    const { weekNum } = req.body;

    let state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found. Generate a schedule first.' });
    }

    if (!state.finalizedWeeks) state.finalizedWeeks = [];
    if (state.finalizedWeeks.includes(weekNum)) {
      return res.status(400).json({ error: `Week ${weekNum} is already finalized.` });
    }

    const divisions = state.schedule.divisions;

    if (divisions.length <= 1) {
      return res.status(400).json({ error: 'Single division — no relegation possible.' });
    }

    const week = state.schedule.weeks.find(w => w.weekNum === weekNum);
    if (!week) {
      return res.status(400).json({ error: `Week ${weekNum} not found.` });
    }

    const scores = state.scores || {};
    const missing = [];
    week.matches.forEach((match, idx) => {
      const key = `week_${weekNum}_match_${idx}`;
      const score = scores[key];
      if (!score || score.scoreA === '' || score.scoreA == null ||
          score.scoreB === '' || score.scoreB == null) {
        missing.push(`${match.teamA} vs ${match.teamB}`);
      }
    });
    if (missing.length > 0) {
      return res.status(400).json({ error: `Missing scores for: ${missing.join('; ')}` });
    }

    const standings = computeStandings(state.schedule, scores);

    const promotions = [];
    const relegations = [];

    for (let i = 0; i < divisions.length - 1; i++) {
      const higherDiv = divisions[i];
      const lowerDiv  = divisions[i + 1];

      const higherRows = standings[higherDiv.name] || [];
      const lowerRows  = standings[lowerDiv.name]  || [];
      if (higherRows.length === 0 || lowerRows.length === 0) continue;

      const relegatedTeam = higherRows[higherRows.length - 1].team;
      const promotedTeam  = lowerRows[0].team;

      const hiIdx = higherDiv.teams.indexOf(relegatedTeam);
      const loIdx = lowerDiv.teams.indexOf(promotedTeam);

      if (hiIdx !== -1 && loIdx !== -1) {
        higherDiv.teams[hiIdx] = promotedTeam;
        lowerDiv.teams[loIdx]  = relegatedTeam;
        promotions.push({ promoted: promotedTeam, from: lowerDiv.name,  to: higherDiv.name });
        relegations.push({ relegated: relegatedTeam, from: higherDiv.name, to: lowerDiv.name });
      }
    }

    const cfg          = state.generateConfig || {};
    const venues       = cfg.venues       || [{ name: 'Venue 1', courts: 2 }, { name: 'Venue 2', courts: 2 }];
    const startTime    = cfg.startTime    || '18:30';
    const slotDuration = cfg.slotDuration || 35;
    const divTeams     = divisions.map(d => d.teams);

    const updatedWeeks = [];
    state.schedule.weeks = state.schedule.weeks.map(w => {
      if (w.weekNum <= weekNum) return w;
      const newMatches = buildWeekSchedule(divTeams, w.weekNum, w.date, venues, startTime, slotDuration);
      const rebuilt = { ...w, matches: newMatches };
      updatedWeeks.push(rebuilt);
      return rebuilt;
    });

    if (state.scores) {
      Object.keys(state.scores).forEach(key => {
        const m = key.match(/^week_(\d+)_/);
        if (m && parseInt(m[1], 10) > weekNum) delete state.scores[key];
      });
    }

    state.finalizedWeeks.push(weekNum);

    await persistState(state);

    res.json({ ok: true, promotions, relegations, updatedWeeks });

  } catch (err) {
    console.error('Error finalizing week:', err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// API: Reschedule Night (Court Reduction Tool)
// ---------------------------------------------------------------------------

/**
 * POST /api/reschedule-night
 * Body: { weekNum, newCourtCount }
 *
 * Reshuffles a game night to fit fewer courts. Keeps matchups intact.
 * Best-effort back-to-back: if fewer courts than before, warns about rest slots.
 */
app.post('/api/reschedule-night', async (req, res) => {
  try {
    const { weekNum, newCourtCount } = req.body;

    if (!weekNum || !newCourtCount || newCourtCount < 1) {
      return res.status(400).json({ error: 'weekNum and newCourtCount (≥1) are required.' });
    }

    let state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found. Generate a schedule first.' });
    }

    const weekIdx = state.schedule.weeks.findIndex(w => w.weekNum === weekNum);
    if (weekIdx === -1) {
      return res.status(400).json({ error: `Week ${weekNum} not found.` });
    }

    const week = state.schedule.weeks[weekIdx];
    const cfg = state.generateConfig || {};
    const venues = cfg.venues || [{ name: 'Venue 1', courts: 2 }, { name: 'Venue 2', courts: 2 }];
    const startTime = cfg.startTime || '18:30';
    const slotDuration = cfg.slotDuration || 35;

    // Extract matchup data only (strip scheduling fields)
    const matchups = week.matches.map(m => ({
      division: m.division,
      teamA: m.teamA,
      teamB: m.teamB,
      round: m.round,
      note: m.note,
    }));

    // Build a reduced venue list with exactly newCourtCount total courts
    const reducedVenues = [];
    let courtsLeft = newCourtCount;
    for (const v of venues) {
      if (courtsLeft <= 0) break;
      const courts = Math.min(v.courts, courtsLeft);
      reducedVenues.push({ name: v.name, courts });
      courtsLeft -= courts;
    }
    // If original venues didn't have enough, use the first venue
    if (reducedVenues.length === 0) {
      reducedVenues.push({ name: venues[0]?.name || 'Venue 1', courts: newCourtCount });
    }

    // Re-assign courts/times
    const rescheduled = assignCourts(matchups, reducedVenues, startTime, slotDuration);
    const newMatches = rescheduled.map(m => ({ ...m, date: week.date }));

    // Determine warnings
    const warnings = [];
    const originalCourts = venues.reduce((s, v) => s + v.courts, 0);
    if (newCourtCount < originalCourts) {
      const originalSlots = 2; // assumed back-to-back
      const newSlots = Math.ceil(matchups.length / newCourtCount);
      if (newSlots > originalSlots) {
        warnings.push(`Night extended from ${originalSlots} to ${newSlots} time slots due to fewer courts.`);
        // Find teams whose matches are now non-consecutive
        const teamSlots = {};
        newMatches.forEach(m => {
          [m.teamA, m.teamB].forEach(t => {
            if (!teamSlots[t]) teamSlots[t] = [];
            teamSlots[t].push(m.time);
          });
        });
        const brokenBackToBack = Object.entries(teamSlots)
          .filter(([, times]) => times.length === 2 && times[0] !== times[1])
          .map(([team]) => team);
        if (brokenBackToBack.length > 0) {
          warnings.push(`Back-to-back may be broken for: ${brokenBackToBack.join(', ')}.`);
        }
      }
    }

    // Update state
    week.matches = newMatches;
    week.note = `Rescheduled to ${newCourtCount} court(s).` +
      (week.note ? ` (Original: ${week.note})` : '');
    state.schedule.weeks[weekIdx] = week;

    await persistState(state);
    res.json({ ok: true, week, warnings });

  } catch (err) {
    console.error('Error rescheduling night:', err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// API: Edit Individual Match
// ---------------------------------------------------------------------------

/**
 * PATCH /api/edit-match
 * Body: { weekNum, matchIndex, updates: { date?, time?, venue?, court?, teamA?, teamB? } }
 */
app.patch('/api/edit-match', async (req, res) => {
  try {
    const { weekNum, matchIndex, updates } = req.body;

    if (weekNum === undefined || matchIndex === undefined || !updates) {
      return res.status(400).json({ error: 'weekNum, matchIndex, and updates are required.' });
    }

    let state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found. Generate a schedule first.' });
    }

    const week = state.schedule.weeks.find(w => w.weekNum === weekNum);
    if (!week) {
      return res.status(400).json({ error: `Week ${weekNum} not found.` });
    }

    if (matchIndex < 0 || matchIndex >= week.matches.length) {
      return res.status(400).json({ error: `Match index ${matchIndex} out of range.` });
    }

    // Whitelist of editable fields
    const allowed = ['date', 'time', 'venue', 'court', 'teamA', 'teamB'];
    allowed.forEach(field => {
      if (updates[field] !== undefined && updates[field] !== '') {
        week.matches[matchIndex][field] = field === 'court'
          ? parseInt(updates[field], 10)
          : updates[field];
      }
    });

    await persistState(state);
    res.json({ ok: true, match: week.matches[matchIndex] });

  } catch (err) {
    console.error('Error editing match:', err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// Start Server
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`League Scheduler running at http://localhost:${PORT}`);
});
