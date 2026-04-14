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
const PDFDocument = require('pdfkit');
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
 * If divisionSizes is provided (e.g. [3,4,3]), use those exact sizes.
 */
function buildDivisions(teams, divisionSizes = null) {
  if (divisionSizes && Array.isArray(divisionSizes) && divisionSizes.length > 0) {
    const divisions = [];
    let offset = 0;
    for (const size of divisionSizes) {
      divisions.push(teams.slice(offset, offset + size));
      offset += size;
    }
    return divisions;
  }

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

/**
 * Check for simultaneous court conflicts (same team in two matches at the same time).
 * Returns array of conflict descriptions, or empty array if clean.
 */
function checkDoubleBooking(matches) {
  const conflicts = [];
  const byTime = {};
  matches.forEach(m => {
    const t = m.time;
    if (!byTime[t]) byTime[t] = [];
    byTime[t].push(m);
  });
  for (const [time, ms] of Object.entries(byTime)) {
    const seen = {};
    for (const m of ms) {
      for (const team of [m.teamA, m.teamB]) {
        if (seen[team]) {
          conflicts.push(`${team} is double-booked at ${time} (on ${m.date || 'unknown date'})`);
        }
        seen[team] = true;
      }
    }
  }
  return conflicts;
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

/**
 * Compute per-venue start times from venueWeekParity.
 * Even parity = early (startTime), Odd parity = late (startTime + 3*slotDuration).
 */
function computeVenueTimes(venues, venueWeekParity, startTime, slotDuration) {
  const startMins = parseTime(startTime);
  const lateOffset = 3 * slotDuration;
  const venueTimes = {};
  for (const venue of venues) {
    const parity = (venueWeekParity || {})[venue.name] != null
      ? (venueWeekParity || {})[venue.name]
      : 0;
    venueTimes[venue.name] = parity % 2 === 0
      ? formatTime(startMins)
      : formatTime(startMins + lateOffset);
  }
  return venueTimes;
}

// ---------------------------------------------------------------------------
// Court Assignment
// ---------------------------------------------------------------------------

/**
 * Assign time slots, venues, and courts to a list of unscheduled matches.
 * Optional venueTimes map { "Venue 1": "18:30", "Venue 2": "20:15" } for
 * per-venue start times (Feature D4: alternating early/late blocks).
 */
function assignCourts(matches, venues, startTime, slotDuration, venueTimes) {
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
    const venueName = courtSlots[courtIndex].venue;
    const venueStart = (venueTimes && venueTimes[venueName] != null)
      ? parseTime(venueTimes[venueName])
      : startMinutes;
    const timeMinutes = venueStart + slotIndex * slotDuration;

    scheduled.push({
      ...matches[i],
      time: formatTime(timeMinutes),
      venue: venueName,
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

/**
 * Build match order for a 3-team division when a team was demoted into it (Feature D2).
 * A = demoted, B = promoted, C = stayed.
 * Slot 1: A vs C, Slot 2: B vs C, Slot 3: A vs B
 */
function div3DemotionMatchOrder(divName, lastMoves) {
  const demoted  = lastMoves.find(m => m.division === divName && m.action === 'demoted');
  const promoted = lastMoves.find(m => m.division === divName && m.action === 'promoted');
  const stayed   = lastMoves.find(m => m.division === divName && m.action === 'stayed');
  if (!demoted || !promoted || !stayed) return null;
  const A = demoted.team, B = promoted.team, C = stayed.team;
  return [
    { division: divName, teamA: A, teamB: C },
    { division: divName, teamA: B, teamB: C },
    { division: divName, teamA: A, teamB: B },
  ];
}

/**
 * Build one week's full match schedule for all divisions.
 * lastMoves  – optional array of { team, action, division } from Feature D2
 * venueTimes – optional map { "Venue 1": "HH:MM", ... } for Feature D4
 */
function buildWeekSchedule(divisions, weekNum, date, venues, startTime, slotDuration, lastMoves, venueTimes) {
  const divMatchLists = divisions.map((teams, divIdx) => {
    const divName = `Division ${divIdx + 1}`;
    if (teams.length === 3) {
      // Feature D2: check if a team was demoted into this division
      if (lastMoves && lastMoves.length > 0) {
        const order = div3DemotionMatchOrder(divName, lastMoves);
        if (order) return order;
      }
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

  const scheduled = assignCourts(interleaved, venues, startTime, slotDuration, venueTimes);
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
 * - If allowExtraMatches is true, fill slot 3 with bonus matches
 *
 * @param {string[]} teams
 * @param {string}   date
 * @param {object[]} venues
 * @param {string}   startTime
 * @param {number}   slotDuration
 * @param {object}   matchCounts        - mutable: { "A|B": count }
 * @param {object}   restCounts         - mutable: { "Team": count }
 * @param {object}   teamConflicts
 * @param {boolean}  allowExtraMatches  - fill spare slot3 court time with bonus matches
 * @param {object}   extraMatchCounts   - mutable: { "Team": count }
 * @returns {object[]} scheduled matches for this night
 */
function buildAllVsAllNight(teams, date, venues, startTime, slotDuration, matchCounts, restCounts, teamConflicts, allowExtraMatches, extraMatchCounts) {
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

  // Extra matches: fill slot 3 court time with bonus matches
  if (allowExtraMatches && extraMatchCounts) {
    const slot3Time = formatTime(parseTime(startTime) + 2 * slotDuration);

    // Track pairs and teams that already played tonight
    const pairsTonight = new Set(
      matches.map(m => [m.teamA, m.teamB].sort().join('|'))
    );
    const extraMatchTonight = new Set();

    for (let courtIdx = 0; courtIdx < totalCourts; courtIdx++) {
      // Candidates: teams not yet in an extra match tonight
      const candidates = teams.filter(t => !extraMatchTonight.has(t));
      if (candidates.length < 2) break;

      let bestPair = null;
      let bestScore = Infinity;

      for (let i = 0; i < candidates.length; i++) {
        for (let j = i + 1; j < candidates.length; j++) {
          const a = candidates[i], b = candidates[j];
          const pairKey = [a, b].sort().join('|');
          if (pairsTonight.has(pairKey)) continue; // already played tonight

          // Prioritise teams with fewest extra matches; tiebreak by matchCounts
          const extraScore = (extraMatchCounts[a] || 0) + (extraMatchCounts[b] || 0);
          const matchScore = matchCounts[pairKey] || 0;
          const totalScore = extraScore * 1000 + matchScore;

          if (totalScore < bestScore) {
            bestScore = totalScore;
            bestPair = [a, b];
          }
        }
      }

      if (!bestPair) break;

      const [a, b] = bestPair;
      const pairKey = [a, b].sort().join('|');

      matches.push({
        division: 'Extra Match',
        teamA: a,
        teamB: b,
        time: slot3Time,
        venue: courtSlots[courtIdx % courtSlots.length].venue,
        court: courtSlots[courtIdx % courtSlots.length].court,
        date,
        extraMatch: true,
      });

      extraMatchCounts[a] = (extraMatchCounts[a] || 0) + 1;
      extraMatchCounts[b] = (extraMatchCounts[b] || 0) + 1;
      matchCounts[pairKey] = (matchCounts[pairKey] || 0) + 1;
      extraMatchTonight.add(a);
      extraMatchTonight.add(b);
      pairsTonight.add(pairKey);
    }
  }

  return matches;
}

/**
 * Build the full All-vs-All season schedule.
 * @param {boolean} allowExtraMatches - fill spare slot3 court time with bonus matches
 */
function buildAllVsAllSchedule(teams, numWeeks, gameNightDates, venues, startTime, slotDuration, teamConflicts, allowExtraMatches) {
  const matchCounts = {};
  const restCounts = {};
  const extraMatchCounts = {};
  teams.forEach(t => { restCounts[t] = 0; extraMatchCounts[t] = 0; });

  const weeks = [];
  for (let w = 1; w <= numWeeks; w++) {
    const date = gameNightDates[w - 1];
    const matches = buildAllVsAllNight(
      teams, date, venues, startTime, slotDuration,
      matchCounts, restCounts, teamConflicts, allowExtraMatches, extraMatchCounts
    );

    // Build rest note (exclude extra-match-only appearances)
    const regularTeams = new Set(
      matches.filter(m => !m.extraMatch).flatMap(m => [m.teamA, m.teamB])
    );
    const restingTeams = teams.filter(t => !regularTeams.has(t));
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

  return { weeks, matchCounts, restCounts, extraMatchCounts };
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
      divisionSizes = null,
      allowExtraMatches = false,
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
      const { weeks, matchCounts, restCounts, extraMatchCounts } = buildAllVsAllSchedule(
        teams, numWeeks, gameNightDates, venues, startTime, slotDuration, teamConflicts, allowExtraMatches
      );

      return res.json({
        leagueName,
        scheduleFormat,
        divisions: [{ name: 'All Teams', teams }],
        weeks,
        playoffs: null,
        matchCounts,
        restCounts,
        extraMatchCounts,
      });
    }

    // --- Divisional with Relegation (existing logic) ---

    // Validate custom division sizes if provided
    if (divisionSizes) {
      const sizeSum = divisionSizes.reduce((s, n) => s + n, 0);
      if (sizeSum !== teams.length) {
        return res.status(400).json({
          error: `divisionSizes sum (${sizeSum}) must equal number of teams (${teams.length}).`,
        });
      }
      if (divisionSizes.some(s => s < 2)) {
        return res.status(400).json({ error: 'Each division must have at least 2 teams.' });
      }
    }

    const divisions = buildDivisions(teams, divisionSizes);

    // Feature D4: initialize venue week parity (alternating: venue[0]=early, venue[1]=late, ...)
    const venueWeekParity = {};
    venues.forEach((v, i) => { venueWeekParity[v.name] = i % 2; });
    const initVenueTimes = computeVenueTimes(venues, venueWeekParity, startTime, slotDuration);

    const weeks = [];
    for (let w = 1; w <= numWeeks; w++) {
      const date = gameNightDates[w - 1];
      const matches = buildWeekSchedule(divisions, w, date, venues, startTime, slotDuration, null, initVenueTimes);

      // Validate no team is double-booked
      const conflicts = checkDoubleBooking(matches);
      if (conflicts.length > 0) {
        return res.status(400).json({
          error: `Schedule conflict detected in week ${w}: ${conflicts.join('; ')}`,
        });
      }

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
      venueWeekParity,
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

    // Determine which divisions are inactive this week
    const inactiveDivisions = (state.inactiveDivisions || {})[`week_${weekNum}`] || [];
    const promotionRules = state.promotionRules || {};

    for (let i = 0; i < divisions.length - 1; i++) {
      const higherDiv = divisions[i];
      const lowerDiv  = divisions[i + 1];

      // Skip boundary if either adjacent division is inactive this week
      const higherInactive = inactiveDivisions.includes(higherDiv.name);
      const lowerInactive  = inactiveDivisions.includes(lowerDiv.name);
      if (higherInactive || lowerInactive) continue;

      const higherRows = standings[higherDiv.name] || [];
      const lowerRows  = standings[lowerDiv.name]  || [];
      if (higherRows.length === 0 || lowerRows.length === 0) continue;

      // Determine how many teams to move at this boundary
      const ruleKey = `${higherDiv.name}|${lowerDiv.name}`;
      const rule = promotionRules[ruleKey] || { teamsPerMove: 1, triggerEvery: 1, gamesPlayedCount: 0 };
      rule.gamesPlayedCount = (rule.gamesPlayedCount || 0) + 1;

      const shouldTrigger = rule.gamesPlayedCount % rule.triggerEvery === 0;
      const numToMove = shouldTrigger ? rule.teamsPerMove : 1;

      // Update the stored rule
      promotionRules[ruleKey] = rule;

      const maxMove = Math.min(numToMove, Math.floor(higherRows.length / 2), Math.floor(lowerRows.length / 2));
      for (let m = 0; m < maxMove; m++) {
        const relegatedTeam = higherRows[higherRows.length - 1 - m].team;
        const promotedTeam  = lowerRows[m].team;

        const hiIdx = higherDiv.teams.indexOf(relegatedTeam);
        const loIdx = lowerDiv.teams.indexOf(promotedTeam);

        if (hiIdx !== -1 && loIdx !== -1) {
          higherDiv.teams[hiIdx] = promotedTeam;
          lowerDiv.teams[loIdx]  = relegatedTeam;
          promotions.push({ promoted: promotedTeam, from: lowerDiv.name,  to: higherDiv.name });
          relegations.push({ relegated: relegatedTeam, from: higherDiv.name, to: lowerDiv.name });
        }
      }
    }

    // Persist updated promotion rules
    if (Object.keys(promotionRules).length > 0) {
      state.promotionRules = promotionRules;
    }

    // Feature 1: Inactive division internal standings reorder (sandwiched rule)
    for (let i = 0; i < divisions.length; i++) {
      const divObj = divisions[i];
      if (!inactiveDivisions.includes(divObj.name)) continue;

      const aboveActive = i > 0 && !inactiveDivisions.includes(divisions[i - 1].name);
      const belowActive = i < divisions.length - 1 && !inactiveDivisions.includes(divisions[i + 1].name);

      // Only reorder if at least one adjacent division played
      if (!aboveActive && !belowActive) continue;

      // Reorder teams by standings (Pts desc → W desc → PF-PA desc)
      const divStandings = standings[divObj.name] || [];
      const sortedTeams = divStandings.map(s => s.team);
      // Keep any teams missing from standings at end
      const missing = divObj.teams.filter(t => !sortedTeams.includes(t));
      divObj.teams = [...sortedTeams, ...missing];
    }

    // Feature 2: Build lastMoves from promotion/relegation results
    const lastMoves = [];
    for (const div of divisions) {
      for (const team of div.teams) {
        const isRelegatedHere = relegations.some(r => r.relegated === team && r.to === div.name);
        const isPromotedHere  = promotions.some(p => p.promoted === team && p.to === div.name);
        const action = isRelegatedHere ? 'demoted' : (isPromotedHere ? 'promoted' : 'stayed');
        lastMoves.push({ team, action, division: div.name });
      }
    }
    state.lastMoves = lastMoves;

    const cfg          = state.generateConfig || {};
    const venues       = cfg.venues       || [{ name: 'Venue 1', courts: 2 }, { name: 'Venue 2', courts: 2 }];
    const startTime    = cfg.startTime    || '18:30';
    const slotDuration = cfg.slotDuration || 35;
    const divTeams     = divisions.map(d => d.teams);

    // Feature 3: Flip venue week parities before rebuilding future weeks
    const currentParity = (state.schedule && state.schedule.venueWeekParity) || {};
    const newParity = {};
    venues.forEach(v => {
      const cur = currentParity[v.name] != null ? currentParity[v.name] : 0;
      newParity[v.name] = 1 - cur;
    });
    if (state.schedule) state.schedule.venueWeekParity = newParity;

    // Compute venueTimes for rebuilt future weeks
    const venueTimes = computeVenueTimes(venues, newParity, startTime, slotDuration);

    const updatedWeeks = [];
    state.schedule.weeks = state.schedule.weeks.map(w => {
      if (w.weekNum <= weekNum) return w;
      const newMatches = buildWeekSchedule(divTeams, w.weekNum, w.date, venues, startTime, slotDuration, lastMoves, venueTimes);
      const rebuilt = { ...w, matches: newMatches };
      updatedWeeks.push(rebuilt);
      return rebuilt;
    });

    // Feature 3: Compute divisionTiers from first future week's venue assignments
    const divisionTiers = {};
    const firstFutureWeek = state.schedule.weeks.find(w => w.weekNum > weekNum);
    if (firstFutureWeek) {
      for (const div of divisions) {
        const divMatch = firstFutureWeek.matches.find(m => m.division === div.name);
        if (divMatch && venueTimes) {
          const venueTime = venueTimes[divMatch.venue] || startTime;
          divisionTiers[div.name] = parseTime(venueTime) > parseTime(startTime) ? 'late' : 'early';
        } else {
          divisionTiers[div.name] = 'early';
        }
      }
    } else {
      divisions.forEach(d => { divisionTiers[d.name] = 'early'; });
    }
    state.divisionTiers = divisionTiers;

    if (state.scores) {
      Object.keys(state.scores).forEach(key => {
        const m = key.match(/^week_(\d+)_/);
        if (m && parseInt(m[1], 10) > weekNum) delete state.scores[key];
      });
    }

    state.finalizedWeeks.push(weekNum);

    await persistState(state);

    res.json({ ok: true, promotions, relegations, updatedWeeks, lastMoves });

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
// API: LeagueApps CSV Export
// ---------------------------------------------------------------------------

function formatLeagueAppsDate(dateStr) {
  if (!dateStr) return '';
  const [year, month, day] = dateStr.split('-');
  return `${month}/${day}/${year}`;
}

function formatLeagueAppsTime(timeStr) {
  if (!timeStr) return '';
  const [h, m] = timeStr.split(':').map(Number);
  const ampm = h >= 12 ? 'PM' : 'AM';
  const hour = h % 12 || 12;
  return `${hour}:${String(m).padStart(2, '0')} ${ampm}`;
}

function buildLeagueAppsCSV(state) {
  const schedule = state.schedule;
  const slotDuration = (state.generateConfig || {}).slotDuration || 35;

  const COLUMNS = ['SUB_PROGRAM','HOME_TEAM','AWAY_TEAM','DATE','START_TIME','END_TIME','LOCATION','SUB_LOCATION','TYPE','NOTES'];
  const rows = [COLUMNS];

  function addMatchRow(m, date, type, notes = '') {
    const endMinutes = parseTime(m.time || '00:00') + slotDuration;
    const endTime = formatTime(endMinutes);
    rows.push([
      schedule.leagueName || '',
      m.teamA || '',
      m.teamB || '',
      formatLeagueAppsDate(date),
      formatLeagueAppsTime(m.time),
      formatLeagueAppsTime(endTime),
      m.venue || '',
      `Court ${m.court || ''}`,
      type,
      notes,
    ]);
  }

  (schedule.weeks || []).forEach(week => {
    (week.matches || []).forEach(m => addMatchRow(m, week.date, 'REGULAR_SEASON'));
  });

  if (schedule.playoffs) {
    [schedule.playoffs.night1, schedule.playoffs.night2].forEach(night => {
      if (!night) return;
      (night.matches || []).forEach(m => addMatchRow(m, night.date, 'PLAYOFF', m.round || ''));
    });
  }

  return rows.map(row =>
    row.map(cell => {
      const s = String(cell == null ? '' : cell);
      if (s.includes(',') || s.includes('"') || s.includes('\n')) {
        return '"' + s.replace(/"/g, '""') + '"';
      }
      return s;
    }).join(',')
  ).join('\r\n');
}

app.get('/api/export/csv', async (req, res) => {
  try {
    const state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found.' });
    }
    const csv = buildLeagueAppsCSV(state);
    const filename = (state.schedule.leagueName || 'league').replace(/[^a-z0-9]/gi, '_') + '_leagueapps_import.csv';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
  } catch (err) {
    console.error('CSV export error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// API: PDF Game Sheet Export
// ---------------------------------------------------------------------------

function buildPDF(doc, title, dateStr, venueNames, matches, slotDuration) {
  const PAGE_W = doc.page.width;
  const MARGIN = 48;
  const CONTENT_W = PAGE_W - MARGIN * 2;

  // Header
  doc.fontSize(18).font('Helvetica-Bold').text(title, MARGIN, MARGIN, { width: CONTENT_W, align: 'center' });
  doc.fontSize(13).font('Helvetica').text(dateStr, MARGIN, MARGIN + 26, { width: CONTENT_W, align: 'center' });
  if (venueNames && venueNames.length) {
    doc.fontSize(11).text(venueNames.join(', '), MARGIN, MARGIN + 44, { width: CONTENT_W, align: 'center' });
  }

  doc.moveTo(MARGIN, MARGIN + 62).lineTo(PAGE_W - MARGIN, MARGIN + 62).stroke();

  // Table header
  const TABLE_TOP = MARGIN + 74;
  const COL_TIME   = MARGIN;
  const COL_COURT  = COL_TIME  + 60;
  const COL_TEAMA  = COL_COURT + 50;
  const COL_VS     = COL_TEAMA + 155;
  const COL_TEAMB  = COL_VS    + 28;
  const COL_SCORE  = COL_TEAMB + 155;

  const ROW_H = 22;

  doc.fontSize(10).font('Helvetica-Bold');
  doc.text('Time',   COL_TIME,  TABLE_TOP, { width: 55 });
  doc.text('Court',  COL_COURT, TABLE_TOP, { width: 45 });
  doc.text('Home Team',   COL_TEAMA, TABLE_TOP, { width: 150 });
  doc.text('vs',     COL_VS,    TABLE_TOP, { width: 24, align: 'center' });
  doc.text('Away Team',   COL_TEAMB, TABLE_TOP, { width: 150 });
  doc.text('Score', COL_SCORE,  TABLE_TOP);

  const headerLineY = TABLE_TOP + ROW_H - 4;
  doc.moveTo(MARGIN, headerLineY).lineTo(PAGE_W - MARGIN, headerLineY).stroke();

  doc.font('Helvetica').fontSize(9);
  let y = TABLE_TOP + ROW_H;
  matches.forEach((m, idx) => {
    if (y > doc.page.height - 150) {
      doc.addPage();
      y = MARGIN;
    }
    if (idx % 2 === 1) {
      doc.rect(MARGIN, y - 2, CONTENT_W, ROW_H).fill('#f5f5f5').stroke('#f5f5f5');
      doc.fillColor('black');
    }
    doc.text(m.time || '', COL_TIME,  y, { width: 55 });
    doc.text(String(m.court || ''), COL_COURT, y, { width: 45 });
    doc.text(m.teamA || '', COL_TEAMA, y, { width: 150 });
    doc.text('vs',     COL_VS,    y, { width: 24, align: 'center' });
    doc.text(m.teamB || '', COL_TEAMB, y, { width: 150 });
    // Score blank field
    const scoreX = COL_SCORE;
    doc.text('___', scoreX, y, { width: 20 });
    doc.text(':', scoreX + 22, y, { width: 10, align: 'center' });
    doc.text('___', scoreX + 32, y, { width: 20 });
    y += ROW_H;
  });

  // Footer: Notes / Marketing box
  const footerTop = Math.max(y + 20, doc.page.height - 160);
  doc.moveTo(MARGIN, footerTop).lineTo(PAGE_W - MARGIN, footerTop).stroke();
  doc.fontSize(10).font('Helvetica-Bold').text('Notes / Marketing', MARGIN, footerTop + 6);
  doc.rect(MARGIN, footerTop + 22, CONTENT_W, 100).stroke();
}

// GET /api/export/pdf/playoff/:nightNum  — must be defined BEFORE the :weekNum route
app.get('/api/export/pdf/playoff/:nightNum', async (req, res) => {
  try {
    const nightNum = parseInt(req.params.nightNum, 10);
    if (nightNum !== 1 && nightNum !== 2) {
      return res.status(400).json({ error: 'nightNum must be 1 or 2.' });
    }
    const state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found.' });
    }
    const playoffs = state.schedule.playoffs;
    if (!playoffs) {
      return res.status(400).json({ error: 'No playoffs in schedule.' });
    }
    const night = nightNum === 1 ? playoffs.night1 : playoffs.night2;
    if (!night) {
      return res.status(400).json({ error: `Playoff night ${nightNum} not found.` });
    }
    const cfg = state.generateConfig || {};
    const slotDuration = cfg.slotDuration || 35;
    const leagueName = (state.schedule.leagueName || 'League');
    const venues = [...new Set((night.matches || []).map(m => m.venue).filter(Boolean))];
    const doc = new PDFDocument({ size: 'LETTER', margin: 0 });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="playoff_night${nightNum}_gamesheet.pdf"`);
    doc.pipe(res);
    buildPDF(doc, `${leagueName} — Playoff Night ${nightNum}`, night.date || '', venues, night.matches || [], slotDuration);
    doc.end();
  } catch (err) {
    console.error('PDF playoff export error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/export/pdf/:weekNum
app.get('/api/export/pdf/:weekNum', async (req, res) => {
  try {
    const weekNum = parseInt(req.params.weekNum, 10);
    if (isNaN(weekNum)) {
      return res.status(400).json({ error: 'Invalid weekNum.' });
    }
    const state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found.' });
    }
    const week = (state.schedule.weeks || []).find(w => w.weekNum === weekNum);
    if (!week) {
      return res.status(400).json({ error: `Week ${weekNum} not found.` });
    }
    const cfg = state.generateConfig || {};
    const slotDuration = cfg.slotDuration || 35;
    const leagueName = (state.schedule.leagueName || 'League');
    const venues = [...new Set((week.matches || []).map(m => m.venue).filter(Boolean))];
    const doc = new PDFDocument({ size: 'LETTER', margin: 0 });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="week${weekNum}_gamesheet.pdf"`);
    doc.pipe(res);
    buildPDF(doc, `${leagueName} — Week ${weekNum}`, week.date || '', venues, week.matches || [], slotDuration);
    doc.end();
  } catch (err) {
    console.error('PDF export error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// API: Division Toggle
// ---------------------------------------------------------------------------

/**
 * POST /api/toggle-division
 * Body: { weekNum, divisionName, active: true|false }
 */
app.post('/api/toggle-division', async (req, res) => {
  try {
    const { weekNum, divisionName, active } = req.body;
    if (!weekNum || !divisionName || active === undefined) {
      return res.status(400).json({ error: 'weekNum, divisionName, and active are required.' });
    }
    let state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found.' });
    }
    if (!state.inactiveDivisions) state.inactiveDivisions = {};
    const key = `week_${weekNum}`;
    if (!state.inactiveDivisions[key]) state.inactiveDivisions[key] = [];

    if (active) {
      // Remove from inactive list
      state.inactiveDivisions[key] = state.inactiveDivisions[key].filter(d => d !== divisionName);
    } else {
      // Add to inactive list (avoid duplicates)
      if (!state.inactiveDivisions[key].includes(divisionName)) {
        state.inactiveDivisions[key].push(divisionName);
      }
    }

    await persistState(state);
    res.json({ ok: true, inactiveDivisions: state.inactiveDivisions });
  } catch (err) {
    console.error('Toggle division error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// API: Set Promotion Rule
// ---------------------------------------------------------------------------

/**
 * POST /api/set-promotion-rule
 * Body: { higherDivision, lowerDivision, teamsPerMove, triggerEvery }
 */
app.post('/api/set-promotion-rule', async (req, res) => {
  try {
    const { higherDivision, lowerDivision, teamsPerMove = 1, triggerEvery = 1 } = req.body;
    if (!higherDivision || !lowerDivision) {
      return res.status(400).json({ error: 'higherDivision and lowerDivision are required.' });
    }
    let state = await loadStateFromFile();
    if (!state || !state.schedule) {
      return res.status(400).json({ error: 'No league state found.' });
    }
    if (!state.promotionRules) state.promotionRules = {};
    const ruleKey = `${higherDivision}|${lowerDivision}`;
    state.promotionRules[ruleKey] = {
      teamsPerMove: Math.max(1, parseInt(teamsPerMove, 10) || 1),
      triggerEvery: Math.max(1, parseInt(triggerEvery, 10) || 1),
      gamesPlayedCount: (state.promotionRules[ruleKey] || {}).gamesPlayedCount || 0,
    };
    await persistState(state);
    res.json({ ok: true, promotionRules: state.promotionRules });
  } catch (err) {
    console.error('Set promotion rule error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// Start Server
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`League Scheduler running at http://localhost:${PORT}`);
});
