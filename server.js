/**
 * Volleyball League Scheduler - Server
 *
 * Express server that:
 *  - Serves the public/ directory as static files
 *  - Exposes POST /api/generate to build a full league schedule
 *
 * Run with: node server.js
 */

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Parse JSON request bodies
app.use(express.json());

// Serve frontend from public/
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// Division Building
// ---------------------------------------------------------------------------

/**
 * Divide an array of team names into divisions of 3 or 4.
 *
 * Rules (n = total teams, base = Math.floor(n/3), rem = n%3):
 *   rem == 0  →  base groups of 3
 *   rem == 1  →  (base-1) groups of 3  +  1 group of 4   (total: base groups)
 *   rem == 2  →  (base-2) groups of 3  +  2 groups of 4  (total: base groups)
 *
 * Teams are assigned in order: [0..2] → div 1, [3..5] → div 2, etc.
 * The "extra" team(s) are appended to the last group(s).
 *
 * @param {string[]} teams - ordered list of team names
 * @returns {string[][]} array of divisions, each an array of team names
 */
function buildDivisions(teams) {
  const n = teams.length;
  const base = Math.floor(n / 3);
  const rem = n % 3;

  // Number of divisions to create
  const numDivs = base;

  if (numDivs === 0) {
    // Fewer than 3 teams — put them all in one group
    return [teams.slice()];
  }

  // Start with all divisions having 3 teams
  const divisions = [];
  for (let i = 0; i < numDivs; i++) {
    divisions.push(teams.slice(i * 3, i * 3 + 3));
  }

  // Distribute remainder teams into the last division(s)
  if (rem === 1) {
    // One leftover team → append to last division → size 4
    divisions[numDivs - 1].push(teams[n - 1]);
  } else if (rem === 2) {
    // Two leftover teams → append one each to last two divisions → size 4,4
    // The "extra" teams are at indices n-2 and n-1 but we already sliced up to
    // numDivs*3 = base*3 = n-2, so teams n-2 and n-1 are unassigned.
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
 * Returns every unique pair [teamA, teamB].
 *
 * @param {string[]} teams
 * @returns {[string, string][]}
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

/**
 * Parse a "HH:MM" string into total minutes since midnight.
 *
 * @param {string} timeStr - e.g. "18:30"
 * @returns {number} minutes since midnight
 */
function parseTime(timeStr) {
  const [h, m] = timeStr.split(':').map(Number);
  return h * 60 + m;
}

/**
 * Convert total minutes since midnight back to "HH:MM" (24-hour format).
 *
 * @param {number} totalMinutes
 * @returns {string}
 */
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
 *
 * Packs up to `totalCourts` matches per time slot (round-robin across venues
 * and their courts).  Each slot is `slotDuration` minutes after the previous.
 *
 * @param {object[]} matches      - array of { division, teamA, teamB }
 * @param {object[]} venues       - array of { name, courts }
 * @param {string}   startTime    - "HH:MM"
 * @param {number}   slotDuration - minutes per slot
 * @returns {object[]} matches with time, venue, court added
 */
function assignCourts(matches, venues, startTime, slotDuration) {
  // Build a flat ordered list of [venueName, courtNumber] slots
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
    const slotIndex = Math.floor(i / totalCourts);       // which time slot
    const courtIndex = i % totalCourts;                  // which court in that slot
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
// Weekly Schedule Builder
// ---------------------------------------------------------------------------

/**
 * For a single division of 3 teams, return the 3 matches ordered so that the
 * "sit in the middle slot" team rotates by weekNum.
 *
 * With 3 teams [A, B, C] and pairs AB, BC, AC:
 *   rotation 0: C sits slot1 (AB), A sits slot2 (BC), B sits slot3 (AC)
 *   rotation 1: B sits slot1 (AC), C sits slot2 (AB), A sits slot3 (BC)
 *   rotation 2: A sits slot1 (BC), B sits slot2 (AC), C sits slot3 (AB)
 *
 * Each team's sequence over 3 weeks:
 *   A: PLAY,SIT,PLAY  → PLAY,PLAY,SIT → SIT,PLAY,PLAY
 *   B: PLAY,PLAY,SIT  → SIT,PLAY,PLAY → PLAY,SIT,PLAY
 *   C: SIT,PLAY,PLAY  → PLAY,SIT,PLAY → PLAY,PLAY,SIT
 *
 * The order of the 3 matches in the returned array maps directly to
 * time slots 1, 2, and 3 for this division.
 *
 * @param {string[]} teams   - exactly 3 teams
 * @param {number}   weekNum - 1-based week number
 * @returns {[string, string][]} ordered match pairs
 */
function div3MatchOrder(teams, weekNum) {
  const [A, B, C] = teams;
  // Base match list for rotation 0
  const orderings = [
    [[A, B], [B, C], [A, C]],  // rotation 0
    [[A, C], [A, B], [B, C]],  // rotation 1
    [[B, C], [A, C], [A, B]],  // rotation 2
  ];
  return orderings[(weekNum - 1) % 3];
}

/**
 * Build all matches for a single regular-season week across all divisions.
 *
 * Strategy:
 *  - For each division of 3: 3 matches, 1 per consecutive slot (uses 1 court
 *    for 3 slots). The sit-rotation is applied.
 *  - For each division of 4: 6 matches (full round-robin).
 *
 * All matches are collected into a flat list and then passed to assignCourts()
 * which packs 4 per time slot.
 *
 * @param {string[][]} divisions    - array of team arrays (from buildDivisions)
 * @param {number}     weekNum      - 1-based
 * @param {string}     date         - "YYYY-MM-DD"
 * @param {object[]}   venues       - [{ name, courts }]
 * @param {string}     startTime    - "HH:MM"
 * @param {number}     slotDuration - minutes
 * @returns {object[]} fully scheduled matches for the week
 */
function buildWeekSchedule(divisions, weekNum, date, venues, startTime, slotDuration) {
  // We want to interleave matches across divisions so that division courts are
  // spread across time slots nicely.  The simplest approach that satisfies the
  // "sit pattern" requirement for div-3:
  //
  //   Collect 1 match per division per slot-group.  Divisions of 3 emit one
  //   match in each of 3 consecutive slot-groups.  Divisions of 4 emit their
  //   6 matches spread across slot-groups.
  //
  // Concretely: build an ordered flat list where matches that should share the
  // same time slot (i.e. same slot-group index within their division) are
  // interleaved, then hand off to assignCourts.

  // Per-division ordered match lists
  const divMatchLists = divisions.map((teams, divIdx) => {
    const divName = `Division ${divIdx + 1}`;
    if (teams.length === 3) {
      return div3MatchOrder(teams, weekNum).map(([tA, tB]) => ({
        division: divName,
        teamA: tA,
        teamB: tB,
      }));
    } else {
      // Division of 4: generate all 6 pairs
      return roundRobinPairs(teams).map(([tA, tB]) => ({
        division: divName,
        teamA: tA,
        teamB: tB,
      }));
    }
  });

  // Interleave: take the i-th match from each division in turn.
  // This ensures matches from the same division that need different slots are
  // naturally separated by matches from other divisions, which is exactly what
  // assignCourts needs (it just packs left-to-right).
  const maxLen = Math.max(...divMatchLists.map(l => l.length));
  const interleaved = [];
  for (let i = 0; i < maxLen; i++) {
    for (const list of divMatchLists) {
      if (i < list.length) {
        interleaved.push(list[i]);
      }
    }
  }

  // Assign courts and times
  const scheduled = assignCourts(interleaved, venues, startTime, slotDuration);

  // Tag each match with the week date
  return scheduled.map(m => ({ ...m, date }));
}

// ---------------------------------------------------------------------------
// Playoff Builder
// ---------------------------------------------------------------------------

/**
 * Seed teams for playoffs based on their initial division order.
 * Division 1 team at index 0 is seed 1, index 1 is seed 2, etc., then
 * division 2, etc.
 *
 * @param {string[][]} divisions
 * @returns {string[]} ordered seed list
 */
function seedTeams(divisions) {
  const seeds = [];
  // Transpose: take position-0 from each div first (best in each div),
  // then position-1, etc.
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

/**
 * Build a single-elimination bracket for a group of 6 seeded teams.
 *
 * Structure (6-team bracket):
 *   Night 1 QFs: seed3 vs seed6, seed4 vs seed5
 *                seed1 and seed2 receive byes → advance directly to SF
 *   Night 1 SFs: seed1 vs winner(3v6), seed2 vs winner(4v5)
 *   Night 2 Final: winner SF1 vs winner SF2
 *   Night 2 3rd place: loser SF1 vs loser SF2
 *   Night 2 5th place: loser QF1 vs loser QF2
 *
 * @param {string[]} seeds - exactly 6 teams, best to worst
 * @param {number}   bracketNum - 1-based bracket index (for naming)
 * @returns {{ qf: object[], sf: object[], final: object[], consolation: object[] }}
 */
function buildBracket(seeds, bracketNum) {
  const label = `Bracket ${bracketNum}`;

  // QF matches (seed1 & seed2 get byes)
  const qf1 = { round: 'QF', division: label, teamA: seeds[2], teamB: seeds[5], note: '' };
  const qf2 = { round: 'QF', division: label, teamA: seeds[3], teamB: seeds[4], note: '' };

  // SF matches (winners of QFs face the top 2 seeds)
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

  // Final
  const final = {
    round: 'Final',
    division: label,
    teamA: `Winner SF1 (${seeds[0]} side)`,
    teamB: `Winner SF2 (${seeds[1]} side)`,
    note: 'Championship',
  };

  // 3rd place
  const thirdPlace = {
    round: '3rd Place',
    division: label,
    teamA: `Loser SF1 (${seeds[0]} side)`,
    teamB: `Loser SF2 (${seeds[1]} side)`,
    note: '3rd place match',
  };

  // 5th place
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

/**
 * Build the full playoff schedule across two nights.
 *
 * Teams are split into brackets of 6 (the last bracket may be smaller if
 * total teams is not divisible by 6; teams are seeded first).
 *
 * Night 1: all QFs and SFs from every bracket
 * Night 2: all Finals and consolation games
 *
 * @param {string[][]} divisions   - initial divisions (for seeding)
 * @param {string}     date1       - Night 1 date "YYYY-MM-DD"
 * @param {string}     date2       - Night 2 date "YYYY-MM-DD"
 * @param {object[]}   venues
 * @param {string}     startTime
 * @param {number}     slotDuration
 * @returns {{ night1: object, night2: object }}
 */
function buildPlayoffs(divisions, date1, date2, venues, startTime, slotDuration) {
  const seeds = seedTeams(divisions);
  const n = seeds.length;

  // Split seeds into brackets of 6
  const brackets = [];
  let i = 0;
  while (i < n) {
    const remaining = n - i;
    // If 7 remain, make a group of 6 and leave 1 — that's a problem.
    // Adjust: if remaining < 6, merge with previous bracket.
    if (remaining >= 6) {
      brackets.push(seeds.slice(i, i + 6));
      i += 6;
    } else {
      // Fewer than 6 left — add to last bracket (making it > 6) or handle as is
      if (brackets.length > 0) {
        // Merge extras into last bracket
        brackets[brackets.length - 1].push(...seeds.slice(i));
      } else {
        brackets.push(seeds.slice(i));
      }
      break;
    }
  }

  // Build bracket structures
  const builtBrackets = brackets.map((bracketSeeds, idx) =>
    buildBracket(bracketSeeds, idx + 1)
  );

  // Night 1: QFs first, then SFs after all QFs are done
  const totalCourts = venues.reduce((sum, v) => sum + v.courts, 0);
  const allQFs = builtBrackets.flatMap(b => b.qf);
  const allSFs = builtBrackets.flatMap(b => b.sf);

  // How many time slots do QFs occupy?
  const qfSlots = Math.ceil(allQFs.length / totalCourts);
  const startMinutes = parseTime(startTime);
  const sfStartTime = formatTime(startMinutes + qfSlots * slotDuration);

  const scheduledQFs = assignCourts(allQFs, venues, startTime, slotDuration);
  const scheduledSFs = assignCourts(allSFs, venues, sfStartTime, slotDuration);

  const night1Matches = [
    ...scheduledQFs.map(m => ({ ...m, date: date1 })),
    ...scheduledSFs.map(m => ({ ...m, date: date1 })),
  ];

  // Night 2: Finals + consolation
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

/**
 * Add `weeks` to a "YYYY-MM-DD" date string, returning a new "YYYY-MM-DD".
 *
 * @param {string} dateStr
 * @param {number} weeks
 * @returns {string}
 */
function addWeeks(dateStr, weeks) {
  const d = new Date(dateStr + 'T00:00:00');
  d.setDate(d.getDate() + weeks * 7);
  return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// API Endpoint
// ---------------------------------------------------------------------------

/**
 * POST /api/generate
 *
 * Accepts a league configuration object and returns a fully generated schedule.
 *
 * Request body (all fields with defaults):
 * {
 *   teams:           string[]  (required, 3-30 teams)
 *   seasonStartDate: string    "YYYY-MM-DD"  (required)
 *   numWeeks:        number    default 4
 *   playoffDate1:    string    "YYYY-MM-DD"
 *   playoffDate2:    string    "YYYY-MM-DD"
 *   venues:          [{ name, courts }]  default [Venue 1 (2 courts), Venue 2 (2 courts)]
 *   slotDuration:    number    default 35 (minutes)
 *   startTime:       string    "HH:MM"  default "18:30"
 *   leagueName:      string    default "Volleyball League"
 * }
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
    } = req.body;

    // Validate required fields
    if (!teams || !Array.isArray(teams) || teams.length < 3) {
      return res.status(400).json({ error: 'At least 3 teams are required.' });
    }
    if (!seasonStartDate) {
      return res.status(400).json({ error: 'seasonStartDate is required.' });
    }

    // Build initial divisions
    const divisions = buildDivisions(teams);

    // Generate regular season weeks
    const weeks = [];
    for (let w = 1; w <= numWeeks; w++) {
      const date = addWeeks(seasonStartDate, w - 1);
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

    // Generate playoffs (only if dates provided)
    let playoffs = null;
    if (playoffDate1 && playoffDate2) {
      playoffs = buildPlayoffs(divisions, playoffDate1, playoffDate2, venues, startTime, slotDuration);
    }

    res.json({
      leagueName,
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
// Start Server
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`League Scheduler running at http://localhost:${PORT}`);
});
