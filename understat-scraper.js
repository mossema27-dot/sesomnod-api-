/**
 * Understat.com xG Scraper for SesomNod
 * Basert på FootballPredictor-V2.0
 * 
 * Henter xG, xGA, xPTS for alle lag i top 5 ligaer
 */

const axios = require('axios');
const cheerio = require('cheerio');

// Liga-konfigurasjon (matcher FootballPredictor)
const UNDERSTAT_LEAGUES = {
  'La Liga': 'https://understat.com/league/La_liga/',
  'EPL': 'https://understat.com/league/EPL/',
  'Bundesliga': 'https://understat.com/league/Bundesliga/',
  'Serie A': 'https://understat.com/league/Serie_A/',
  'Ligue 1': 'https://understat.com/league/Ligue_1/'
};

/**
 * Scraper xG-data fra Understat for én liga
 * @param {string} leagueUrl - Understat URL
 * @returns {Array} Lag med xG-stats
 */
async function scrapeUnderstatLeague(leagueUrl) {
  try {
    const response = await axios.get(leagueUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      }
    });
    
    const $ = cheerio.load(response.data);
    const scripts = $('script').toArray();
    
    // Finn script med teamsData (vanligvis script #2)
    let teamsData = null;
    for (const script of scripts) {
      const scriptContent = $(script).html();
      if (scriptContent && scriptContent.includes('teamsData')) {
        // Ekstraher JSON-data
        const match = scriptContent.match(/var teamsData = JSON\.parse\('(.+?)'\)/);
        if (match) {
          const jsonStr = match[1].replace(/\\x([0-9A-Fa-f]{2})/g, (_, hex) => 
            String.fromCharCode(parseInt(hex, 16))
          );
          teamsData = JSON.parse(jsonStr);
          break;
        }
      }
    }
    
    if (!teamsData) {
      throw new Error('Fant ikke teamsData i HTML');
    }
    
    // Prosesser data for hvert lag
    const teams = [];
    for (const [teamId, teamData] of Object.entries(teamsData)) {
      const teamStats = {
        team: teamData.title,
        matches: 0,
        wins: 0,
        draws: 0,
        losses: 0,
        goals: 0,
        goalsAgainst: 0,
        points: 0,
        xG: 0,
        xGA: 0,
        xPTS: 0,
        // Home stats
        homeMatches: 0,
        homeWins: 0,
        homeDraws: 0,
        homeLosses: 0,
        homeGoals: 0,
        homeGoalsAgainst: 0,
        homePoints: 0,
        homeXG: 0,
        homeXGA: 0,
        homeXPTS: 0,
        // Away stats
        awayMatches: 0,
        awayWins: 0,
        awayDraws: 0,
        awayLosses: 0,
        awayGoals: 0,
        awayGoalsAgainst: 0,
        awayPoints: 0,
        awayXG: 0,
        awayXGA: 0,
        awayXPTS: 0
      };
      
      // Summer stats fra alle kamper
      for (const game of teamData.history) {
        // Overall
        teamStats.matches++;
        teamStats.wins += game.wins;
        teamStats.draws += game.draws;
        teamStats.losses += game.loses;
        teamStats.goals += game.scored;
        teamStats.goalsAgainst += game.missed;
        teamStats.points += game.pts;
        teamStats.xG += game.xG;
        teamStats.xGA += game.xGA;
        teamStats.xPTS += game.xpts;
        
        // Home/Away split
        if (game.h_a === 'h') {
          teamStats.homeMatches++;
          teamStats.homeWins += game.wins;
          teamStats.homeDraws += game.draws;
          teamStats.homeLosses += game.loses;
          teamStats.homeGoals += game.scored;
          teamStats.homeGoalsAgainst += game.missed;
          teamStats.homePoints += game.pts;
          teamStats.homeXG += game.xG;
          teamStats.homeXGA += game.xGA;
          teamStats.homeXPTS += game.xpts;
        } else {
          teamStats.awayMatches++;
          teamStats.awayWins += game.wins;
          teamStats.awayDraws += game.draws;
          teamStats.awayLosses += game.loses;
          teamStats.awayGoals += game.scored;
          teamStats.awayGoalsAgainst += game.missed;
          teamStats.awayPoints += game.pts;
          teamStats.awayXG += game.xG;
          teamStats.awayXGA += game.xGA;
          teamStats.awayXPTS += game.xpts;
        }
      }
      
      // Beregn per-game stats
      teamStats.xGPerGame = teamStats.xG / teamStats.matches;
      teamStats.xGAPerGame = teamStats.xGA / teamStats.matches;
      teamStats.homeXGPerGame = teamStats.homeXG / teamStats.homeMatches;
      teamStats.homeXGAPerGame = teamStats.homeXGA / teamStats.homeMatches;
      teamStats.awayXGPerGame = teamStats.awayXG / teamStats.awayMatches;
      teamStats.awayXGAPerGame = teamStats.awayXGA / teamStats.awayMatches;
      
      teams.push(teamStats);
    }
    
    return teams;
    
  } catch (error) {
    console.error(`[Understat] Feil ved scraping av ${leagueUrl}:`, error.message);
    return null;
  }
}

/**
 * Henter xG-data for alle ligaer
 * @returns {Object} xG-data per liga
 */
async function scrapeAllUnderstatLeagues() {
  const results = {};
  
  for (const [leagueName, url] of Object.entries(UNDERSTAT_LEAGUES)) {
    console.log(`[Understat] Scraper ${leagueName}...`);
    const data = await scrapeUnderstatLeague(url);
    if (data) {
      results[leagueName] = data;
      console.log(`[Understat] ✅ ${leagueName}: ${data.length} lag`);
    } else {
      console.log(`[Understat] ❌ ${leagueName}: Feil`);
    }
    
    // Rate limiting (1 sekund mellom requests)
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  return results;
}

module.exports = {
  scrapeUnderstatLeague,
  scrapeAllUnderstatLeagues,
  UNDERSTAT_LEAGUES
};