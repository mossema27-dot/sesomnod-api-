/**
 * FBRef.com Scraper for SesomNod
 * Henter data fra 40+ ligaer (inkludert Eredivisie)
 * 
 * FBRef har xG for mange ligaer, ikke bare top 5
 */

const axios = require('axios');
const cheerio = require('cheerio');

// FBRef Liga-konfigurasjon (40+ ligaer)
const FBREF_LEAGUES = {
  // Europa - Top ligaer
  'Eredivisie': 'https://fbref.com/en/comps/23/Eredivisie-Stats',
  'Primeira Liga': 'https://fbref.com/en/comps/32/Primeira-Liga-Stats',
  'Belgian Pro League': 'https://fbref.com/en/comps/37/Belgian-Pro-League-Stats',
  
  // Europa - Andre
  'Bundesliga 2': 'https://fbref.com/en/comps/33/2-Bundesliga-Stats',
  'Championship': 'https://fbref.com/en/comps/10/Championship-Stats',
  'La Liga 2': 'https://fbref.com/en/comps/17/Segunda-Division-Stats',
  'Serie B': 'https://fbref.com/en/comps/18/Serie-B-Stats',
  'Ligue 2': 'https://fbref.com/en/comps/60/Ligue-2-Stats',
  
  // Norden
  'Allsvenskan': 'https://fbref.com/en/comps/29/Allsvenskan-Stats',
  'Eliteserien': 'https://fbref.com/en/comps/28/Eliteserien-Stats',
  'Superliga': 'https://fbref.com/en/comps/50/Danish-Superliga-Stats',
  'Veikkausliiga': 'https://fbref.com/en/comps/43/Veikkausliiga-Stats',
  
  // Øst-Europa
  'Ekstraklasa': 'https://fbref.com/en/comps/36/Polish-Ekstraklasa-Stats',
  'Czech Liga': 'https://fbref.com/en/comps/66/Czech-First-League-Stats',
  'Super League': 'https://fbref.com/en/comps/57/Swiss-Super-League-Stats',
  'Austrian Bundesliga': 'https://fbref.com/en/comps/56/Austrian-Bundesliga-Stats',
  
  // Sør-Amerika
  'Brasileirao': 'https://fbref.com/en/comps/24/Serie-A-Stats',
  'Primera Division': 'https://fbref.com/en/comps/21/Primera-Division-Stats',
  'Liga MX': 'https://fbref.com/en/comps/31/Liga-MX-Stats',
  
  // Asia
  'J1 League': 'https://fbref.com/en/comps/25/J1-League-Stats',
  'K League 1': 'https://fbref.com/en/comps/55/K-League-1-Stats',
  
  // USA
  'MLS': 'https://fbref.com/en/comps/22/Major-League-Soccer-Stats'
};

// Våre 8 autoriserte ligaer (prioritet)
const PRIORITY_LEAGUES = {
  'Eredivisie': FBREF_LEAGUES['Eredivisie']
};

/**
 * Scraper lag-data fra FBRef
 * @param {string} leagueUrl - FBRef URL
 * @returns {Array} Lag med stats
 */
async function scrapeFBRefLeague(leagueUrl) {
  try {
    const response = await axios.get(leagueUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      },
      timeout: 30000
    });
    
    const $ = cheerio.load(response.data);
    
    // Finn standings-tabellen (vanligvis første store tabell)
    const table = $('#results2024-2025231_overall').length > 0 
      ? $('#results2024-2025231_overall') 
      : $('table.stats_table').first();
    
    if (!table.length) {
      throw new Error('Fant ikke standings-tabell');
    }
    
    const teams = [];
    
    table.find('tbody tr').each((i, row) => {
      const cells = $(row).find('td');
      if (cells.length < 10) return;
      
      const teamName = $(cells[0]).text().trim();
      if (!teamName) return;
      
      // Standard stats (alle ligaer har dette)
      const stats = {
        team: teamName,
        matches: parseInt($(cells[1]).text().trim()) || 0,
        wins: parseInt($(cells[2]).text().trim()) || 0,
        draws: parseInt($(cells[3]).text().trim()) || 0,
        losses: parseInt($(cells[4]).text().trim()) || 0,
        goalsFor: parseInt($(cells[5]).text().trim()) || 0,
        goalsAgainst: parseInt($(cells[6]).text().trim()) || 0,
        goalDiff: parseInt($(cells[7]).text().trim()) || 0,
        points: parseInt($(cells[8]).text().trim()) || 0,
        
        // xG stats (hvis tilgjengelig)
        xG: null,
        xGA: null,
        xGD: null,
        xGPer90: null,
        xGAPer90: null
      };
      
      // Sjekk om xG-kolonner finnes (kolonne 9+)
      if (cells.length >= 12) {
        const xGText = $(cells[9]).text().trim();
        const xGAText = $(cells[10]).text().trim();
        const xGDText = $(cells[11]).text().trim();
        
        if (xGText && !isNaN(parseFloat(xGText))) {
          stats.xG = parseFloat(xGText);
          stats.xGA = parseFloat(xGAText) || 0;
          stats.xGD = parseFloat(xGDText) || 0;
          stats.xGPer90 = stats.matches > 0 ? (stats.xG / stats.matches).toFixed(2) : 0;
          stats.xGAPer90 = stats.matches > 0 ? (stats.xGA / stats.matches).toFixed(2) : 0;
        }
      }
      
      // Beregn per-game stats
      if (stats.matches > 0) {
        stats.goalsPerGame = (stats.goalsFor / stats.matches).toFixed(2);
        stats.goalsAgainstPerGame = (stats.goalsAgainst / stats.matches).toFixed(2);
      }
      
      teams.push(stats);
    });
    
    return teams;
    
  } catch (error) {
    console.error(`[FBRef] Feil ved scraping av ${leagueUrl}:`, error.message);
    return null;
  }
}

/**
 * Henter data for prioritererte ligaer (våre 8 autoriserte)
 * @returns {Object} Data per liga
 */
async function scrapePriorityLeagues() {
  const results = {};
  
  for (const [leagueName, url] of Object.entries(PRIORITY_LEAGUES)) {
    console.log(`[FBRef] Scraper ${leagueName}...`);
    const data = await scrapeFBRefLeague(url);
    if (data) {
      results[leagueName] = data;
      console.log(`[FBRef] ✅ ${leagueName}: ${data.length} lag`);
      
      // Sjekk om xG er tilgjengelig
      const hasXG = data.some(team => team.xG !== null);
      console.log(`[FBRef]    xG-data: ${hasXG ? 'Ja' : 'Nei'}`);
    } else {
      console.log(`[FBRef] ❌ ${leagueName}: Feil`);
    }
    
    // Rate limiting (2 sekunder mellom requests)
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  return results;
}

/**
 * Henter data for alle 40+ ligaer
 * @returns {Object} Data per liga
 */
async function scrapeAllFBRefLeagues() {
  const results = {};
  
  for (const [leagueName, url] of Object.entries(FBREF_LEAGUES)) {
    console.log(`[FBRef] Scraper ${leagueName}...`);
    const data = await scrapeFBRefLeague(url);
    if (data) {
      results[leagueName] = data;
      console.log(`[FBRef] ✅ ${leagueName}: ${data.length} lag`);
    } else {
      console.log(`[FBRef] ❌ ${leagueName}: Feil`);
    }
    
    // Rate limiting
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  return results;
}

module.exports = {
  scrapeFBRefLeague,
  scrapePriorityLeagues,
  scrapeAllFBRefLeagues,
  FBREF_LEAGUES,
  PRIORITY_LEAGUES
};