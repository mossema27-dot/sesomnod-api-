/**
 * Poisson-modell for fotball-mål-prediksjon
 * Basert på FootballPredictor-V2.0
 * 
 * Bruker xG-data til å beregne sannsynlighet for Over/Under
 */

const { scrapeAllUnderstatLeagues } = require('./understat-scraper');

/**
 * Beregn lambda (forventede mål) for en kamp
 * @param {Object} homeTeam - Hjemmelagets xG-stats
 * @param {Object} awayTeam - Bortelagets xG-stats
 * @returns {Object} lambda_home, lambda_away
 */
function calculateLambda(homeTeam, awayTeam) {
  // xG per game
  const homeXG = homeTeam.xGPerGame;
  const homeXGA = homeTeam.xGAPerGame;
  const awayXG = awayTeam.xGPerGame;
  const awayXGA = awayTeam.xGAPerGame;
  
  // xG diff (hvor mye bedre/svakere enn gjennomsnitt)
  const homeXGDiff = homeXG - (homeTeam.xG / homeTeam.matches);
  const homeXGADiff = homeXGA - (homeTeam.xGA / homeTeam.matches);
  const awayXGDiff = awayXG - (awayTeam.xG / awayTeam.matches);
  const awayXGADiff = awayXGA - (awayTeam.xGA / awayTeam.matches);
  
  // Lambda formel fra FootballPredictor
  const lambdaHome = (homeXG + homeXGDiff) * (awayXGA - awayXGADiff);
  const lambdaAway = (awayXG + awayXGDiff) * (homeXGA - homeXGADiff);
  
  return {
    lambdaHome: Math.max(0, lambdaHome), // Ikke negativ
    lambdaAway: Math.max(0, lambdaAway)
  };
}

/**
 * Poisson sannsynlighetsmassefunksjon
 * @param {number} k - Antall mål
 * @param {number} lambda - Forventet antall mål
 * @returns {number} Sannsynlighet
 */
function poissonPMF(k, lambda) {
  return (Math.pow(lambda, k) * Math.exp(-lambda)) / factorial(k);
}

/**
 * Fakultet (n!)
 * @param {number} n 
 * @returns {number}
 */
function factorial(n) {
  if (n === 0 || n === 1) return 1;
  let result = 1;
  for (let i = 2; i <= n; i++) {
    result *= i;
  }
  return result;
}

/**
 * Kumulativ Poisson (CDF)
 * @param {number} k - Antall mål
 * @param {number} lambda - Forventet antall mål
 * @returns {number} P(X <= k)
 */
function poissonCDF(k, lambda) {
  let sum = 0;
  for (let i = 0; i <= k; i++) {
    sum += poissonPMF(i, lambda);
  }
  return sum;
}

/**
 * Prediker Over/Under for en kamp
 * @param {string} homeTeamName - Hjemmelag
 * @param {string} awayTeamName - Bortelag
 * @param {Object} leagueData - xG-data for ligaen
 * @returns {Object} Prediksjoner
 */
function predictOverUnder(homeTeamName, awayTeamName, leagueData) {
  const homeTeam = leagueData.find(t => t.team === homeTeamName);
  const awayTeam = leagueData.find(t => t.team === awayTeamName);
  
  if (!homeTeam || !awayTeam) {
    return { error: 'Lag ikke funnet i xG-data' };
  }
  
  const { lambdaHome, lambdaAway } = calculateLambda(homeTeam, awayTeam);
  const lambdaTotal = lambdaHome + lambdaAway;
  
  // Over/Under sannsynligheter
  const probOver1_5 = 1 - poissonCDF(1, lambdaTotal);
  const probOver2_5 = 1 - poissonCDF(2, lambdaTotal);
  const probOver3_5 = 1 - poissonCDF(3, lambdaTotal);
  
  // Under sannsynligheter
  const probUnder1_5 = poissonCDF(1, lambdaTotal);
  const probUnder2_5 = poissonCDF(2, lambdaTotal);
  const probUnder3_5 = poissonCDF(3, lambdaTotal);
  
  // Begge lag scorer
  const probBTTS = (1 - poissonPMF(0, lambdaHome)) * (1 - poissonPMF(0, lambdaAway));
  
  // H2H sannsynligheter (forenklet)
  let probHomeWin = 0;
  let probDraw = 0;
  let probAwayWin = 0;
  
  for (let i = 0; i <= 5; i++) {
    for (let j = 0; j <= 5; j++) {
      const prob = poissonPMF(i, lambdaHome) * poissonPMF(j, lambdaAway);
      if (i > j) probHomeWin += prob;
      else if (i === j) probDraw += prob;
      else probAwayWin += prob;
    }
  }
  
  return {
    homeTeam: homeTeamName,
    awayTeam: awayTeamName,
    lambdaHome: lambdaHome.toFixed(2),
    lambdaAway: lambdaAway.toFixed(2),
    expectedGoals: lambdaTotal.toFixed(2),
    overUnder: {
      over1_5: (probOver1_5 * 100).toFixed(1) + '%',
      under1_5: (probUnder1_5 * 100).toFixed(1) + '%',
      over2_5: (probOver2_5 * 100).toFixed(1) + '%',
      under2_5: (probUnder2_5 * 100).toFixed(1) + '%',
      over3_5: (probOver3_5 * 100).toFixed(1) + '%',
      under3_5: (probUnder3_5 * 100).toFixed(1) + '%'
    },
    btts: (probBTTS * 100).toFixed(1) + '%',
    h2h: {
      homeWin: (probHomeWin * 100).toFixed(1) + '%',
      draw: (probDraw * 100).toFixed(1) + '%',
      awayWin: (probAwayWin * 100).toFixed(1) + '%'
    },
    // Anbefaling basert på sannsynlighet
    recommendation: probOver2_5 > 0.55 ? 'Over 2.5' : 
                    probUnder2_5 > 0.55 ? 'Under 2.5' : 
                    'Ingen klar edge'
  };
}

/**
 * Finn value bets ved å sammenligne modell-odds med bookmaker-odds
 * @param {Object} prediction - Poisson-prediksjon
 * @param {Object} bookmakerOdds - Odds fra bookmaker
 * @returns {Object} Value bet analyse
 */
function findValueBet(prediction, bookmakerOdds) {
  const modelProbOver2_5 = parseFloat(prediction.overUnder.over2_5) / 100;
  const impliedProbOver2_5 = 1 / bookmakerOdds.over2_5;
  
  const edge = (modelProbOver2_5 - impliedProbOver2_5) * 100;
  
  return {
    market: 'Over 2.5',
    modelProbability: (modelProbOver2_5 * 100).toFixed(1) + '%',
    bookmakerOdds: bookmakerOdds.over2_5,
    impliedProbability: (impliedProbOver2_5 * 100).toFixed(1) + '%',
    edge: edge.toFixed(2) + '%',
    isValueBet: edge > 5 // Value bet hvis edge > 5%
  };
}

module.exports = {
  calculateLambda,
  predictOverUnder,
  findValueBet,
  poissonPMF,
  poissonCDF
};