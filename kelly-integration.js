/**
 * Kelly-js integrasjon for SesomNod
 * Gir Kelly Criterion, CLV, og bankroll management
 */

const { kelly, clv, bankrollStats, impliedProb, toDecimal } = require('./lib/kelly-js/dist/index.js');

/**
 * Beregn optimal innsats basert på Kelly Criterion
 * @param {number} winProbability - Sannsynlighet for seier (0-1)
 * @param {number} decimalOdds - Desimalodds (f.eks. 2.4)
 * @param {number} bankroll - Total bankroll
 * @returns {object} Kelly-beregning
 */
function calculateKellyBet(winProbability, decimalOdds, bankroll) {
  // Konverter desimalodds til American odds for kelly-js
  const americanOdds = decimalOdds >= 2.0 
    ? (decimalOdds - 1) * 100  // +odds
    : -100 / (decimalOdds - 1); // -odds
  
  const k = kelly(winProbability, americanOdds);
  
  return {
    fraction: k.fraction,           // Full Kelly (brutal varians)
    halfKelly: k.halfKelly,         // Anbefalt for de fleste
    quarterKelly: k.quarterKelly,   // Konservativ
    dollars: k.dollars(bankroll),   // Full Kelly i $
    halfDollars: k.halfDollars(bankroll), // Anbefalt i $
    ev: k.ev,                       // Expected value
    edge: k.edge,                   // Edge over implied prob
    hasEdge: k.hasEdge              // Boolean: har vi edge?
  };
}

/**
 * Beregn Closing Line Value (CLV)
 * @param {number} betOdds - Odds vi tok (desimal)
 * @param {number} closingOdds - Closing odds (desimal)
 * @returns {object} CLV-analyse
 */
function calculateCLV(betOdds, closingOdds) {
  // Konverter til American odds
  const betAmerican = betOdds >= 2.0 ? (betOdds - 1) * 100 : -100 / (betOdds - 1);
  const closingAmerican = closingOdds >= 2.0 ? (closingOdds - 1) * 100 : -100 / (closingOdds - 1);
  
  const c = clv(betAmerican, closingAmerican);
  
  return {
    clvPercent: c.clv,              // CLV i prosent
    verdict: c.verdict,             // 'positive', 'negative', 'neutral'
    betOdds: betOdds,
    closingOdds: closingOdds
  };
}

/**
 * Bankroll health check
 * @param {array} bets - Array av bet-objekter
 * @param {number} initialBankroll - Start-bankroll
 * @returns {object} Bankroll-statistikk
 */
function getBankrollHealth(bets, initialBankroll) {
  return bankrollStats(bets, initialBankroll);
}

/**
 * Konverter desimalodds til implied probability
 * @param {number} decimalOdds - Desimalodds
 * @returns {number} Implied probability (0-1)
 */
function getImpliedProbability(decimalOdds) {
  return 1 / decimalOdds;
}

module.exports = {
  calculateKellyBet,
  calculateCLV,
  getBankrollHealth,
  getImpliedProbability
};