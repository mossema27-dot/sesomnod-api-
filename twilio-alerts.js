/**
 * Twilio SMS Alerts for SesomNod
 * Sender varsler når picks er klare eller edge oppdages
 */

const twilio = require('twilio');

// Twilio credentials (må settes i Railway)
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_PHONE_NUMBER = process.env.TWILIO_PHONE_NUMBER;
const ADMIN_PHONE_NUMBER = process.env.ADMIN_PHONE_NUMBER; // Ditt nummer, Don

let client = null;

// Initialiser Twilio-client hvis credentials finnes
if (TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN) {
  client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
}

/**
 * Send SMS-varsel om nytt pick
 * @param {object} pick - Pick-objekt
 * @returns {Promise} Twilio-respons
 */
async function sendPickAlert(pick) {
  if (!client || !ADMIN_PHONE_NUMBER) {
    console.log('[Twilio] Credentials mangler - logger til konsoll i stedet:');
    console.log(`[ALERT] Nytt pick: ${pick.home} vs ${pick.away} - ${pick.selection} @ ${pick.odds}`);
    return { status: 'logged', message: 'Credentials mangler' };
  }

  const message = `🎯 SESOMNOD PICK
${pick.home} vs ${pick.away}
Selection: ${pick.selection}
Odds: ${pick.odds}
Edge: ${pick.edge}%
Kelly: ${pick.kellyStake} USD

God lykke! 🦅`;

  try {
    const result = await client.messages.create({
      body: message,
      from: TWILIO_PHONE_NUMBER,
      to: ADMIN_PHONE_NUMBER
    });
    return { status: 'sent', sid: result.sid };
  } catch (error) {
    console.error('[Twilio] Feil:', error.message);
    return { status: 'error', error: error.message };
  }
}

/**
 * Send daglig oppsummering
 * @param {array} picks - Dagens picks
 * @param {object} stats - Dagens stats
 */
async function sendDailySummary(picks, stats) {
  if (!client || !ADMIN_PHONE_NUMBER) {
    console.log('[Twilio] Daily summary (console):', { picks: picks.length, ...stats });
    return { status: 'logged' };
  }

  const message = `📊 SESOMNOD DAGLIG OPPSUMMERING

Picks i dag: ${picks.length}
Total Edge: ${stats.totalEdge}%
Bankroll: ${stats.bankroll} USD
ROI: ${stats.roi}%

SesomNod - Phase 0: ${stats.phase0Progress}/30`;

  try {
    const result = await client.messages.create({
      body: message,
      from: TWILIO_PHONE_NUMBER,
      to: ADMIN_PHONE_NUMBER
    });
    return { status: 'sent', sid: result.sid };
  } catch (error) {
    console.error('[Twilio] Feil:', error.message);
    return { status: 'error', error: error.message };
  }
}

module.exports = {
  sendPickAlert,
  sendDailySummary
};