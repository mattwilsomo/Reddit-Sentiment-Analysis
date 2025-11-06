// src/envCheck.js
// Fail-fast check for required environment variables.
// Require this at the top of any script that needs env vars (scraper, db setup, etc).

const required = [
  // Reddit / Snoowrap
  'USER_AGENT',
  'CLIENT_ID',
  'CLIENT_SECRET',
  'REDDIT_USER',
  'REDDIT_PASS',

  // Postgres
  'DB_USER',
  'DB_HOST',
  'DB_NAME',
  'DB_PASSWORD',
  'DB_PORT'
];

const missing = required.filter(k => !process.env[k] || process.env[k].trim() === '');

if (missing.length > 0) {
  console.error('❌ Missing required environment variables:', missing.join(', '));
  // Exit with a non-zero code so a process manager (or CI) notices
  process.exit(1);
}

// Optionally, normalize DB_PORT to integer for downstream code
if (process.env.DB_PORT) {
  const p = parseInt(process.env.DB_PORT, 10);
  if (Number.isNaN(p)) {
    console.error(`❌ DB_PORT "${process.env.DB_PORT}" is not a valid integer.`);
    process.exit(1);
  }
  process.env.DB_PORT = String(p);
}

module.exports = true; // not necessary, but makes require() explicit
