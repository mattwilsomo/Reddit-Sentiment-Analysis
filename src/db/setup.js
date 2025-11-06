// Import the PostgreSQL connection pool
// (connection.js should export a configured 'pg' Pool instance)
const pool = require('./connection.js');
require('../envCheck'); // fail-fast if env missing

// ==========================================
// Function: setupDatabase()
// Purpose:  Create all required tables if they don't already exist.
// Tables:
//   1. posts          - raw Reddit post data
//   2. comments       - Reddit comment data linked to posts
//   3. processed_data - aggregated ticker/day data for analysis
// ==========================================
async function setupDatabase() {
    // Get a client connection from the pool
    const client = await pool.connect();

    try {
        // -----------------------------
        // Create 'posts' table
        // -----------------------------
        const createPostsTableQuery = `
        CREATE TABLE IF NOT EXISTS posts(
            id VARCHAR(255) PRIMARY KEY,                        -- Reddit post ID (e.g. t3_xxxxxx)
            subreddit TEXT,                                     -- Subreddit name (e.g. r/pennystocks)
            title TEXT,                                         -- Post title
            body TEXT,                                          -- Post body text
            author TEXT,                                        -- Reddit username of author
            created_utc BIGINT,                                 -- UTC timestamp from Reddit (epoch)
            score INTEGER,                                      -- Reddit upvote score
            inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now()  -- When this record was inserted
        );`;

        // -----------------------------
        // Create 'comments' table
        // -----------------------------
        const createCommentTableQuery = `
        CREATE TABLE IF NOT EXISTS comments(
            id VARCHAR(255) PRIMARY KEY,                        -- Reddit comment ID (e.g. t1_xxxxxx)
            body TEXT,                                          -- Comment body text
            author TEXT,                                        -- Comment author
            created_utc BIGINT,                                 -- UTC timestamp from Reddit (epoch)
            parent_id VARCHAR(255) REFERENCES comments(id), -- Optional self-reference for nested replies
            post_id VARCHAR(255) REFERENCES posts(id),          -- Foreign key → parent post
            score INTEGER,                                        -- Reddit upvote score
            inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now()  -- When this comment was inserted
        );`;

        // -----------------------------
        // Create 'processed_data' table
        // This table stores daily aggregated sentiment + market data
        // -----------------------------
        const createProcessedDataTableQuery = `
        CREATE TABLE IF NOT EXISTS processed_data(
            ticker TEXT NOT NULL,                               -- Stock ticker symbol (e.g. GME)
            mention_date DATE NOT NULL,                         -- Date of mentions (UTC)
            mention_volume INTEGER,                             -- # of total mentions that day
            unique_posters INTEGER,                             -- # of unique users mentioning ticker
            avg_sentiment REAL,                                 -- Mean sentiment score (VADER compound)
            sum_sentiment REAL,                                 -- Sum of all sentiment scores
            sentiment_stddev REAL,                              -- Standard deviation of sentiment (volatility)
            sentiment_skew REAL,                                -- Skewness of sentiment distribution
            moving_avg_sentiment REAL,                          -- Rolling 3-day average sentiment (optional)
            bull_ratio REAL,                                    -- % of mentions with positive sentiment
            bear_ratio REAL,                                    -- % of mentions with negative sentiment
            price_open NUMERIC,                                 -- Market open price for the ticker that day
            price_close NUMERIC,                                -- Market close price for the ticker that day
            price_change_pct NUMERIC,                           -- (close - open) / open * 100
            volume_traded BIGINT,                               -- Trading volume (from yfinance)
            created_at TIMESTAMP DEFAULT now(),                 -- Insert timestamp
            PRIMARY KEY (ticker, mention_date)                  -- One row per ticker per date
        );`;

        // Execute table creation queries sequentially
        await client.query(createPostsTableQuery);
        console.log("✅ Database table 'posts' is ready.");

        await client.query(createCommentTableQuery);
        console.log("✅ Database table 'comments' is ready.");

        await client.query(createProcessedDataTableQuery);
        console.log("✅ Database table 'processed_data' is ready.");

    } catch (e) {
        // Handle any SQL or connection errors
        console.error("❌ Error setting up database tables:", e);
    } finally {
        // Always release the client back to the pool
        client.release();
    }
}

// Run the setup when this script is executed
setupDatabase()
    .then(() => {
        console.log("🎉 Database setup complete.");
        pool.end(); // Close all pool connections
    })
    .catch(err => {
        console.error("❌ An error occurred during database setup:", err);
        pool.end();
    });
