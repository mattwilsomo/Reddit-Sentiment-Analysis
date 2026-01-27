CREATE TABLE IF NOT EXISTS post_tickers(
    id BIGSERIAL PRIMARY KEY,
    post_id VARCHAR(255) NOT NULL REFERENCES posts(id),
    ticker TEXT NOT NULL,
    detected_by TEXT NOT NULL,
    confidence REAL NOT NULL,
    body TEXT,
    -- mention_kind TEXT,
    author TEXT, 
    created_utc BIGINT,
    sentiment_real REAL,
    validated BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_post_tickers_date ON post_tickers (ticker, to_timestamp(created_utc)::date);
CREATE INDEX IF NOT EXISTS idx_post_tickers_post ON post_ticker (post_id);