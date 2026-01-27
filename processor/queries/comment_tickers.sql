CREATE TABLE IF NOT EXISTS comment_tickers (
    id BIGSERIAL PRIMARY KEY,
    comment_id VARCHAR(255) NOT NULL REFERENCES comments(id),
    parent_id VARCHAR(255) REFERENCES comments(id)
    post_id VARCHAR(255) NULL REFERENCES posts(id),   -- helpful to query by post
    ticker TEXT NOT NULL,                              -- normalized uppercase ticker
    detected_by TEXT NOT NULL,                         -- 'dollar_regex'|'allcaps_regex'|'symbol_prefix'|'ml'|'propagated'
    confidence REAL NOT NULL,                          -- 0.0+
    context_snippet TEXT,                              -- short excerpt around the mention
    --mention_kind TEXT,                                 -- e.g., 'dollar','allcaps','title_inferred'
    inferred_from_id VARCHAR(255),                     -- if propagated, the comment/post id it was inferred from
    author TEXT,                                       -- comment author (copy for fast aggregation)
    created_utc BIGINT,                                -- copy of comment timestamp (epoch)
    sentiment_real REAL,                               -- raw sentiment score (e.g. VADER compound) for this comment
    validated BOOLEAN DEFAULT FALSE,                   -- set true if human-reviewed
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- index for fast aggregation & lookups
CREATE INDEX IF NOT EXISTS idx_comment_tickers_ticker_date ON comment_tickers (ticker, to_timestamp(created_utc)::date);
CREATE INDEX IF NOT EXISTS idx_comment_tickers_comment ON comment_tickers (comment_id);
CREATE INDEX IF NOT EXISTS idx_comment_tickers_post ON comment_tickers (post_id);
