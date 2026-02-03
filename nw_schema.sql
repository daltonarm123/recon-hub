CREATE TABLE IF NOT EXISTS nw_history (
    kingdom TEXT NOT NULL,
    networth BIGINT NOT NULL,
    tick_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (kingdom, tick_time)
);

CREATE INDEX IF NOT EXISTS nw_history_kingdom_time_idx
ON nw_history (kingdom, tick_time DESC);
