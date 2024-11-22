CREATE TABLE IF NOT EXISTS market_hour (
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   start_time INTEGER NOT NULL,
   end_time INTEGER NOT NULL,
   period INTEGER NOT NULL,
   round_interval INTEGER NOT NULL,
   updated_at INTEGER NOT NULL
);
