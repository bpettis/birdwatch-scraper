CREATE TABLE note_requests (
    requestId TEXT PRIMARY KEY,
    userId TEXT NOT NULL,
    tweetId BIGINT NOT NULL,
    createdAtMillis BIGINT NOT NULL,
    sourceLink TEXT
);
