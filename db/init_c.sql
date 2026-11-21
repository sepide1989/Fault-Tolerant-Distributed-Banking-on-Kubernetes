CREATE TABLE accounts (account_id TEXT PRIMARY KEY, balance BIGINT NOT NULL CHECK (balance >= 0));
CREATE TABLE local_requests (request_id TEXT PRIMARY KEY, op_type TEXT NOT NULL, payload JSONB NOT NULL, result JSONB);
CREATE TABLE participant_fences (tx_id TEXT PRIMARY KEY);
CREATE TABLE participant_results (tx_id TEXT PRIMARY KEY, status TEXT NOT NULL CHECK(status IN ('COMMITTED','ABORTED')));
