CREATE TABLE comments (
    id        BIGINT NOT NULL,          -- Snowflake ID (time-sortable)
    post_id   BIGINT NOT NULL,
    user_id   BIGINT NOT NULL,
    parent_id BIGINT,                   -- NULL = top-level comment, else a reply
    body      TEXT   NOT NULL,
    metadata  JSONB  NOT NULL DEFAULT '{}',
    PRIMARY KEY (post_id, id)           -- partition column must be in the key
) PARTITION BY HASH (post_id);

CREATE TABLE comments_p0 PARTITION OF comments FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE comments_p1 PARTITION OF comments FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE comments_p2 PARTITION OF comments FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE comments_p3 PARTITION OF comments FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- Needed for GET /comments/{id} and for replies
CREATE INDEX idx_comments_id ON comments (id);
CREATE INDEX idx_comments_parent ON comments (parent_id);