CREATE OR REPLACE TABLE NBA_DB.RAW.EMBEDDINGS (
    ENTITY_TYPE         VARCHAR(20)     NOT NULL COMMENT 'Type of entity: team or player',
    ENTITY_ID           INTEGER         NOT NULL COMMENT 'Team ID or Player ID',
    ENTITY_NAME         VARCHAR(100)    NOT NULL COMMENT 'Team or player name',
    SUMMARY             VARCHAR(2000)   NOT NULL COMMENT 'Human-readable text summary used to generate the embedding',
    EMBEDDING           VECTOR(FLOAT, 3072) NOT NULL COMMENT 'Google gemini-embedding-001 vector (3072 dimensions)',
    EMBEDDED_AT         TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'When this embedding was generated',
    CONSTRAINT PK_EMBEDDINGS PRIMARY KEY (ENTITY_TYPE, ENTITY_ID)
)
COMMENT = 'Vector embeddings for NBA teams and players, generated from mart aggregates using Google gemini-embedding-001. Used for natural language querying via cosine similarity search.';