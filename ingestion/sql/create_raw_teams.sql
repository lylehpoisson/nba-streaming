CREATE OR REPLACE TABLE NBA_DB.RAW.RAW_TEAMS (
    ID              INTEGER       NOT NULL COMMENT 'nba_api internal team ID (PK)',
    FULL_NAME       VARCHAR(100)  NOT NULL COMMENT 'Full team name e.g. Los Angeles Lakers',
    ABBREVIATION    VARCHAR(5)    NOT NULL COMMENT '3-letter team abbreviation e.g. LAL',
    NICKNAME        VARCHAR(50)            COMMENT 'Team nickname e.g. Lakers',
    CITY            VARCHAR(50)            COMMENT 'Team city e.g. Los Angeles',
    STATE           VARCHAR(50)            COMMENT 'Team state e.g. California',
    YEAR_FOUNDED    INTEGER                COMMENT 'Year the franchise was founded',
    CONSTRAINT PK_RAW_TEAMS PRIMARY KEY (ID)
)
COMMENT = 'Static NBA team reference data loaded from nba_api. One row per franchise.';