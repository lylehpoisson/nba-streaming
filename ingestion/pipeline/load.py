"""
NBA -> Snowflake ingestion logic.
Pulls game logs + player stats from nba_api and loads raw tables into Snowflake.
Invoked via cli.py -- do not run directly.
"""

import csv
import os
import tempfile
import time
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from nba_api.stats.endpoints import leaguegamelog, playergamelogs
from nba_api.stats.static import teams

load_dotenv()

# -- Constants ----------------------------------------------------------------

DELAY = 0.6  # seconds between API calls -- be polite to stats.nba.com
SQL_DIR = Path(__file__).parent.parent / "sql"


# Explicit allowlist of columns to keep for player game logs -- matches RAW_PLAYER_GAME_LOGS schema.
# Using an allowlist rather than a drop-list guards against future API columns causing load errors.
PLAYER_LOG_KEEP_COLS = [
    "GAME_ID",
    "PLAYER_ID",
    "PLAYER_NAME",
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "GAME_DATE",
    "MATCHUP",
    "WL",
    "MIN",
    "PTS",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "PLUS_MINUS",
    "NBA_FANTASY_PTS",
]

# -- Config -------------------------------------------------------------------


def get_snowflake_config() -> dict:
    """Load Snowflake connection config from environment / .env file."""
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "role": os.environ["SNOWFLAKE_ROLE"],
    }


# -- Helpers ------------------------------------------------------------------


def get_conn():
    return snowflake.connector.connect(**get_snowflake_config())


def run_sql_file(conn, path: Path):
    """Read a .sql file and execute it."""
    sql = path.read_text()
    conn.cursor().execute(sql)


def setup_snowflake(conn):
    """Create database, schema, and all raw tables from SQL files in sql/."""
    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS NBA_DB")
    cur.execute("CREATE SCHEMA IF NOT EXISTS NBA_DB.RAW")
    cur.close()

    for sql_file in sorted(SQL_DIR.glob("create_*.sql")):
        run_sql_file(conn, sql_file)
        print(f"  {sql_file.name} executed")

    print("  Snowflake database, schema, and tables ready")


def load_df(conn, df: pd.DataFrame, table: str):
    """
    Load a DataFrame into Snowflake via an internal stage + COPY INTO.
    More reliable than write_pandas -- gives explicit control over
    compression, error handling, and stage lifecycle.
    """
    df.columns = [c.upper() for c in df.columns]

    stage = f"nba_stage_{table.lower()}"
    cur = conn.cursor()

    # Create a temporary internal stage
    cur.execute(
        f"CREATE OR REPLACE TEMPORARY STAGE {stage} FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' NULL_IF = ('') SKIP_HEADER = 1 ENCODING = 'UTF-8')"
    )

    # Write DataFrame to a temp CSV and PUT it to the stage
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
    ) as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_NONNUMERIC)
        tmp_path = f.name

    cur.execute(f"PUT file://{tmp_path} @{stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

    # Truncate and load
    cur.execute(f"TRUNCATE TABLE NBA_DB.RAW.{table}")
    cur.execute(f"""
        COPY INTO NBA_DB.RAW.{table}
        FROM @{stage}
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('') SKIP_HEADER = 1)
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE = TRUE
    """)

    # Get row count from COPY INTO result
    results = cur.fetchall()
    nrows = sum(r[3] for r in results)  # rows_loaded column

    cur.close()
    Path(tmp_path).unlink(missing_ok=True)  # clean up temp file
    print(f"  {table}: {nrows} rows loaded")
    return nrows


# -- Extractors ---------------------------------------------------------------


def extract_teams() -> pd.DataFrame:
    print("Fetching static team list...")
    df = pd.DataFrame(teams.get_teams())
    print(f"  -> {len(df)} teams")
    return df


def extract_team_game_logs(season: str) -> pd.DataFrame:
    print(f"Fetching team game logs ({season})...")
    gl = leaguegamelog.LeagueGameLog(
        season=season,
        player_or_team_abbreviation="T",
        season_type_all_star="Regular Season",
        timeout=60,
    )
    df = gl.get_data_frames()[0]
    df = df.drop(columns=["VIDEO_AVAILABLE"], errors="ignore")
    print(f"  -> {len(df)} team-game rows")
    return df


def extract_player_game_logs(season: str) -> pd.DataFrame:
    print(f"Fetching player game logs ({season})...")
    time.sleep(DELAY)
    pl = playergamelogs.PlayerGameLogs(
        season_nullable=season,
        season_type_nullable="Regular Season",
        timeout=60,
    )
    df = pl.get_data_frames()[0]
    df = df[[c for c in PLAYER_LOG_KEEP_COLS if c in df.columns]]
    print(f"  -> {len(df)} player-game rows")
    return df


# -- Load entrypoint ----------------------------------------------------------


def load(season: str):
    """Load all NBA raw tables for a given season into Snowflake."""
    print(f"Loading season: {season}")

    conn = get_conn()
    setup_snowflake(conn)

    df_teams = extract_teams()
    load_df(conn, df_teams, "RAW_TEAMS")

    df_team_logs = extract_team_game_logs(season)
    load_df(conn, df_team_logs, "RAW_TEAM_GAME_LOGS")

    df_player_logs = extract_player_game_logs(season)
    load_df(conn, df_player_logs, "RAW_PLAYER_GAME_LOGS")

    conn.close()
    print("\nDone! Check NBA_DB.RAW in Snowflake.")
