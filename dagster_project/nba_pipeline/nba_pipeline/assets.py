import sys
from pathlib import Path

# Allow importing from the ingestion folder
sys.path.append(str(Path(__file__).parent.parent.parent.parent / "ingestion"))

from dagster import asset, OpExecutionContext, AssetKey

import load_to_snowflake as ingest

SEASON = "2024-25"


@asset(key=AssetKey(["raw", "RAW_TEAMS"]), group_name="raw", compute_kind="python")
def raw_teams(context: OpExecutionContext):
    """Static NBA team reference data loaded from nba_api."""
    conn = ingest.get_conn()
    ingest.setup_snowflake(conn)
    df = ingest.extract_teams()
    nrows = ingest.load_df(conn, df, "RAW_TEAMS")
    conn.close()
    context.log.info(f"Loaded {nrows} teams")


@asset(
    key=AssetKey(["raw", "RAW_TEAM_GAME_LOGS"]), group_name="raw", compute_kind="python"
)
def raw_team_game_logs(context: OpExecutionContext):
    """Raw team box scores per game from nba_api LeagueGameLog."""
    conn = ingest.get_conn()
    df = ingest.extract_team_game_logs(SEASON)
    nrows = ingest.load_df(conn, df, "RAW_TEAM_GAME_LOGS")
    conn.close()
    context.log.info(f"Loaded {nrows} team-game rows")


@asset(
    key=AssetKey(["raw", "RAW_PLAYER_GAME_LOGS"]),
    group_name="raw",
    compute_kind="python",
)
def raw_player_game_logs(context: OpExecutionContext):
    """Raw player box scores per game from nba_api PlayerGameLogs."""
    conn = ingest.get_conn()
    df = ingest.extract_player_game_logs(SEASON)
    nrows = ingest.load_df(conn, df, "RAW_PLAYER_GAME_LOGS")
    conn.close()
    context.log.info(f"Loaded {nrows} player-game rows")
