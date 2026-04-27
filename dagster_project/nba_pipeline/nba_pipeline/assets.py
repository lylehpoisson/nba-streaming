from dagster import asset, OpExecutionContext, AssetKey
from pipeline import load as ingest
from pipeline import rag

SEASON = "2025-26"


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


@asset(
    key=AssetKey(["embeddings"]),
    group_name="rag",
    compute_kind="python",
    deps=[
        AssetKey(["marts", "mart_team_season_stats"]),
        AssetKey(["marts", "mart_player_season_stats"]),
    ],
)
def embeddings(context: OpExecutionContext):
    """Generate and load vector embeddings for all teams and players."""
    n = rag.embed_all()
    context.log.info(f"Loaded {n} embeddings")
