from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource
from nba_pipeline.assets import (
    raw_teams,
    raw_team_game_logs,
    raw_player_game_logs,
    embeddings,
)
from nba_pipeline.dbt_assets import nba_dbt_assets, DBT_PROJECT_PATH

nba_pipeline_job = define_asset_job(
    name="nba_pipeline_job",
    selection=[
        raw_teams,
        raw_team_game_logs,
        raw_player_game_logs,
        nba_dbt_assets,
    ],
)

nightly_schedule = ScheduleDefinition(
    job=nba_pipeline_job,
    cron_schedule="0 9 * * *",  # 9am UTC = 1am PST / 4am EST, after all games finish
)

defs = Definitions(
    assets=[
        raw_teams,
        raw_team_game_logs,
        raw_player_game_logs,
        nba_dbt_assets,
        embeddings,
    ],
    jobs=[nba_pipeline_job],
    schedules=[nightly_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_PATH)),
    },
)
