# NBA Pipeline 🏀

End-to-end data pipeline pulling NBA game logs from the public `nba_api` into Snowflake, transforming them with dbt, and orchestrated with Dagster on a nightly schedule.

**Stack:** Python · nba_api · Snowflake · dbt-core · Dagster

## Architecture

```
nba_api (free, no key)
    └─► Dagster (nightly schedule, 9am UTC)
            └─► ingestion/cli.py
                    └─► NBA_DB.RAW           (3 raw tables, loaded via stage + COPY INTO)
                            └─► dbt run
                                    ├─► DBT_DEV_STAGING   (views — type-cast + renamed)
                                    └─► DBT_DEV_MARTS     (tables — aggregated, ranked)
```

## Repo layout

```
nba_pipeline/
├── ingestion/
│   ├── cli.py                  # Entry point: python cli.py load [--season YYYY-YY]
│   ├── load_to_snowflake.py    # Extracts from nba_api, loads RAW layer
│   └── sql/
│       ├── create_raw_teams.sql
│       ├── create_raw_team_game_logs.sql
│       └── create_raw_player_game_logs.sql
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml            # Reads SNOWFLAKE_PASSWORD from env
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_team_game_logs.sql
│       │   └── stg_player_game_logs.sql
│       └── marts/
│           ├── mart_team_season_stats.sql
│           └── mart_player_season_stats.sql
├── dagster_project/
│   └── nba_pipeline/
│       └── nba_pipeline/
│           ├── assets.py       # Raw ingestion assets (Python)
│           ├── dbt_assets.py   # dbt models as Dagster assets
│           └── definitions.py  # Wires assets, resources, schedule
├── requirements.in             # Top-level deps (compile with pip-compile)
└── requirements.txt
```

## Setup

### 1. Clone and create a virtual environment

```bash
git clone <repo-url>
cd nba_pipeline
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure credentials

Create a `.env` file in the repo root:

```ini
SNOWFLAKE_ACCOUNT=your-account-id
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=NBA_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN
DBT_SCHEMA=DBT_DEV
```

`SNOWFLAKE_SCHEMA` is the raw ingestion target; `DBT_SCHEMA` is the schema dbt writes transformed models into. `dbt_project/profiles.yml` reads all connection fields from the environment.

### 3. Load raw data manually (optional)

```bash
cd ingestion
python cli.py load                    # defaults to 2025-26
python cli.py load --season 2023-24   # any prior season
```

This creates `NBA_DB.RAW` in Snowflake and loads three tables:

| Table | Description | ~Rows |
|---|---|---|
| `RAW_TEAMS` | Static team reference (id, name, abbreviation, city) | 30 |
| `RAW_TEAM_GAME_LOGS` | One row per team per game — full box score | 2,460 |
| `RAW_PLAYER_GAME_LOGS` | One row per player per game — full box score | 26,000+ |

### 4. Run dbt manually (optional)

```bash
cd dbt_project
dbt debug          # verify Snowflake connection
dbt run            # build all models
dbt test           # run data quality tests
dbt docs generate
dbt docs serve     # browse lineage in browser
```

### 5. Run with Dagster

```bash
# Set DAGSTER_HOME for persistent run history
export DAGSTER_HOME=~/.dagster_home   # Windows: $env:DAGSTER_HOME = "C:\dagster_home"

cd dagster_project/nba_pipeline
dagster dev
```

Open http://127.0.0.1:3000. The pipeline runs on a nightly schedule at 9am UTC (after all NBA games have finished). To trigger a manual run, go to Catalog → select all assets → Materialize all.

## Data models

### Staging (views in `DBT_DEV_STAGING`)

| Model | Source table | What it does |
|---|---|---|
| `stg_team_game_logs` | `RAW_TEAM_GAME_LOGS` | Types all columns, derives `season` from `SEASON_ID`, adds `home_away` flag |
| `stg_player_game_logs` | `RAW_PLAYER_GAME_LOGS` | Types all columns, excludes DNPs |

### Marts (tables in `DBT_DEV_MARTS`)

| Model | Description |
|---|---|
| `mart_team_season_stats` | Season aggregates per team: W/L record, win %, per-game shooting/rebounding/playmaking averages, home/away splits, and win % rank |
| `mart_player_season_stats` | Season aggregates per player: PPG/RPG/APG, true shooting %, possession actions per game, fantasy points average, PPG rank and TS% rank (min 10 games) |

## Updating dependencies

Top-level deps are in `requirements.in`. To recompile the pinned lockfile:

```bash
pip install pip-tools
pip-compile requirements.in
```

## Roadmap

- Incremental dbt models — append new games only instead of full refresh
- `mart_rolling_team_form` — last-10-games rolling window using Snowflake window functions
- Multi-season backfill command (`cli.py backfill --start 2020-21 --end 2025-26`)
- RAG layer — player/team embeddings in a vector database for natural language queries