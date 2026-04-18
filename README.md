# NBA Pipeline 🏀

Snowflake + nba_api + dbt portfolio project.

## Stack
- **Source**: nba_api (free, no key needed)
- **Warehouse**: Snowflake (free trial)
- **Transformation**: dbt-core + dbt-snowflake
- **Orchestration**: Dagster (next phase)

## Project Structure
```
nba_pipeline/
├── ingestion/
│   └── load_to_snowflake.py    # Pulls from nba_api → Snowflake RAW layer
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── staging/
        │   ├── sources.yml
        │   ├── stg_team_game_logs.sql
        │   └── stg_player_game_logs.sql
        └── marts/
            ├── mart_team_season_stats.sql
            └── mart_player_season_stats.sql
```

## Setup

### 1. Install dependencies
```bash
pip install nba_api snowflake-connector-python dbt-snowflake
```

### 2. Set your Snowflake password
```bash
export SNOWFLAKE_PASSWORD="your_password_here"
```

### 3. Run ingestion
```bash
cd ingestion
python load_to_snowflake.py
```
This creates `NBA_DB.RAW` with 3 tables:
- `RAW_TEAMS` — static team reference (~30 rows)
- `RAW_TEAM_GAME_LOGS` — one row per team per game (~2,400 rows)
- `RAW_PLAYER_GAME_LOGS` — one row per player per game (~50,000+ rows)

### 4. Run dbt
```bash
cd dbt_project

# Tell dbt where profiles.yml is (or copy it to ~/.dbt/)
export DBT_PROFILES_DIR=.

dbt debug        # verify connection
dbt run          # build all models
dbt test         # run data quality tests
dbt docs generate && dbt docs serve   # browse lineage
```

## Snowflake Schema Layout
```
NBA_DB
├── RAW          (raw tables — loaded by Python)
├── DBT_DEV_STAGING  (views — stg_ models)
└── DBT_DEV_MARTS    (tables — mart_ models)
```

## dbt Models

### Staging (views)
| Model | Description |
|-------|-------------|
| `stg_team_game_logs` | Cleaned team box scores with typed columns + home/away flag |
| `stg_player_game_logs` | Cleaned player box scores with typed columns |

### Marts (tables)
| Model | Description |
|-------|-------------|
| `mart_team_season_stats` | Season aggregates + home/away splits + rankings |
| `mart_player_season_stats` | PPG/RPG/APG + true shooting % + usage + rankings |

## Next Steps (Phase 2)
- Add Dagster to orchestrate ingestion + dbt runs on a schedule
- Add `mart_rolling_team_form` — last 10 games rolling window
- Add RAG layer with player/game embeddings