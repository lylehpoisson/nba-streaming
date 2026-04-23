-- models/staging/stg_player_game_logs.sql
-- Clean and type-cast the raw player game log data
-- Incremental: appends new games only, identified by game_id + player_id
{{ config(materialized="incremental", unique_key=["game_id", "player_id"]) }}

with
    source as (
        select *
        from {{ source("raw", "RAW_PLAYER_GAME_LOGS") }}

        {% if is_incremental() %}
            where to_date(game_date) > (select max(game_date) from {{ this }})
        {% endif %}
    ),

    renamed as (
        select
            game_id as game_id,
            player_id as player_id,
            player_name as player_name,
            team_id as team_id,
            team_abbreviation as team_abbreviation,
            to_date(game_date) as game_date,
            matchup as matchup,
            case when wl = 'W' then true else false end as is_win,
            cast(min as float) as minutes,
            cast(pts as integer) as points,
            cast(fgm as integer) as fgm,
            cast(fga as integer) as fga,
            cast(fg_pct as float) as fg_pct,
            cast(fg3m as integer) as fg3m,
            cast(fg3a as integer) as fg3a,
            cast(fg3_pct as float) as fg3_pct,
            cast(ftm as integer) as ftm,
            cast(fta as integer) as fta,
            cast(ft_pct as float) as ft_pct,
            cast(oreb as integer) as offensive_rebounds,
            cast(dreb as integer) as defensive_rebounds,
            cast(reb as integer) as rebounds,
            cast(ast as integer) as assists,
            cast(stl as integer) as steals,
            cast(blk as integer) as blocks,
            cast(tov as integer) as turnovers,
            cast(pf as integer) as personal_fouls,
            cast(plus_minus as float) as plus_minus,
            cast(nba_fantasy_pts as float) as fantasy_points
        from source
    )

select *
from renamed
