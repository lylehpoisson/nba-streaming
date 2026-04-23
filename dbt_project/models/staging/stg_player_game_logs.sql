-- models/staging/stg_player_game_logs.sql
-- Clean and type-cast the raw player game log data

with source as (
    select * from {{ source('raw', 'RAW_PLAYER_GAME_LOGS') }}
),

renamed as (
    select
        GAME_ID                             as game_id,
        PLAYER_ID                           as player_id,
        PLAYER_NAME                         as player_name,
        TEAM_ID                             as team_id,
        TEAM_ABBREVIATION                   as team_abbreviation,
        to_date(GAME_DATE, 'YYYY-MM-DD')    as game_date,
    MATCHUP                             as matchup,
        case when WL = 'W' then true else false end as is_win,
        -- Minutes as float (stored as "MM:SS" string in some seasons)
        cast(MIN as float)                  as minutes,
        cast(PTS as integer)                as points,
        cast(FGM as integer)                as fgm,
        cast(FGA as integer)                as fga,
        cast(FG_PCT as float)               as fg_pct,
        cast(FG3M as integer)               as fg3m,
        cast(FG3A as integer)               as fg3a,
        cast(FG3_PCT as float)              as fg3_pct,
        cast(FTM as integer)                as ftm,
        cast(FTA as integer)                as fta,
        cast(FT_PCT as float)               as ft_pct,
        cast(OREB as integer)               as offensive_rebounds,
        cast(DREB as integer)               as defensive_rebounds,
        cast(REB as integer)                as rebounds,
        cast(AST as integer)                as assists,
        cast(STL as integer)                as steals,
        cast(BLK as integer)                as blocks,
        cast(TOV as integer)                as turnovers,
        cast(PF as integer)                 as personal_fouls,
        cast(PLUS_MINUS as float)           as plus_minus,
        cast(NBA_FANTASY_PTS as float)      as fantasy_points
    from source
)

select * from renamed