-- models/marts/mart_player_season_stats.sql
-- Aggregated player stats for the season -- supports player rankings / comparisons

with game_logs as (
    select * from {{ ref('stg_player_game_logs') }}
),

aggregated as (
    select
        player_id,
        player_name,
        team_id,
        team_abbreviation,
        count(*)                                    as games_played,
        round(avg(minutes), 1)                      as avg_minutes,
        -- Per-game averages
        round(avg(points), 1)                       as ppg,
        round(avg(rebounds), 1)                     as rpg,
        round(avg(assists), 1)                      as apg,
        round(avg(steals), 1)                       as spg,
        round(avg(blocks), 1)                       as bpg,
        round(avg(turnovers), 1)                    as topg,
        round(avg(plus_minus), 1)                   as avg_plus_minus,
        -- Shooting efficiency
        round(avg(fg_pct), 3)                       as fg_pct,
        round(avg(fg3_pct), 3)                      as fg3_pct,
        round(avg(ft_pct), 3)                       as ft_pct,
        -- True shooting %: PTS / (2 * (FGA + 0.44 * FTA))
        round(
            sum(points)::float / nullif(
                2 * (sum(fga) + 0.44 * sum(fta)),
                0
            ),
            3
        )                                           as true_shooting_pct,
        -- Possession actions per game (usage proxy)
        round(
            (sum(fga) + 0.44 * sum(fta) + sum(turnovers))::float
            / nullif(count(*), 0),
            1
        )                                           as possession_actions_per_game,
        -- Win stats
        sum(case when is_win then 1 else 0 end)     as wins,
        round(avg(fantasy_points), 1)               as avg_fantasy_pts
    from game_logs
    where minutes > 0   -- exclude DNPs
    group by 1, 2, 3, 4
)

select
    *,
    -- Rankings (players with >= 10 games only)
    case when games_played >= 10 then
        rank() over (order by ppg desc)
    end                                             as ppg_rank,
    case when games_played >= 10 then
        rank() over (order by true_shooting_pct desc)
    end                                             as ts_pct_rank
from aggregated
order by ppg desc