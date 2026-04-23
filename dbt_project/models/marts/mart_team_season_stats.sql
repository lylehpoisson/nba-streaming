-- models/marts/mart_team_season_stats.sql
-- Aggregated team performance for the full season — good for standings/rankings
with
    game_logs as (select * from {{ ref("stg_team_game_logs") }}),

    aggregated as (
        select
            team_id,
            team_abbreviation,
            team_name,
            season,
            count(*) as games_played,
            sum(case when is_win then 1 else 0 end) as wins,
            sum(case when not is_win then 1 else 0 end) as losses,
            round(
                sum(case when is_win then 1 else 0 end)::float / count(*), 3
            ) as win_pct,
            -- Scoring
            round(avg(points), 1) as avg_points,
            round(avg(fg_pct), 3) as avg_fg_pct,
            round(avg(fg3_pct), 3) as avg_fg3_pct,
            round(avg(ft_pct), 3) as avg_ft_pct,
            -- Rebounding
            round(avg(rebounds), 1) as avg_rebounds,
            round(avg(offensive_rebounds), 1) as avg_offensive_rebounds,
            round(avg(defensive_rebounds), 1) as avg_defensive_rebounds,
            -- Playmaking / defense
            round(avg(assists), 1) as avg_assists,
            round(avg(steals), 1) as avg_steals,
            round(avg(blocks), 1) as avg_blocks,
            round(avg(turnovers), 1) as avg_turnovers,
            -- Net rating proxy
            round(avg(plus_minus), 1) as avg_plus_minus,
            -- Home / away splits
            round(
                sum(case when home_away = 'home' and is_win then 1 else 0 end)::float
                / nullif(sum(case when home_away = 'home' then 1 else 0 end), 0),
                3
            ) as home_win_pct,
            round(
                sum(case when home_away = 'away' and is_win then 1 else 0 end)::float
                / nullif(sum(case when home_away = 'away' then 1 else 0 end), 0),
                3
            ) as away_win_pct
        from game_logs
        group by 1, 2, 3, 4
    )

select
    *,
    -- Simple ranking columns (useful for dashboards)
    rank() over (partition by season order by win_pct desc) as win_pct_rank,
    rank() over (partition by season order by avg_points desc) as scoring_rank,
    rank() over (partition by season order by avg_plus_minus desc) as net_rating_rank
from aggregated
order by win_pct desc
