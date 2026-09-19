---
title: Team Overview & Executive Performance
description: Comprehensive match analytics, weekly momentum, map dominance, and core squad ratings for OUR_TEAM.
---

# 🎮 Valorant Squad Analytics — Team Overview

Welcome to the executive command center for **OUR_TEAM** (Captain: **Agamemnon**). Real-time analytics ingested across all 211 competitive matches from Databricks Unity Catalog.

```sql kpi_summary
SELECT 
    COUNT(match_id) AS total_matches,
    ROUND(AVG(CASE WHEN is_our_team_win THEN 1.0 ELSE 0.0 END) * 100, 1) AS win_rate_pct,
    SUM(our_team_rounds_won) AS rounds_won,
    SUM(opponent_rounds_won) AS rounds_lost,
    SUM(net_round_differential) AS net_differential
FROM valorant.gold.gold_match_summary
```

```sql weekly_trend
SELECT 
    time_period AS week_label,
    matches_won,
    matches_lost,
    team_win_pct AS win_rate_pct,
    matches_played
FROM valorant.gold.gold_team_performance
WHERE period_type = 'WEEKLY'
ORDER BY period_start_date ASC
```

```sql map_dominance
SELECT 
    map_name,
    COUNT(match_id) AS matches_played,
    SUM(CASE WHEN is_our_team_win THEN 1 ELSE 0 END) AS matches_won,
    SUM(CASE WHEN is_our_team_win = false AND match_outcome = 'DEFEAT' THEN 1 ELSE 0 END) AS matches_lost,
    ROUND(AVG(CASE WHEN is_our_team_win THEN 1.0 ELSE 0.0 END) * 100, 1) AS win_rate_pct,
    SUM(net_round_differential) AS round_differential
FROM valorant.gold.gold_match_summary
GROUP BY map_name
ORDER BY matches_played DESC
```

```sql core_squad_leaderboard
SELECT 
    player_name,
    primary_agent,
    primary_role,
    matches_played,
    ROUND(kd_ratio, 2) AS kd_ratio,
    ROUND(avg_damage_per_round, 1) AS adr,
    ROUND(avg_combat_score, 1) AS acs,
    ROUND(headshot_pct, 1) AS headshot_pct,
    ROUND(clutch_success_pct, 1) AS clutch_pct
FROM valorant.gold.gold_player_overall_summary
WHERE is_core_squad = true
ORDER BY kd_ratio DESC
```

```sql recent_match_feed
SELECT 
    date_format(game_start_time, 'yyyy-MM-dd HH:mm') AS match_time,
    map_name,
    match_outcome,
    CONCAT(our_team_rounds_won, ' - ', opponent_rounds_won) AS score,
    match_mvp_name AS mvp,
    game_duration_minutes AS duration_mins
FROM valorant.gold.gold_match_summary
ORDER BY game_start_time DESC
LIMIT 15
```

---

## 🏆 Key Performance Indicators

{% row %}
  {% big_value
    data="kpi_summary"
    value="sum(total_matches)"
    title="Total Matches Played"
  /%}

  {% big_value
    data="kpi_summary"
    value="avg(win_rate_pct)"
    title="Match Win Rate"
    fmt="pct1"
  /%}

  {% big_value
    data="kpi_summary"
    value="sum(rounds_won)"
    title="Rounds Won"
  /%}

  {% big_value
    data="kpi_summary"
    value="sum(net_differential)"
    title="Net Round Differential"
  /%}
{% /row %}

---

## 📈 Form & Map Pool Analysis

{% row %}
  {% bar_chart
    data="weekly_trend"
    x="week_label"
    y="sum(matches_won)"
    title="Weekly Match Victories Trend"
  /%}

  {% bar_chart
    data="map_dominance"
    x="map_name"
    y="sum(matches_played)"
    title="Map Pool Volume & Dominance"
    order="sum(matches_played) desc"
  /%}
{% /row %}

---

## 👑 Core Squad Leaderboard & Efficiency Ratings

{% table data="core_squad_leaderboard" %}
  {% dimension value="player_name" title="Player" /%}
  {% dimension value="primary_agent" title="Top Agent" /%}
  {% dimension value="primary_role" title="Role" /%}
  {% measure value="sum(matches_played)" title="Matches" /%}
  {% measure value="avg(kd_ratio)" title="K/D Ratio" /%}
  {% measure value="avg(adr)" title="ADR" /%}
  {% measure value="avg(acs)" title="ACS" /%}
  {% measure value="avg(headshot_pct)" title="HS %" fmt="pct1" /%}
  {% measure value="avg(clutch_pct)" title="Clutch %" fmt="pct1" /%}
{% /table %}

---

## ⏱️ Recent Match History Feed (Last 15 Matches)

{% table data="recent_match_feed" %}
  {% dimension value="match_time" title="Date / Time" /%}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="match_outcome" title="Outcome" /%}
  {% dimension value="score" title="Final Score" /%}
  {% dimension value="mvp" title="Match MVP" /%}
  {% measure value="avg(duration_mins)" title="Duration (Mins)" /%}
{% /table %}
