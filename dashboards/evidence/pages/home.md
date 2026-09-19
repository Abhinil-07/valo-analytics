---
title: Team Overview & Executive Performance
description: Comprehensive match analytics, weekly momentum, map dominance, and core squad ratings for OUR_TEAM.
---

# 🎮 Valorant Squad Analytics — Team Overview

Welcome to the executive command center for **OUR_TEAM** (Captain: **Agamemnon**). Real-time analytics ingested across all 211 competitive matches from Databricks Unity Catalog.

```sql kpi_summary
SELECT 
    COUNT(match_id) AS total_matches,
    AVG(CASE WHEN is_our_team_win THEN 1.0 ELSE 0.0 END) AS win_rate_ratio,
    SUM(our_team_rounds_won) AS rounds_won,
    SUM(opponent_rounds_won) AS rounds_lost,
    SUM(round_differential) AS net_differential
FROM valorant.gold.gold_match_summary
```

```sql weekly_trend
SELECT 
    time_period AS week_label,
    matches_won,
    matches_lost,
    team_win_pct / 100.0 AS win_rate_ratio,
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
    AVG(CASE WHEN is_our_team_win THEN 1.0 ELSE 0.0 END) AS win_rate_ratio,
    SUM(round_differential) AS round_differential
FROM valorant.gold.gold_match_summary
GROUP BY map_name
ORDER BY matches_played DESC
```

```sql core_squad_leaderboard
SELECT 
    player_name,
    most_played_agent AS primary_agent,
    most_played_agent_role AS primary_role,
    total_matches_played AS matches_played,
    ROUND(career_kd_ratio, 2) AS kd_ratio,
    ROUND(career_avg_adr, 1) AS adr,
    ROUND(career_avg_acs, 1) AS acs,
    career_headshot_pct / 100.0 AS headshot_ratio
FROM valorant.gold.gold_player_overall_summary
WHERE is_core_team = true
ORDER BY career_kd_ratio DESC
```

```sql recent_match_feed
SELECT 
    date_format(match_start_timestamp, 'yyyy-MM-dd HH:mm') AS match_time,
    map_name,
    match_outcome,
    CONCAT(our_team_rounds_won, ' - ', opponent_rounds_won) AS score,
    match_mvp_player AS mvp,
    game_duration_minutes AS duration_mins
FROM valorant.gold.gold_match_summary
ORDER BY match_start_timestamp DESC
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
    value="avg(win_rate_ratio)"
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

```sql starting_side_kpis
SELECT 
    SUM(attack_start_matches) AS atk_starts,
    SUM(attack_start_wins) AS atk_wins,
    SUM(attack_start_wins) * 1.0 / NULLIF(SUM(attack_start_matches), 0) AS atk_start_win_ratio,
    SUM(defense_start_matches) AS def_starts,
    SUM(defense_start_wins) AS def_wins,
    SUM(defense_start_wins) * 1.0 / NULLIF(SUM(defense_start_matches), 0) AS def_start_win_ratio
FROM valorant.gold.gold_map_performance
```

### ⚔️ Coin Toss Impact — Starting Side Win Rates (All Maps)

{% row %}
  {% big_value
    data="starting_side_kpis"
    value="avg(atk_start_win_ratio)"
    title="Win % Starting on Attack"
    fmt="pct1"
  /%}

  {% big_value
    data="starting_side_kpis"
    value="sum(atk_starts)"
    title="Attack Starts"
    fmt="num0"
  /%}

  {% big_value
    data="starting_side_kpis"
    value="avg(def_start_win_ratio)"
    title="Win % Starting on Defense"
    fmt="pct1"
  /%}

  {% big_value
    data="starting_side_kpis"
    value="sum(def_starts)"
    title="Defense Starts"
    fmt="num0"
  /%}
{% /row %}

---

## 📈 Form & Map Pool Analysis

{% row %}
  {% combo_chart
    data="weekly_trend"
    x="week_label"
    title="Weekly Match Results & Win Rate Trend"
    y_fmt="num0"
    y2_fmt="pct1"
  %}
    {% bar y="matches_won" /%}
    {% bar y="matches_lost" /%}
    {% line y="win_rate_ratio" axis="y2" /%}
  {% /combo_chart %}

  {% combo_chart
    data="map_dominance"
    x="map_name"
    title="Map Performance (Wins vs. Losses)"
  %}
    {% bar y="matches_won" /%}
    {% bar y="matches_lost" /%}
  {% /combo_chart %}
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
  {% measure value="avg(headshot_ratio)" title="HS %" fmt="pct1" /%}
{% /table %}

---

## ⏱️ Recent Match History Feed (Last 15 Matches)

{% table data="recent_match_feed" repeat_values=true %}
  {% dimension value="match_time" title="Date / Time" /%}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="match_outcome" title="Outcome" /%}
  {% dimension value="score" title="Final Score" /%}
  {% dimension value="mvp" title="Match MVP" /%}
  {% dimension value="duration_mins" title="Duration (Mins)" /%}
{% /table %}

