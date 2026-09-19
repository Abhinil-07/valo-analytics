---
title: Player Profiles & Agent Mastery
description: Dedicated individual player command center, complete agent pool breakdown, weapon lethality, and match history logs.
---

# 👤 Player Command Center & Tactical Dossiers

Select any squad member from the dropdown to inspect their entire career dossier — including every agent they have ever played, weapon lethality, and match-by-match history.

```sql player_dropdown_list
SELECT 
    current_display_name AS player_name,
    CONCAT(current_display_name, ' (', most_played_agent_role, ' — ', total_matches_played, ' Matches)') AS player_label
FROM valorant.gold.gold_player_overall_summary
WHERE total_matches_played >= 10
ORDER BY total_matches_played DESC
```

```sql all_players_data
SELECT 
    current_display_name AS player_name,
    roster_role,
    is_core_team,
    total_matches_played,
    matches_won,
    match_win_pct / 100.0 AS match_win_ratio,
    total_rounds_played,
    rounds_won,
    round_win_pct / 100.0 AS round_win_ratio,
    total_kills,
    total_deaths,
    total_assists,
    career_kd_ratio,
    career_kill_differential,
    first_blood_count,
    first_death_count,
    first_blood_differential,
    trade_kill_count,
    career_avg_acs,
    career_avg_adr,
    career_headshot_pct / 100.0 AS headshot_ratio,
    most_played_agent,
    most_played_agent_role,
    most_played_agent_icon_url,
    most_played_agent_matches,
    highest_winrate_agent,
    match_mvp_count,
    team_top_fragger_count,
    overall_rating
FROM valorant.gold.gold_player_overall_summary
ORDER BY total_matches_played DESC
```

```sql player_agent_pool
SELECT 
    o.current_display_name AS player_name,
    a.agent_name,
    a.agent_role,
    a.agent_icon_url,
    a.matches_played,
    a.matches_won,
    a.matches_lost,
    a.agent_win_pct / 100.0 AS win_rate_ratio,
    ROUND(a.agent_win_pct, 1) AS win_pct_num,
    a.kd_ratio,
    ROUND(a.avg_acs, 1) AS avg_acs,
    ROUND(a.avg_adr, 1) AS avg_adr,
    a.headshot_pct / 100.0 AS hs_ratio,
    a.mastery_tier
FROM valorant.gold.gold_agent_performance a
JOIN valorant.gold.gold_player_overall_summary o ON a.player_puuid = o.player_puuid
WHERE o.total_matches_played >= 10
ORDER BY a.matches_played DESC
```

```sql player_weapons
SELECT 
    o.current_display_name AS player_name,
    c.weapon_name,
    c.weapon_category,
    c.total_kills,
    c.kill_share_pct / 100.0 AS kill_share_ratio,
    c.weapon_round_win_pct / 100.0 AS win_rate_ratio,
    c.weapon_headshot_pct / 100.0 AS hs_ratio,
    c.first_bloods_secured AS first_bloods
FROM valorant.gold.gold_combat_performance c
JOIN valorant.gold.gold_player_overall_summary o ON c.player_puuid = o.player_puuid
WHERE c.total_kills > 0
ORDER BY c.total_kills DESC
```

```sql player_recent_matches
SELECT 
    current_display_name AS player_name,
    match_date,
    map_name,
    agent_name,
    match_outcome,
    score_display,
    ROUND(average_combat_score, 0) AS acs,
    ROUND(kill_death_ratio, 2) AS match_kd,
    kills,
    deaths,
    assists,
    headshot_pct / 100.0 AS match_hs_ratio,
    is_match_mvp
FROM valorant.gold.gold_player_match_performance
ORDER BY match_date DESC
LIMIT 500
```

---

## 🎯 Select Player to Inspect

{% dropdown 
    id="player_filter" 
    data="player_dropdown_list" 
    value_column="player_name" 
    label_column="player_label"
    select_first=true 
    title="Choose Squad Member" 
/%}

---

## 🪪 Career Combat Scorecard

{% row %}
  {% big_value
    data="all_players_data"
    value="sum(total_matches_played)"
    title="Matches Played"
    fmt="num0"
    filters=["player_filter"]
  /%}

  {% big_value
    data="all_players_data"
    value="avg(match_win_ratio)"
    title="Career Win Rate"
    fmt="pct1"
    filters=["player_filter"]
  /%}

  {% big_value
    data="all_players_data"
    value="avg(career_kd_ratio)"
    title="Career K/D Ratio"
    fmt="num2"
    filters=["player_filter"]
  /%}

  {% big_value
    data="all_players_data"
    value="avg(career_avg_acs)"
    title="Avg Combat Score (ACS)"
    fmt="num1"
    filters=["player_filter"]
  /%}

  {% big_value
    data="all_players_data"
    value="avg(career_avg_adr)"
    title="Avg Damage / Round (ADR)"
    fmt="num1"
    filters=["player_filter"]
  /%}

  {% big_value
    data="all_players_data"
    value="avg(headshot_ratio)"
    title="Headshot Accuracy"
    fmt="pct1"
    filters=["player_filter"]
  /%}
{% /row %}

{% row %}
  {% table data="all_players_data" filters=["player_filter"] %}
    {% dimension value="player_name" title="Player" /%}
    {% dimension value="most_played_agent_role" title="Primary Role" /%}
    {% dimension value="overall_rating" title="Rating Tier" /%}
    {% measure value="sum(match_mvp_count)" title="🏆 Match MVPs" fmt="num0" /%}
    {% measure value="sum(team_top_fragger_count)" title="🥇 Team MVPs" fmt="num0" /%}
    {% measure value="sum(first_blood_count)" title="First Bloods" fmt="num0" /%}
    {% measure value="sum(first_death_count)" title="First Deaths" fmt="num0" /%}
    {% measure value="sum(first_blood_differential)" title="Net Opening Duels (+/-)" fmt="num0" /%}
  {% /table %}
{% /row %}

---

## 🎭 Complete Agent Pool (All Agents Played by Selected Player)

Every agent this player has picked in competitive play — showing full individual stats, K/D, win rates, and ACS:


{% row %}
  {% bar_chart
    data="player_agent_pool"
    x="agent_name"
    y="matches_played"
    title="Matches Played by Agent"
    order="matches_played desc"
    filters=["player_filter"]
  /%}

  {% bar_chart
    data="player_agent_pool"
    x="agent_name"
    y="win_rate_ratio"
    title="Win Rate % by Agent"
    y_fmt="pct1"
    order="matches_played desc"
    filters=["player_filter"]
  /%}
{% /row %}

{% table data="player_agent_pool" filters=["player_filter"] repeat_values=true %}
  {% dimension value="agent_name" title="Agent" /%}
  {% dimension value="agent_role" title="Role" /%}
  {% dimension value="mastery_tier" title="Mastery Tier" /%}
  {% measure value="sum(matches_played)" title="Played" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="sum(matches_lost)" title="Losses" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(kd_ratio)" title="K/D" fmt="num2" /%}
  {% measure value="avg(avg_acs)" title="ACS" fmt="num1" /%}
  {% measure value="avg(avg_adr)" title="ADR" fmt="num1" /%}
  {% measure value="avg(hs_ratio)" title="HS %" fmt="pct1" /%}
{% /table %}

---

## 🔫 Weapon Lethality for Selected Player

Weapons that this squad member secures the most kills and victories with:

{% table data="player_weapons" filters=["player_filter"] repeat_values=true %}
  {% dimension value="weapon_name" title="Weapon" /%}
  {% dimension value="weapon_category" title="Category" /%}
  {% measure value="sum(total_kills)" title="Fatal Kills" fmt="num0" /%}
  {% measure value="avg(kill_share_ratio)" title="Kill Share %" fmt="pct1" /%}
  {% measure value="avg(win_rate_ratio)" title="Round Win %" fmt="pct1" /%}
  {% measure value="avg(hs_ratio)" title="Headshot %" fmt="pct1" /%}
  {% measure value="sum(first_bloods)" title="First Bloods" fmt="num0" /%}
{% /table %}

---

## ⏱️ Recent Match History Log for Selected Player

Recent competitive matches played by this squad member:

{% table data="player_recent_matches" filters=["player_filter"] repeat_values=true %}
  {% dimension value="match_date" title="Date" /%}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="agent_name" title="Agent" /%}
  {% dimension value="match_outcome" title="Outcome" /%}
  {% dimension value="score_display" title="Score" /%}
  {% measure value="avg(acs)" title="ACS" fmt="num0" /%}
  {% measure value="avg(match_kd)" title="K/D" fmt="num2" /%}
  {% measure value="sum(kills)" title="K" fmt="num0" /%}
  {% measure value="sum(deaths)" title="D" fmt="num0" /%}
  {% measure value="sum(assists)" title="A" fmt="num0" /%}
  {% measure value="avg(match_hs_ratio)" title="HS %" fmt="pct1" /%}
{% /table %}

---

## 👑 Full Squad Roster Leaderboard & Overall Ratings

Compare all active squad members side-by-side:

{% table data="all_players_data" repeat_values=true %}
  {% dimension value="player_name" title="Player" /%}
  {% dimension value="most_played_agent_role" title="Primary Role" /%}
  {% dimension value="overall_rating" title="Rating" /%}
  {% dimension value="most_played_agent" title="Top Agent" /%}
  {% measure value="sum(total_matches_played)" title="Matches" fmt="num0" /%}
  {% measure value="avg(match_win_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(career_kd_ratio)" title="K/D" fmt="num2" /%}
  {% measure value="avg(career_avg_acs)" title="ACS" fmt="num1" /%}
  {% measure value="avg(career_avg_adr)" title="ADR" fmt="num1" /%}
  {% measure value="avg(headshot_ratio)" title="HS %" fmt="pct1" /%}
  {% measure value="sum(match_mvp_count)" title="MVPs" fmt="num0" /%}
{% /table %}
