---
title: Player Head-to-Head Comparison
description: Direct dual-player tactical duel, end-to-end combat scorecard, map-by-map rotation efficiency, agent pool mastery, and role dominance.
---

# ⚔️ Player Head-to-Head Duel & Tactical Synergy

Directly compare two squad members across all combat metrics, map proficiencies, agent mastery, and tactical role performance to optimize roster rotations.

```sql player_list_a
SELECT 
    current_display_name AS player_name,
    CONCAT(current_display_name, ' (', most_played_agent_role, ' — ', total_matches_played, ' Matches)') AS player_label
FROM valorant.gold.gold_player_overall_summary
WHERE total_matches_played >= 5
ORDER BY total_matches_played DESC
```

```sql player_list_b
SELECT 
    current_display_name AS player_name,
    CONCAT(current_display_name, ' (', most_played_agent_role, ' — ', total_matches_played, ' Matches)') AS player_label
FROM valorant.gold.gold_player_overall_summary
WHERE total_matches_played >= 5
ORDER BY total_matches_played DESC
```

```sql player_overall_dossier
SELECT 
    current_display_name AS player_name,
    roster_role,
    is_core_team,
    overall_rating,
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
    career_bodyshot_pct / 100.0 AS bodyshot_ratio,
    career_legshot_pct / 100.0 AS legshot_ratio,
    most_played_agent,
    most_played_agent_role,
    most_played_agent_icon_url,
    most_played_agent_matches,
    highest_winrate_agent,
    total_ultimate_casts,
    total_ability_casts,
    match_mvp_count,
    team_top_fragger_count
FROM valorant.gold.gold_player_overall_summary
```

```sql player_maps_dossier
SELECT 
    current_display_name AS player_name,
    map_name,
    COUNT(DISTINCT match_id) AS map_matches,
    SUM(CASE WHEN is_match_win = true THEN 1 ELSE 0 END) AS map_wins,
    SUM(CASE WHEN is_match_win = false THEN 1 ELSE 0 END) AS map_losses,
    ROUND(SUM(CASE WHEN is_match_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT match_id), 3) AS map_win_ratio,
    ROUND(SUM(kills) * 1.0 / NULLIF(SUM(deaths), 0), 2) AS map_kd_ratio,
    ROUND(AVG(average_combat_score), 1) AS map_avg_acs,
    ROUND(AVG(average_damage_per_round), 1) AS map_avg_adr,
    ROUND(AVG(headshot_pct) / 100.0, 3) AS map_hs_ratio
FROM valorant.gold.gold_player_match_performance
GROUP BY current_display_name, map_name
ORDER BY map_matches DESC
```

```sql player_agents_dossier
SELECT 
    current_display_name AS player_name,
    agent_name,
    agent_role,
    COUNT(DISTINCT match_id) AS agent_matches,
    SUM(CASE WHEN is_match_win = true THEN 1 ELSE 0 END) AS agent_wins,
    ROUND(SUM(CASE WHEN is_match_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT match_id), 3) AS agent_win_ratio,
    ROUND(SUM(kills) * 1.0 / NULLIF(SUM(deaths), 0), 2) AS agent_kd_ratio,
    ROUND(AVG(average_combat_score), 1) AS agent_avg_acs,
    ROUND(AVG(average_damage_per_round), 1) AS agent_avg_adr,
    ROUND(AVG(headshot_pct) / 100.0, 3) AS agent_hs_ratio
FROM valorant.gold.gold_player_match_performance
GROUP BY current_display_name, agent_name, agent_role
ORDER BY agent_matches DESC
```

```sql player_roles_dossier
SELECT 
    current_display_name AS player_name,
    agent_role,
    COUNT(DISTINCT match_id) AS role_matches,
    SUM(CASE WHEN is_match_win = true THEN 1 ELSE 0 END) AS role_wins,
    ROUND(SUM(CASE WHEN is_match_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT match_id), 3) AS role_win_ratio,
    ROUND(SUM(kills) * 1.0 / NULLIF(SUM(deaths), 0), 2) AS role_kd_ratio,
    ROUND(AVG(average_combat_score), 1) AS role_avg_acs,
    ROUND(AVG(average_damage_per_round), 1) AS role_avg_adr
FROM valorant.gold.gold_player_match_performance
GROUP BY current_display_name, agent_role
ORDER BY role_matches DESC
```

```sql squad_rotation_matrix
WITH player_map_summary AS (
    SELECT 
        map_name,
        current_display_name AS player_name,
        COUNT(DISTINCT match_id) AS matches_played,
        ROUND(SUM(CASE WHEN is_match_win = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT match_id), 1) AS win_pct,
        ROUND(SUM(kills) * 1.0 / NULLIF(SUM(deaths), 0), 2) AS kd_ratio,
        ROUND(AVG(average_combat_score), 1) AS avg_acs
    FROM valorant.gold.gold_player_match_performance
    GROUP BY map_name, current_display_name
    HAVING matches_played >= 3
),
ranked_players AS (
    SELECT 
        map_name,
        player_name,
        matches_played,
        win_pct,
        kd_ratio,
        avg_acs,
        ROW_NUMBER() OVER (PARTITION BY map_name ORDER BY win_pct DESC, kd_ratio DESC) AS win_rank,
        ROW_NUMBER() OVER (PARTITION BY map_name ORDER BY avg_acs DESC) AS frag_rank
    FROM player_map_summary
)
SELECT 
    w.map_name,
    CONCAT(w.player_name, ' (', CAST(w.win_pct AS STRING), '% Win Rate in ', w.matches_played, ' Games)') AS highest_winrate_player,
    CONCAT(f.player_name, ' (', CAST(f.avg_acs AS STRING), ' ACS, ', f.kd_ratio, ' K/D)') AS highest_impact_fragger
FROM (SELECT * FROM ranked_players WHERE win_rank = 1) w
JOIN (SELECT * FROM ranked_players WHERE frag_rank = 1) f ON w.map_name = f.map_name
ORDER BY w.map_name ASC
```

---

## 🎯 Select Players to Duel

{% row %}
  {% dropdown 
      id="player_a" 
      data="player_list_a" 
      value_column="player_name" 
      label_column="player_label"
      select_first=true 
      title="🔵 Choose Player 1" 
  /%}

  {% dropdown 
      id="player_b" 
      data="player_list_b" 
      value_column="player_name" 
      label_column="player_label"
      title="🔴 Choose Player 2" 
  /%}
{% /row %}

---

## 🥊 Head-to-Head Career Combat Scorecard

Side-by-side comparison across overall career impact, accuracy, and tactical clutch ratings:

{% row %}
  {% table data="player_overall_dossier" filters=["player_a"] %}
    {% dimension value="player_name" title="🔵 Player 1" /%}
    {% dimension value="roster_role" title="Roster Role" /%}
    {% dimension value="overall_rating" title="Rating" /%}
    {% measure value="sum(total_matches_played)" title="Matches" fmt="num0" /%}
    {% measure value="avg(match_win_ratio)" title="Win %" fmt="pct1" /%}
    {% measure value="avg(career_kd_ratio)" title="K/D" fmt="num2" /%}
    {% measure value="avg(career_avg_acs)" title="Avg ACS" fmt="num1" /%}
    {% measure value="avg(career_avg_adr)" title="Avg ADR" fmt="num1" /%}
    {% measure value="avg(headshot_ratio)" title="HS %" fmt="pct1" /%}
    {% measure value="sum(first_blood_differential)" title="+/- FB" fmt="num0" /%}
    {% measure value="sum(match_mvp_count)" title="MVPs" fmt="num0" /%}
  {% /table %}

  {% table data="player_overall_dossier" filters=["player_b"] %}
    {% dimension value="player_name" title="🔴 Player 2" /%}
    {% dimension value="roster_role" title="Roster Role" /%}
    {% dimension value="overall_rating" title="Rating" /%}
    {% measure value="sum(total_matches_played)" title="Matches" fmt="num0" /%}
    {% measure value="avg(match_win_ratio)" title="Win %" fmt="pct1" /%}
    {% measure value="avg(career_kd_ratio)" title="K/D" fmt="num2" /%}
    {% measure value="avg(career_avg_acs)" title="Avg ACS" fmt="num1" /%}
    {% measure value="avg(career_avg_adr)" title="Avg ADR" fmt="num1" /%}
    {% measure value="avg(headshot_ratio)" title="HS %" fmt="pct1" /%}
    {% measure value="sum(first_blood_differential)" title="+/- FB" fmt="num0" /%}
    {% measure value="sum(match_mvp_count)" title="MVPs" fmt="num0" /%}
  {% /table %}
{% /row %}

{% row %}
  {% big_value
    data="player_overall_dossier"
    value="career_kd_ratio"
    title="🔵 Player 1 Career K/D"
    fmt="num2"
    filters=["player_a"]
  /%}

  {% big_value
    data="player_overall_dossier"
    value="career_avg_acs"
    title="🔵 Player 1 Avg ACS"
    fmt="num1"
    filters=["player_a"]
  /%}

  {% big_value
    data="player_overall_dossier"
    value="career_kd_ratio"
    title="🔴 Player 2 Career K/D"
    fmt="num2"
    filters=["player_b"]
  /%}

  {% big_value
    data="player_overall_dossier"
    value="career_avg_acs"
    title="🔴 Player 2 Avg ACS"
    fmt="num1"
    filters=["player_b"]
  /%}
{% /row %}

---

## 🗺️ Map-by-Map Performance Comparison & Rotation Decider

Compare both players' efficiency on every map to make data-backed substitution and rotation decisions:

{% row %}
  {% table data="player_maps_dossier" filters=["player_a"] repeat_values=true %}
    {% dimension value="map_name" title="🔵 Player 1 Map" /%}
    {% measure value="sum(map_matches)" title="Games" fmt="num0" /%}
    {% measure value="sum(map_wins)" title="Wins" fmt="num0" /%}
    {% measure value="avg(map_win_ratio)" title="Win %" fmt="pct1" /%}
    {% measure value="avg(map_kd_ratio)" title="K/D" fmt="num2" /%}
    {% measure value="avg(map_avg_acs)" title="Avg ACS" fmt="num1" /%}
    {% measure value="avg(map_hs_ratio)" title="HS %" fmt="pct1" /%}
  {% /table %}

  {% table data="player_maps_dossier" filters=["player_b"] repeat_values=true %}
    {% dimension value="map_name" title="🔴 Player 2 Map" /%}
    {% measure value="sum(map_matches)" title="Games" fmt="num0" /%}
    {% measure value="sum(map_wins)" title="Wins" fmt="num0" /%}
    {% measure value="avg(map_win_ratio)" title="Win %" fmt="pct1" /%}
    {% measure value="avg(map_kd_ratio)" title="K/D" fmt="num2" /%}
    {% measure value="avg(map_avg_acs)" title="Avg ACS" fmt="num1" /%}
    {% measure value="avg(map_hs_ratio)" title="HS %" fmt="pct1" /%}
  {% /table %}
{% /row %}

---

## 🎭 Agent Pool Mastery Comparison

Compare both players across their deployed agent pools to see who pilots which agent with higher impact:

{% row %}
  {% table data="player_agents_dossier" filters=["player_a"] repeat_values=true %}
    {% dimension value="agent_name" title="🔵 Player 1 Agent" /%}
    {% dimension value="agent_role" title="Role" /%}
    {% measure value="sum(agent_matches)" title="Picks" fmt="num0" /%}
    {% measure value="avg(agent_win_ratio)" title="Win %" fmt="pct1" /%}
    {% measure value="avg(agent_kd_ratio)" title="K/D" fmt="num2" /%}
    {% measure value="avg(agent_avg_acs)" title="Avg ACS" fmt="num1" /%}
    {% measure value="avg(agent_hs_ratio)" title="HS %" fmt="pct1" /%}
  {% /table %}

  {% table data="player_agents_dossier" filters=["player_b"] repeat_values=true %}
    {% dimension value="agent_name" title="🔴 Player 2 Agent" /%}
    {% dimension value="agent_role" title="Role" /%}
    {% measure value="sum(agent_matches)" title="Picks" fmt="num0" /%}
    {% measure value="avg(agent_win_ratio)" title="Win %" fmt="pct1" /%}
    {% measure value="avg(agent_kd_ratio)" title="K/D" fmt="num2" /%}
    {% measure value="avg(agent_avg_acs)" title="Avg ACS" fmt="num1" /%}
    {% measure value="avg(agent_hs_ratio)" title="HS %" fmt="pct1" /%}
  {% /table %}
{% /row %}

---

## 🛡️ Tactical Role Mastery (Duelist / Initiator / Controller / Sentinel)

Evaluate tactical archetype versatility and win rates between both players:

{% row %}
  {% table data="player_roles_dossier" filters=["player_a"] repeat_values=true %}
    {% dimension value="agent_role" title="🔵 Player 1 Role" /%}
    {% measure value="sum(role_matches)" title="Picks" fmt="num0" /%}
    {% measure value="avg(role_win_ratio)" title="Role Win %" fmt="pct1" /%}
    {% measure value="avg(role_kd_ratio)" title="Role K/D" fmt="num2" /%}
    {% measure value="avg(role_avg_acs)" title="Avg ACS" fmt="num1" /%}
  {% /table %}

  {% table data="player_roles_dossier" filters=["player_b"] repeat_values=true %}
    {% dimension value="agent_role" title="🔴 Player 2 Role" /%}
    {% measure value="sum(role_matches)" title="Picks" fmt="num0" /%}
    {% measure value="avg(role_win_ratio)" title="Role Win %" fmt="pct1" /%}
    {% measure value="avg(role_kd_ratio)" title="Role K/D" fmt="num2" /%}
    {% measure value="avg(role_avg_acs)" title="Avg ACS" fmt="num1" /%}
  {% /table %}
{% /row %}

---

## 🏆 Master Squad Map Rotation Guide (All Core Members)

Quick-reference squad guide detailing which team member has the absolute highest win rate and fragger rating on each terrain:

{% table data="squad_rotation_matrix" repeat_values=true %}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="highest_winrate_player" title="🥇 Highest Win Rate Specialist" /%}
  {% dimension value="highest_impact_fragger" title="🎯 Top Fragger & ACS Leader" /%}
{% /table %}
