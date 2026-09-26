---
title: Map Strategy & Tier Rankings
description: Interactive Tracker-style map tactical deep dive with high-resolution Riot splash artwork, side bias, winning agent compositions, and pick/ban tiers.
---

# 🗺️ Tactical Map Command Center

Select any map from the rotation below to view high-resolution Riot artwork, team win rates, side bias, round conversion splits, and winning agent lineups.

```sql all_map_data
SELECT 
    map_name,
    CONCAT(map_name, ' (', map_tier, ' — ', matches_played, ' matches)') AS map_label,
    map_tier,
    map_splash_url,
    bomb_site_count,
    matches_played,
    matches_won,
    matches_lost,
    map_win_pct / 100.0 AS map_win_ratio,
    round_differential,
    team_kd_ratio,
    attack_win_pct / 100.0 AS atk_win_ratio,
    defense_win_pct / 100.0 AS def_win_ratio,
    attack_start_matches,
    attack_start_win_pct / 100.0 AS atk_start_win_ratio,
    defense_start_matches,
    defense_start_win_pct / 100.0 AS def_start_win_ratio,
    side_bias,
    thrifty_rounds_won
FROM valorant.gold.gold_map_performance
ORDER BY matches_played DESC
```

```sql map_agent_comps
WITH match_comps AS (
    SELECT 
        m.match_id,
        m.map_name,
        m.is_our_team_win,
        array_join(array_sort(collect_set(p.agent_name)), ', ') AS comp_lineup
    FROM valorant.gold.gold_match_summary m
    JOIN valorant.gold.gold_player_match_performance p ON m.match_id = p.match_id
    GROUP BY m.match_id, m.map_name, m.is_our_team_win
)
SELECT 
    map_name,
    comp_lineup,
    COUNT(*) AS matches_played,
    SUM(CASE WHEN is_our_team_win = true THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN is_our_team_win = false THEN 1 ELSE 0 END) AS losses,
    ROUND(SUM(CASE WHEN is_our_team_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3) AS win_rate_ratio,
    CASE 
        WHEN SUM(CASE WHEN is_our_team_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*) >= 0.70 THEN '🔥 Dominant Meta'
        WHEN SUM(CASE WHEN is_our_team_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*) >= 0.50 THEN '✅ Viable Lineup'
        ELSE '⚠️ High-Risk Comp'
    END AS comp_viability
FROM match_comps
GROUP BY map_name, comp_lineup
ORDER BY matches_played DESC, win_rate_ratio DESC
```

```sql map_agent_winrates
SELECT 
    m.map_name,
    p.agent_name,
    p.agent_role,
    COUNT(DISTINCT m.match_id) AS agent_matches,
    COUNT(DISTINCT CASE WHEN m.is_our_team_win = true THEN m.match_id END) AS agent_wins,
    COUNT(DISTINCT CASE WHEN m.is_our_team_win = false THEN m.match_id END) AS agent_losses,
    ROUND(COUNT(DISTINCT CASE WHEN m.is_our_team_win = true THEN m.match_id END) * 1.0 / COUNT(DISTINCT m.match_id), 3) AS agent_win_ratio,
    ROUND(SUM(p.kills) * 1.0 / NULLIF(SUM(p.deaths), 0), 2) AS agent_kd,
    ROUND(AVG(p.average_combat_score), 1) AS agent_avg_acs,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN m.is_our_team_win = true THEN m.match_id END) * 1.0 / COUNT(DISTINCT m.match_id) >= 0.65 THEN '⭐ Must-Pick'
        WHEN COUNT(DISTINCT CASE WHEN m.is_our_team_win = true THEN m.match_id END) * 1.0 / COUNT(DISTINCT m.match_id) >= 0.50 THEN 'Solid Pick'
        ELSE '❌ Avoid / Low Win Rate'
    END AS pick_recommendation
FROM valorant.gold.gold_match_summary m
JOIN valorant.gold.gold_player_match_performance p ON m.match_id = p.match_id
GROUP BY m.map_name, p.agent_name, p.agent_role
ORDER BY agent_win_ratio DESC, agent_matches DESC
```

```sql global_map_blueprint
WITH match_comps AS (
    SELECT 
        m.match_id,
        m.map_name,
        m.is_our_team_win,
        array_join(array_sort(collect_set(p.agent_name)), ', ') AS comp_lineup
    FROM valorant.gold.gold_match_summary m
    JOIN valorant.gold.gold_player_match_performance p ON m.match_id = p.match_id
    GROUP BY m.match_id, m.map_name, m.is_our_team_win
),
comp_stats AS (
    SELECT 
        map_name,
        comp_lineup,
        COUNT(*) AS comp_matches,
        SUM(CASE WHEN is_our_team_win = true THEN 1 ELSE 0 END) AS comp_wins,
        ROUND(SUM(CASE WHEN is_our_team_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 3) AS comp_win_ratio,
        ROW_NUMBER() OVER (
            PARTITION BY map_name 
            ORDER BY 
                SUM(CASE WHEN is_our_team_win = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*) DESC, 
                COUNT(*) DESC
        ) AS rn
    FROM match_comps
    GROUP BY map_name, comp_lineup
    HAVING COUNT(*) >= 2
)
SELECT 
    g.map_name,
    g.map_tier,
    g.matches_played AS total_map_matches,
    g.map_win_pct / 100.0 AS map_win_ratio,
    COALESCE(c.comp_lineup, 'Custom / Varied Lineups') AS optimal_5man_comp,
    COALESCE(c.comp_matches, 0) AS comp_matches_played,
    COALESCE(c.comp_win_ratio, g.map_win_pct / 100.0) AS comp_win_ratio,
    g.side_bias
FROM valorant.gold.gold_map_performance g
LEFT JOIN comp_stats c ON g.map_name = c.map_name AND c.rn = 1
ORDER BY g.matches_played DESC
```

---

## 🎯 Select Map to Inspect

{% dropdown 
    id="map_filter" 
    data="all_map_data" 
    value_column="map_name" 
    label_column="map_label"
    select_first=true 
    title="Choose Map" 
/%}

---

## 🖼️ Selected Map Tactical Dossier

{% row %}
  {% image 
    data="all_map_data" 
    column="map_splash_url" 
    filters=["map_filter"]
    border=true
    max_width=480
    description="Valorant Official Map Artwork" 
  /%}

  {% table data="all_map_data" filters=["map_filter"] %}
    {% dimension value="map_name" title="Map" /%}
    {% dimension value="map_tier" title="Competitive Tier" /%}
    {% dimension value="side_bias" title="Tactical Bias" /%}
    {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
    {% measure value="sum(matches_lost)" title="Losses" fmt="num0" /%}
    {% measure value="avg(atk_start_win_ratio)" title="Atk Start Win %" fmt="pct1" /%}
    {% measure value="avg(def_start_win_ratio)" title="Def Start Win %" fmt="pct1" /%}
    {% measure value="sum(thrifty_rounds_won)" title="Thrifty Rounds" fmt="num0" /%}
  {% /table %}
{% /row %}

{% row %}
  {% big_value
    data="all_map_data"
    value="sum(matches_played)"
    title="Matches Played"
    fmt="num0"
    filters=["map_filter"]
  /%}

  {% big_value
    data="all_map_data"
    value="avg(map_win_ratio)"
    title="Match Win Rate"
    fmt="pct1"
    filters=["map_filter"]
  /%}

  {% big_value
    data="all_map_data"
    value="avg(team_kd_ratio)"
    title="Squad K/D Ratio"
    fmt="num2"
    filters=["map_filter"]
  /%}

  {% big_value
    data="all_map_data"
    value="sum(round_differential)"
    title="Net Round Differential"
    fmt="num0"
    filters=["map_filter"]
  /%}
{% /row %}

---

### 🛡️ Optimal 5-Agent Lineups on Selected Map

Historical performance of our squad's 5-agent team combinations on this map:

{% table data="map_agent_comps" filters=["map_filter"] %}
  {% dimension value="comp_lineup" title="5-Agent Lineup" /%}
  {% dimension value="comp_viability" title="Viability" /%}
  {% measure value="sum(matches_played)" title="Played" fmt="num0" /%}
  {% measure value="sum(wins)" title="Wins" fmt="num0" /%}
  {% measure value="sum(losses)" title="Losses" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Comp Win %" fmt="pct1" /%}
{% /table %}

---

### ⭐ Agent Tactical Effectiveness & Recommendations

Individual agent performance, tactical roles, and win rates on this map:

{% table data="map_agent_winrates" filters=["map_filter"] %}
  {% dimension value="agent_name" title="Agent" /%}
  {% dimension value="agent_role" title="Role" /%}
  {% dimension value="pick_recommendation" title="Tactical Recommendation" /%}
  {% measure value="sum(agent_matches)" title="Matches" fmt="num0" /%}
  {% measure value="sum(agent_wins)" title="Wins" fmt="num0" /%}
  {% measure value="avg(agent_win_ratio)" title="Agent Win %" fmt="pct1" /%}
  {% measure value="avg(agent_kd)" title="Squad K/D" fmt="num2" /%}
  {% measure value="avg(agent_avg_acs)" title="Avg ACS" fmt="num1" /%}
{% /table %}

---

### ⚖️ Side Advantage & Tactical Conversion Breakdown

{% row %}
  {% bar_chart
    data="all_map_data"
    x="map_name"
    y="atk_start_win_ratio"
    y2="def_start_win_ratio"
    title="Starting Coin-Toss Advantage (Win %)"
    y_fmt="pct1"
    filters=["map_filter"]
  /%}

  {% bar_chart
    data="all_map_data"
    x="map_name"
    y="atk_win_ratio"
    y2="def_win_ratio"
    title="Round Conversion: Attack vs. Defense"
    y_fmt="pct1"
    filters=["map_filter"]
  /%}
{% /row %}

---

## 🏆 Squad Map-by-Map Winning Blueprint (All Maps)

Quick-reference tactical guide showing the optimal 5-agent combination for every map in rotation:

{% table data="global_map_blueprint" repeat_values=true %}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="map_tier" title="Map Tier" /%}
  {% dimension value="side_bias" title="Side Bias" /%}
  {% dimension value="optimal_5man_comp" title="🥇 Optimal 5-Agent Lineup" /%}
  {% measure value="sum(comp_matches_played)" title="Lineup Games" fmt="num0" /%}
  {% measure value="avg(comp_win_ratio)" title="Lineup Win %" fmt="pct1" /%}
  {% measure value="avg(map_win_ratio)" title="Overall Map Win %" fmt="pct1" /%}
{% /table %}

---

## 📋 Full Map Pool Competitive Tier Standings

Compare all maps in the competitive pool across the squad's history:

{% table data="all_map_data" repeat_values=true %}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="map_tier" title="Competitive Tier" /%}
  {% measure value="sum(matches_played)" title="Played" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="sum(matches_lost)" title="Losses" fmt="num0" /%}
  {% measure value="avg(map_win_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(team_kd_ratio)" title="Squad K/D" fmt="num2" /%}
  {% measure value="sum(round_differential)" title="+/- Rounds" fmt="num0" /%}
  {% dimension value="side_bias" title="Squad Bias" /%}
{% /table %}
