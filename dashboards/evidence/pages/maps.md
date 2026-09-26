---
title: Map Strategy & Tier Rankings
description: Interactive Tracker-style map tactical deep dive with high-resolution Riot splash artwork, side bias, attack vs defense round splits, winning agent compositions, and pick/ban tiers.
---

# 🗺️ Tactical Map Command Center

Select any map from the rotation below to view high-resolution Riot artwork, team win rates, attack vs. defense round conversion, side bias, and winning agent lineups.

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
    attack_rounds_won,
    attack_rounds_played,
    defense_rounds_won,
    defense_rounds_played,
    attack_win_pct / 100.0 AS atk_win_ratio,
    defense_win_pct / 100.0 AS def_win_ratio,
    attack_start_matches,
    attack_start_win_pct / 100.0 AS atk_start_win_ratio,
    defense_start_matches,
    defense_start_win_pct / 100.0 AS def_start_win_ratio,
    CASE 
        WHEN (attack_win_pct - defense_win_pct) >= 5.0 THEN '⚔️ Attack Dominant Map'
        WHEN (defense_win_pct - attack_win_pct) >= 5.0 THEN '🛡️ Defense Fortress Map'
        ELSE '⚖️ Balanced Battlefield'
    END AS tactical_side_category,
    CONCAT(attack_rounds_won, ' / ', attack_rounds_played, ' (', CAST(attack_win_pct AS STRING), '%)') AS attack_rounds_summary,
    CONCAT(defense_rounds_won, ' / ', defense_rounds_played, ' (', CAST(defense_win_pct AS STRING), '%)') AS defense_rounds_summary,
    side_bias,
    thrifty_rounds_won
FROM valorant.gold.gold_map_performance
ORDER BY matches_played DESC
```

```sql map_side_breakdown
SELECT 
    map_name,
    '⚔️ Attack' AS tactical_side,
    attack_rounds_won AS rounds_won,
    (attack_rounds_played - attack_rounds_won) AS rounds_lost,
    attack_rounds_played AS rounds_played,
    attack_win_pct / 100.0 AS side_win_ratio,
    CASE 
        WHEN (attack_win_pct - defense_win_pct) >= 5.0 THEN '🔥 Squad Stronghold'
        WHEN (defense_win_pct - attack_win_pct) >= 5.0 THEN '⚠️ Tactical Struggle'
        ELSE '⚖️ Stable Execution'
    END AS side_verdict
FROM valorant.gold.gold_map_performance
UNION ALL
SELECT 
    map_name,
    '🛡️ Defense' AS tactical_side,
    defense_rounds_won AS rounds_won,
    (defense_rounds_played - defense_rounds_won) AS rounds_lost,
    defense_rounds_played AS rounds_played,
    defense_win_pct / 100.0 AS side_win_ratio,
    CASE 
        WHEN (defense_win_pct - attack_win_pct) >= 5.0 THEN '🔥 Squad Stronghold'
        WHEN (attack_win_pct - defense_win_pct) >= 5.0 THEN '⚠️ Tactical Struggle'
        ELSE '⚖️ Stable Execution'
    END AS side_verdict
FROM valorant.gold.gold_map_performance
```

```sql all_maps_attack_defense_matrix
SELECT 
    map_name,
    map_tier,
    CASE 
        WHEN (attack_win_pct - defense_win_pct) >= 5.0 THEN '⚔️ Attack Dominant'
        WHEN (defense_win_pct - attack_win_pct) >= 5.0 THEN '🛡️ Defense Fortress'
        ELSE '⚖️ Balanced Battlefield'
    END AS terrain_classification,
    CONCAT(attack_rounds_won, ' / ', attack_rounds_played, ' (', CAST(attack_win_pct AS STRING), '%)') AS attack_rounds_display,
    attack_win_pct / 100.0 AS atk_win_ratio,
    CONCAT(defense_rounds_won, ' / ', defense_rounds_played, ' (', CAST(defense_win_pct AS STRING), '%)') AS defense_rounds_display,
    defense_win_pct / 100.0 AS def_win_ratio,
    ROUND(attack_win_pct - defense_win_pct, 1) AS net_attack_lead_pct,
    CASE
        WHEN (attack_win_pct - defense_win_pct) >= 10.0 THEN '🔥 Heavy Attack Dominance'
        WHEN (attack_win_pct - defense_win_pct) >= 5.0 THEN '⚔️ Attack Favored'
        WHEN (defense_win_pct - attack_win_pct) >= 10.0 THEN '🏰 Impenetrable Defense'
        WHEN (defense_win_pct - attack_win_pct) >= 5.0 THEN '🛡️ Defense Favored'
        ELSE '⚖️ True Neutral / Even'
    END AS tactical_verdict
FROM valorant.gold.gold_map_performance
ORDER BY (attack_win_pct - defense_win_pct) DESC
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
    {% dimension value="tactical_side_category" title="Terrain Verdict" /%}
    {% dimension value="side_bias" title="Tactical Bias" /%}
    {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
    {% measure value="sum(matches_lost)" title="Losses" fmt="num0" /%}
    {% measure value="avg(atk_win_ratio)" title="Overall Atk Win %" fmt="pct1" /%}
    {% measure value="avg(def_win_ratio)" title="Overall Def Win %" fmt="pct1" /%}
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

### ⚖️ Attack vs. Defense Mastery & Terrain Classification

Comprehensive evaluation of our squad's attacking vs. defending efficiency on this map, irrespective of coin-toss starting side:

{% row %}
  {% big_value
    data="all_map_data"
    value="tactical_side_category"
    title="Map Tactical Classification"
    filters=["map_filter"]
  /%}

  {% big_value
    data="all_map_data"
    value="attack_rounds_summary"
    title="⚔️ Attack Rounds Won / Total (%)"
    filters=["map_filter"]
  /%}

  {% big_value
    data="all_map_data"
    value="defense_rounds_summary"
    title="🛡️ Defense Rounds Won / Total (%)"
    filters=["map_filter"]
  /%}

  {% big_value
    data="all_map_data"
    value="side_bias"
    title="Squad Advantage Bias"
    filters=["map_filter"]
  /%}
{% /row %}

#### 📊 Detailed Side-by-Side Round Conversion Table

{% table data="map_side_breakdown" filters=["map_filter"] %}
  {% dimension value="tactical_side" title="Tactical Side" /%}
  {% dimension value="side_verdict" title="Performance Verdict" /%}
  {% measure value="sum(rounds_won)" title="Rounds Won" fmt="num0" /%}
  {% measure value="sum(rounds_lost)" title="Rounds Lost" fmt="num0" /%}
  {% measure value="sum(rounds_played)" title="Total Rounds" fmt="num0" /%}
  {% measure value="avg(side_win_ratio)" title="Side Win %" fmt="pct1" /%}
{% /table %}

{% row %}
  {% bar_chart
    data="all_map_data"
    x="map_name"
    y="atk_win_ratio"
    y2="def_win_ratio"
    title="Overall Round Conversion: Attack vs. Defense"
    y_fmt="pct1"
    filters=["map_filter"]
  /%}

  {% bar_chart
    data="all_map_data"
    x="map_name"
    y="atk_start_win_ratio"
    y2="def_start_win_ratio"
    title="Coin-Toss Starting Advantage (Match Win %)"
    y_fmt="pct1"
    filters=["map_filter"]
  /%}
{% /row %}

---

## ⚔️ All Maps: Attack vs. Defense Master Matrix

At-a-glance comparison across all maps in the competitive pool to instantly see which battlefields are **Attack Dominant**, **Defense Fortresses**, or **Balanced**, along with exact rounds won, played, and side conversion percentages:

{% table data="all_maps_attack_defense_matrix" repeat_values=true %}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="map_tier" title="Competitive Tier" /%}
  {% dimension value="terrain_classification" title="Terrain Classification" /%}
  {% dimension value="attack_rounds_display" title="⚔️ Attack Rounds (Win %)" /%}
  {% dimension value="defense_rounds_display" title="🛡️ Defense Rounds (Win %)" /%}
  {% measure value="avg(net_attack_lead_pct)" title="Net Atk Spread (+/- %)" fmt="num1" /%}
  {% dimension value="tactical_verdict" title="Tactical Verdict" /%}
{% /table %}

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
  {% dimension value="tactical_side_category" title="Tactical Classification" /%}
  {% dimension value="attack_rounds_summary" title="⚔️ Atk Rounds (Win %)" /%}
  {% dimension value="defense_rounds_summary" title="🛡️ Def Rounds (Win %)" /%}
  {% measure value="sum(matches_played)" title="Played" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="sum(matches_lost)" title="Losses" fmt="num0" /%}
  {% measure value="avg(map_win_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(team_kd_ratio)" title="Squad K/D" fmt="num2" /%}
  {% measure value="sum(round_differential)" title="+/- Rounds" fmt="num0" /%}
  {% dimension value="side_bias" title="Squad Bias" /%}
{% /table %}
