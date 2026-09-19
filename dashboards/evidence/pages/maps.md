---
title: Map Strategy & Tier Rankings
description: Interactive Tracker-style map tactical deep dive with high-resolution Riot splash artwork, side bias, and pick/ban tiers.
---

# 🗺️ Tactical Map Command Center

Select any map from the rotation below to view high-resolution Riot artwork, team win rates, side bias, and round conversion splits.

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
