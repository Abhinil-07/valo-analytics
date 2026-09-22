---
title: Spike & Site Objectives
description: Bomb site execution preferences, post-plant hold win rates, and defense retake defusals across maps.
---

# 💣 Spike & Bomb Site Objectives

Tactical site execution preference, post-plant hold conversion, and defense retake efficiency across all competitive matches.

```sql objective_overview_kpis
SELECT 
    SUM(our_plants_count) AS total_plants,
    SUM(our_post_plant_wins) AS total_post_plant_wins,
    SUM(our_post_plant_wins) * 1.0 / NULLIF(SUM(our_plants_count), 0) AS squad_post_plant_win_ratio,
    SUM(our_spike_detonations) AS total_detonations,
    SUM(opponent_plants_count) AS total_enemy_plants,
    SUM(our_retake_defuses) AS total_retake_defuses,
    SUM(our_retake_defuses) * 1.0 / NULLIF(SUM(opponent_plants_count), 0) AS squad_retake_win_ratio,
    ROUND(AVG(avg_plant_time_seconds), 1) AS avg_squad_plant_seconds
FROM valorant.gold.gold_spike_performance
```

{% row %}
  {% big_value
    data="objective_overview_kpis"
    value="total_plants"
    title="Spikes Planted (Attack)"
    fmt="num0"
  /%}

  {% big_value
    data="objective_overview_kpis"
    value="squad_post_plant_win_ratio"
    title="Post-Plant Hold Win %"
    fmt="pct1"
  /%}

  {% big_value
    data="objective_overview_kpis"
    value="total_retake_defuses"
    title="Retake Defuses (Defense)"
    fmt="num0"
  /%}

  {% big_value
    data="objective_overview_kpis"
    value="squad_retake_win_ratio"
    title="Defense Retake Win %"
    fmt="pct1"
  /%}

  {% big_value
    data="objective_overview_kpis"
    value="avg_squad_plant_seconds"
    title="Avg Execution Pace"
    fmt="num1"
  /%}
{% /row %}

---

## 🗺️ Map-Specific Site Tactics & Conversion

```sql all_site_data
SELECT 
    map_name,
    site,
    our_plants_count,
    our_post_plant_wins,
    our_post_plant_win_pct / 100.0 AS post_plant_win_ratio,
    site_plant_preference_pct / 100.0 AS site_preference_ratio,
    ROUND(avg_plant_time_seconds, 1) AS avg_plant_seconds,
    opponent_plants_count,
    our_retake_defuses,
    our_retake_win_pct / 100.0 AS retake_win_ratio,
    top_planter_display_name AS top_planter,
    top_defuser_display_name AS top_defuser
FROM valorant.gold.gold_spike_performance
ORDER BY map_name, site ASC
```

{% dropdown 
    id="obj_map_filter" 
    data="all_site_data" 
    value_column="map_name" 
    label_column="map_name"
    select_first=true 
    title="Select Map" 
/%}

{% row %}
  {% bar_chart
    data="all_site_data"
    x="site"
    y="our_plants_count"
    title="Attack Site Execution Volume"
    y_fmt="num0"
    filters=["obj_map_filter"]
  /%}

  {% bar_chart
    data="all_site_data"
    x="site"
    y="post_plant_win_ratio"
    title="Post-Plant Hold Win Rate %"
    y_fmt="pct1"
    filters=["obj_map_filter"]
  /%}

  {% bar_chart
    data="all_site_data"
    x="site"
    y="retake_win_ratio"
    title="Defense Retake Win Rate %"
    y_fmt="pct1"
    filters=["obj_map_filter"]
  /%}
{% /row %}

{% table data="all_site_data" filters=["obj_map_filter"] %}
  {% dimension value="site" title="Bomb Site" /%}
  {% measure value="our_plants_count" title="Our Plants" /%}
  {% measure value="post_plant_win_ratio" title="Post-Plant Win %" fmt="pct1" /%}
  {% measure value="site_preference_ratio" title="Site Bias" fmt="pct1" /%}
  {% measure value="avg_plant_seconds" title="Avg Plant Pace (Sec)" fmt="num1" /%}
  {% measure value="opponent_plants_count" title="Enemy Plants" /%}
  {% measure value="retake_win_ratio" title="Defense Retake %" fmt="pct1" /%}
  {% dimension value="top_planter" title="Site Planter Leader" /%}
  {% dimension value="top_defuser" title="Site Defuser Leader" /%}
{% /table %}

---

## 📋 Comprehensive Squad Map & Site Matrix

All competitive bomb sites across the entire active map rotation.

{% table data="all_site_data" repeat_values=true %}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="site" title="Site" /%}
  {% measure value="our_plants_count" title="Plants" /%}
  {% measure value="post_plant_win_ratio" title="Post-Plant Win %" fmt="pct1" /%}
  {% measure value="site_preference_ratio" title="Site Bias" fmt="pct1" /%}
  {% measure value="avg_plant_seconds" title="Pace (Sec)" fmt="num1" /%}
  {% measure value="opponent_plants_count" title="Enemy Plants" /%}
  {% measure value="retake_win_ratio" title="Retake Win %" fmt="pct1" /%}
  {% dimension value="top_planter" title="Top Planter" /%}
  {% dimension value="top_defuser" title="Top Defuser" /%}
{% /table %}
