---
title: Weapons & Combat Mastery
description: Arsenal lethal efficiency, Vandal vs Phantom rifle duel, accuracy breakdowns, and squad specialist trophy badges.
---

# 🔫 Arsenal & Combat Mastery — Weapon Analytics

Deep-dive combat intelligence for **OUR_TEAM** across all competitive matches, sourced from `valorant.gold.gold_combat_performance`.

```sql squad_combat_kpis
SELECT 
    SUM(total_kills) AS total_squad_kills,
    SUM(first_bloods_secured) AS total_first_bloods,
    SUM(trade_kills_secured) AS total_trade_kills,
    ROUND(SUM(headshots) * 1.0 / NULLIF(SUM(headshots + bodyshots + legshots), 0), 3) AS overall_hs_ratio
FROM valorant.gold.gold_combat_performance
WHERE player_puuid = 'ALL_SQUAD'
```

```sql rifle_duel
SELECT 
    weapon_name,
    total_kills,
    weapon_round_win_pct / 100.0 AS win_rate_ratio,
    weapon_headshot_pct / 100.0 AS hs_ratio,
    avg_damage_per_round AS adr,
    first_bloods_secured AS first_bloods,
    top_specialist_name
FROM valorant.gold.gold_combat_performance
WHERE player_puuid = 'ALL_SQUAD'
  AND weapon_name IN ('Vandal', 'Phantom')
ORDER BY total_kills DESC
```

```sql top_weapons_volume
SELECT 
    weapon_name,
    weapon_category,
    total_kills,
    kill_share_pct / 100.0 AS kill_share_ratio,
    weapon_headshot_pct / 100.0 AS hs_ratio,
    weapon_round_win_pct / 100.0 AS win_rate_ratio,
    rounds_equipped,
    credits_per_kill,
    top_specialist_name,
    specialist_badge
FROM valorant.gold.gold_combat_performance
WHERE player_puuid = 'ALL_SQUAD'
  AND total_kills > 0
ORDER BY total_kills DESC
LIMIT 12
```

```sql category_breakdown
SELECT 
    weapon_category,
    SUM(total_kills) AS category_kills,
    SUM(first_bloods_secured) AS category_first_bloods,
    ROUND(SUM(headshots) * 1.0 / NULLIF(SUM(headshots + bodyshots + legshots), 0), 3) AS category_hs_ratio
FROM valorant.gold.gold_combat_performance
WHERE player_puuid = 'ALL_SQUAD'
  AND weapon_category IS NOT NULL
GROUP BY weapon_category
ORDER BY category_kills DESC
```

```sql squad_specialist_roster
SELECT 
    current_display_name AS player_name,
    weapon_name,
    weapon_category,
    total_kills,
    weapon_headshot_pct / 100.0 AS hs_ratio,
    weapon_round_win_pct / 100.0 AS win_rate_ratio,
    first_bloods_secured AS first_bloods,
    specialist_badge
FROM valorant.gold.gold_combat_performance
WHERE player_puuid != 'ALL_SQUAD'
  AND is_squad_weapon_specialist = true
ORDER BY total_kills DESC
```

---

## 🎯 Combat Lethality & Accuracy Overview

{% row %}
  {% big_value
    data="squad_combat_kpis"
    value="sum(total_squad_kills)"
    title="Total Fatal Squad Kills"
  /%}

  {% big_value
    data="squad_combat_kpis"
    value="avg(overall_hs_ratio)"
    title="Squad Headshot Ratio"
    fmt="pct1"
  /%}

  {% big_value
    data="squad_combat_kpis"
    value="sum(total_first_bloods)"
    title="Opening Kills (First Bloods)"
  /%}

  {% big_value
    data="squad_combat_kpis"
    value="sum(total_trade_kills)"
    title="Trade Kills Secured"
  /%}
{% /row %}

---

## ⚔️ The Rifle Duel: Vandal vs. Phantom Meta

{% row %}
  {% bar_chart
    data="rifle_duel"
    x="weapon_name"
    y="total_kills"
    title="Total Fatal Kills: Vandal vs. Phantom"
  /%}

  {% bar_chart
    data="rifle_duel"
    x="weapon_name"
    y="hs_ratio"
    title="Headshot Accuracy Comparison"
    y_fmt="pct1"
  /%}
{% /row %}

{% table data="rifle_duel" %}
  {% dimension value="weapon_name" title="Primary Rifle" /%}
  {% measure value="sum(total_kills)" title="Kills" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Round Win Rate" fmt="pct1" /%}
  {% measure value="avg(hs_ratio)" title="Headshot %" fmt="pct1" /%}
  {% measure value="avg(adr)" title="Avg Damage / Round" fmt="num1" /%}
  {% measure value="sum(first_bloods)" title="First Bloods" fmt="num0" /%}
  {% dimension value="top_specialist_name" title="🏆 Top Squad Specialist" /%}
{% /table %}

---

## 📊 Weapon Arsenal Distribution & Category Power

{% row %}
  {% bar_chart
    data="top_weapons_volume"
    x="weapon_name"
    y="total_kills"
    title="Top 12 Most Lethal Weapons by Squad Kills"
    order="total_kills desc"
  /%}

  {% bar_chart
    data="category_breakdown"
    x="weapon_category"
    y="category_kills"
    title="Fatal Kills by Weapon Category"
    order="category_kills desc"
  /%}
{% /row %}

---

## 🏆 Squad Weapon Specialists & Mastery Badges

Who commands each weapon with maximum lethality within OUR_TEAM?

{% table data="squad_specialist_roster" repeat_values=true %}
  {% dimension value="player_name" title="Player" /%}
  {% dimension value="weapon_name" title="Weapon" /%}
  {% dimension value="weapon_category" title="Class" /%}
  {% measure value="sum(total_kills)" title="Kills" fmt="num0" /%}
  {% measure value="avg(hs_ratio)" title="HS %" fmt="pct1" /%}
  {% measure value="avg(win_rate_ratio)" title="Round Win %" fmt="pct1" /%}
  {% measure value="sum(first_bloods)" title="First Bloods" fmt="num0" /%}
  {% dimension value="specialist_badge" title="Trophy Badge" /%}
{% /table %}

---

## 📋 Full Weapon Arsenal Performance Matrix

{% table data="top_weapons_volume" repeat_values=true %}
  {% dimension value="weapon_name" title="Weapon" /%}
  {% dimension value="weapon_category" title="Category" /%}
  {% measure value="sum(total_kills)" title="Kills" fmt="num0" /%}
  {% measure value="avg(kill_share_ratio)" title="Kill Share" fmt="pct1" /%}
  {% measure value="avg(hs_ratio)" title="HS %" fmt="pct1" /%}
  {% measure value="avg(win_rate_ratio)" title="Round Win %" fmt="pct1" /%}
  {% measure value="sum(rounds_equipped)" title="Rounds Used" fmt="num0" /%}
  {% measure value="avg(credits_per_kill)" title="Credits / Kill" fmt="num0" /%}
  {% dimension value="specialist_badge" title="Top Specialist Tag" /%}
{% /table %}
