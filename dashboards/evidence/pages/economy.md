# 💰 Economy & Buy Tier Performance

Strategic buy decisions, anti-eco discipline, and thrifty round conversion across all competitive matches.

```sql economy_kpis
SELECT 
    SUM(rounds_played) AS total_rounds,
    SUM(rounds_won) AS total_won,
    SUM(rounds_lost) AS total_lost,
    SUM(rounds_won) * 1.0 / NULLIF(SUM(rounds_played), 0) AS overall_win_ratio,
    SUM(CASE WHEN our_buy_tier = 'Full Buy' AND opponent_buy_tier = 'Full Buy' THEN rounds_won ELSE 0 END) * 1.0 /
        NULLIF(SUM(CASE WHEN our_buy_tier = 'Full Buy' AND opponent_buy_tier = 'Full Buy' THEN rounds_played ELSE 0 END), 0) AS full_buy_win_ratio,
    SUM(thrifty_rounds_won) AS total_thrifties,
    SUM(anti_eco_throw_count) AS total_anti_eco_throws,
    SUM(team_kills) AS total_kills,
    SUM(team_deaths) AS total_deaths,
    SUM(team_kills) * 1.0 / NULLIF(SUM(team_deaths), 0) AS squad_kd_ratio
FROM valorant.gold.gold_economy_performance
```

{% row %}
  {% big_value
    data="economy_kpis"
    value="total_rounds"
    title="Recorded Buy Rounds"
    fmt="num0"
  /%}

  {% big_value
    data="economy_kpis"
    value="full_buy_win_ratio"
    title="Full Buy vs Full Buy Win %"
    fmt="pct1"
  /%}

  {% big_value
    data="economy_kpis"
    value="total_thrifties"
    title="Thrifty Rounds Won"
    fmt="num0"
  /%}

  {% big_value
    data="economy_kpis"
    value="total_anti_eco_throws"
    title="Anti-Eco Rounds Thrown"
    fmt="num0"
  /%}

  {% big_value
    data="economy_kpis"
    value="squad_kd_ratio"
    title="Overall Economic K/D"
    fmt="num2"
  /%}
{% /row %}

---

## ⚖️ Buy Tier Matchup Analysis

How the squad converts when operating across various loadout investments.

```sql buy_tier_breakdown
SELECT 
    our_buy_tier,
    SUM(rounds_played) AS rounds_played,
    SUM(rounds_won) AS rounds_won,
    SUM(rounds_lost) AS rounds_lost,
    SUM(rounds_won) * 1.0 / NULLIF(SUM(rounds_played), 0) AS win_ratio,
    ROUND(AVG(avg_our_loadout_value), 0) AS avg_credits_spent,
    ROUND(AVG(team_kd_ratio), 2) AS avg_kd
FROM valorant.gold.gold_economy_performance
GROUP BY our_buy_tier
ORDER BY 
    CASE our_buy_tier
        WHEN 'Full Buy' THEN 1
        WHEN 'Semi-Buy' THEN 2
        WHEN 'Semi-Eco' THEN 3
        WHEN 'Eco' THEN 4
        ELSE 5
    END
```

{% row %}
  {% bar_chart
    data="buy_tier_breakdown"
    x="our_buy_tier"
    y="win_ratio"
    title="Win Rate % by Our Team Buy Tier"
    y_fmt="pct1"
  /%}

  {% bar_chart
    data="buy_tier_breakdown"
    x="our_buy_tier"
    y="rounds_played"
    title="Round Volume by Buy Tier"
    y_fmt="num0"
  /%}
{% /row %}

---

## 🎯 Head-to-Head Buy Tier Matchup Matrix

Detailed conversion across all economic engagements.

```sql matchup_matrix
SELECT 
    buy_matchup,
    our_buy_tier,
    opponent_buy_tier,
    rounds_played,
    rounds_won,
    rounds_lost,
    matchup_win_pct / 100.0 AS win_ratio,
    avg_our_loadout_value,
    avg_opponent_loadout_value,
    avg_loadout_advantage,
    team_kills,
    team_deaths,
    ROUND(team_kd_ratio, 2) AS team_kd,
    thrifty_rounds_won,
    anti_eco_throw_count
FROM valorant.gold.gold_economy_performance
ORDER BY rounds_played DESC
```

{% table data="matchup_matrix" %}
  {% dimension value="buy_matchup" title="Matchup" /%}
  {% dimension value="our_buy_tier" title="Our Tier" /%}
  {% dimension value="opponent_buy_tier" title="Enemy Tier" /%}
  {% measure value="rounds_played" title="Rounds" /%}
  {% measure value="rounds_won" title="Won" /%}
  {% measure value="win_ratio" title="Win %" fmt="pct1" /%}
  {% measure value="avg_our_loadout_value" title="Our Avg Value" fmt="num0" /%}
  {% measure value="avg_opponent_loadout_value" title="Enemy Avg Value" fmt="num0" /%}
  {% measure value="avg_loadout_advantage" title="Net Advantage" fmt="num0" /%}
  {% measure value="team_kd" title="K/D" /%}
  {% measure value="thrifty_rounds_won" title="Thrifties" /%}
  {% measure value="anti_eco_throw_count" title="Throws" /%}
{% /table %}

---

## 🛡️ Economic Discipline: Thrifty & Anti-Eco Indicators

Understanding round efficiency when overcoming deficits or protecting advantages.

```sql thrifty_vs_anticos
SELECT 
    buy_matchup,
    rounds_played,
    thrifty_rounds_won,
    thrifty_conversion_pct / 100.0 AS thrifty_conversion_ratio,
    anti_eco_throw_count,
    avg_loadout_advantage
FROM valorant.gold.gold_economy_performance
WHERE thrifty_rounds_won > 0 OR anti_eco_throw_count > 0
ORDER BY (thrifty_rounds_won + anti_eco_throw_count) DESC
```

{% table data="thrifty_vs_anticos" %}
  {% dimension value="buy_matchup" title="Matchup Scenario" /%}
  {% measure value="rounds_played" title="Total Occurrences" /%}
  {% measure value="thrifty_rounds_won" title="Thrifty Rounds Won" /%}
  {% measure value="thrifty_conversion_ratio" title="Thrifty Conversion %" fmt="pct1" /%}
  {% measure value="anti_eco_throw_count" title="Anti-Eco Rounds Thrown" /%}
  {% measure value="avg_loadout_advantage" title="Avg Loadout Advantage" fmt="num0" /%}
{% /table %}
