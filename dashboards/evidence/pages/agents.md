---
title: Agent Meta & Tactical Roles
description: Squad role distribution, interactive agent artwork dossiers, agent tier rankings, and specialist mastery badges.
---

# 🎭 Agent Meta & Tactical Role Intelligence

Explore the squad's agent selection hierarchy, tactical role balance across 211 competitive matches, and individual player mastery tiers.

```sql agent_dropdown_list
SELECT 
    agent_name,
    CONCAT(agent_name, ' (', agent_role, ' — ', SUM(matches_played), ' picks)') AS agent_label
FROM valorant.gold.gold_agent_performance
GROUP BY agent_name, agent_role
ORDER BY SUM(matches_played) DESC
```

```sql single_agent_dossier
SELECT 
    a.agent_name,
    a.agent_role,
    MAX(a.agent_icon_url) AS agent_icon_url,
    SUM(a.matches_played) AS total_picks,
    SUM(a.matches_won) AS total_wins,
    SUM(a.matches_lost) AS total_losses,
    ROUND(SUM(a.matches_won) * 1.0 / SUM(a.matches_played), 3) AS win_rate_ratio,
    ROUND(SUM(a.total_kills) * 1.0 / NULLIF(SUM(a.total_deaths), 0), 2) AS combined_kd,
    ROUND(AVG(a.avg_acs), 1) AS avg_acs,
    ROUND(AVG(a.avg_adr), 1) AS avg_adr,
    ROUND(AVG(a.headshot_pct) / 100.0, 3) AS hs_ratio,
    MAX(spec.top_specialist) AS top_specialist
FROM valorant.gold.gold_agent_performance a
LEFT JOIN (
    SELECT 
        agent_name,
        current_display_name AS top_specialist
    FROM (
        SELECT 
            ap.agent_name,
            op.current_display_name,
            ROW_NUMBER() OVER (PARTITION BY ap.agent_name ORDER BY ap.matches_played DESC) AS rn
        FROM valorant.gold.gold_agent_performance ap
        JOIN valorant.gold.gold_player_overall_summary op ON ap.player_puuid = op.player_puuid
    )
    WHERE rn = 1
) spec ON a.agent_name = spec.agent_name
GROUP BY a.agent_name, a.agent_role
```

```sql agent_players_breakdown
SELECT 
    a.agent_name,
    o.current_display_name AS player_name,
    a.matches_played,
    a.matches_won,
    a.matches_lost,
    a.agent_win_pct / 100.0 AS win_rate_ratio,
    a.kd_ratio,
    ROUND(a.avg_acs, 1) AS avg_acs,
    ROUND(a.avg_adr, 1) AS avg_adr,
    ROUND(a.headshot_pct / 100.0, 3) AS hs_ratio,
    a.mastery_tier
FROM valorant.gold.gold_agent_performance a
JOIN valorant.gold.gold_player_overall_summary o ON a.player_puuid = o.player_puuid
WHERE a.matches_played > 0
ORDER BY a.matches_played DESC
```

```sql player_role_winrate_matrix
SELECT 
    o.current_display_name AS player_name,
    ROUND(SUM(CASE WHEN a.agent_role = 'Sentinel' THEN a.matches_won ELSE 0 END) * 1.0 / 
          NULLIF(SUM(CASE WHEN a.agent_role = 'Sentinel' THEN a.matches_played ELSE 0 END), 0), 3) AS sentinel_win_ratio,
    ROUND(SUM(CASE WHEN a.agent_role = 'Duelist' THEN a.matches_won ELSE 0 END) * 1.0 / 
          NULLIF(SUM(CASE WHEN a.agent_role = 'Duelist' THEN a.matches_played ELSE 0 END), 0), 3) AS duelist_win_ratio,
    ROUND(SUM(CASE WHEN a.agent_role = 'Controller' THEN a.matches_won ELSE 0 END) * 1.0 / 
          NULLIF(SUM(CASE WHEN a.agent_role = 'Controller' THEN a.matches_played ELSE 0 END), 0), 3) AS controller_win_ratio,
    ROUND(SUM(CASE WHEN a.agent_role = 'Initiator' THEN a.matches_won ELSE 0 END) * 1.0 / 
          NULLIF(SUM(CASE WHEN a.agent_role = 'Initiator' THEN a.matches_played ELSE 0 END), 0), 3) AS initiator_win_ratio,
    ROUND(SUM(a.matches_won) * 1.0 / NULLIF(SUM(a.matches_played), 0), 3) AS overall_win_ratio
FROM valorant.gold.gold_agent_performance a
JOIN valorant.gold.gold_player_overall_summary o ON a.player_puuid = o.player_puuid
WHERE o.total_matches_played >= 10
GROUP BY o.current_display_name
ORDER BY SUM(a.matches_played) DESC
```

```sql meta_kpis
SELECT 
    COUNT(DISTINCT agent_name) AS unique_agents_played,
    SUM(matches_played) AS total_agent_deployments,
    ROUND(SUM(matches_won) * 100.0 / SUM(matches_played), 1) AS overall_agent_win_pct,
    ROUND(SUM(total_kills) * 1.0 / NULLIF(SUM(total_deaths), 0), 2) AS overall_team_kd
FROM valorant.gold.gold_agent_performance
```

```sql role_summary
SELECT 
    agent_role,
    COUNT(DISTINCT agent_name) AS unique_agents,
    SUM(matches_played) AS total_picks,
    SUM(matches_won) AS total_wins,
    SUM(matches_lost) AS total_losses,
    SUM(matches_won) * 1.0 / SUM(matches_played) AS win_rate_ratio,
    ROUND(SUM(total_kills) * 1.0 / NULLIF(SUM(total_deaths), 0), 2) AS role_kd,
    ROUND(AVG(avg_acs), 1) AS avg_acs,
    ROUND(AVG(avg_adr), 1) AS avg_adr,
    ROUND(AVG(headshot_pct) / 100.0, 3) AS hs_ratio
FROM valorant.gold.gold_agent_performance
GROUP BY agent_role
ORDER BY total_picks DESC
```

```sql player_role_breakdown
SELECT 
    o.current_display_name AS player_name,
    SUM(CASE WHEN a.agent_role = 'Sentinel' THEN a.matches_played ELSE 0 END) AS sentinel_picks,
    SUM(CASE WHEN a.agent_role = 'Duelist' THEN a.matches_played ELSE 0 END) AS duelist_picks,
    SUM(CASE WHEN a.agent_role = 'Controller' THEN a.matches_played ELSE 0 END) AS controller_picks,
    SUM(CASE WHEN a.agent_role = 'Initiator' THEN a.matches_played ELSE 0 END) AS initiator_picks,
    SUM(a.matches_played) AS total_picks
FROM valorant.gold.gold_agent_performance a
JOIN valorant.gold.gold_player_overall_summary o ON a.player_puuid = o.player_puuid
WHERE o.total_matches_played >= 10
GROUP BY o.current_display_name
ORDER BY total_picks DESC
```

```sql agent_tier_list
SELECT 
    agent_name,
    agent_role,
    SUM(matches_played) AS total_picks,
    SUM(matches_won) AS total_wins,
    SUM(matches_lost) AS total_losses,
    SUM(matches_won) * 1.0 / SUM(matches_played) AS win_rate_ratio,
    ROUND(SUM(total_kills) * 1.0 / NULLIF(SUM(total_deaths), 0), 2) AS combined_kd,
    ROUND(AVG(avg_acs), 1) AS team_avg_acs,
    ROUND(AVG(avg_adr), 1) AS team_avg_adr,
    ROUND(AVG(headshot_pct) / 100.0, 3) AS team_hs_ratio,
    CASE 
        WHEN SUM(matches_played) >= 20 AND (SUM(matches_won) * 1.0 / SUM(matches_played)) >= 0.58 THEN 'S-Tier'
        WHEN SUM(matches_played) >= 15 AND (SUM(matches_won) * 1.0 / SUM(matches_played)) >= 0.52 THEN 'A-Tier'
        WHEN SUM(matches_played) >= 10 THEN 'B-Tier'
        ELSE 'Situational'
    END AS squad_tier
FROM valorant.gold.gold_agent_performance
GROUP BY agent_name, agent_role
ORDER BY total_picks DESC
```

```sql top_winrate_agents
SELECT 
    agent_name,
    agent_role,
    SUM(matches_played) AS total_picks,
    SUM(matches_won) * 1.0 / SUM(matches_played) AS win_rate_ratio,
    ROUND(SUM(total_kills) * 1.0 / NULLIF(SUM(total_deaths), 0), 2) AS combined_kd,
    ROUND(AVG(avg_acs), 1) AS team_avg_acs
FROM valorant.gold.gold_agent_performance
GROUP BY agent_name, agent_role
HAVING SUM(matches_played) >= 10
ORDER BY win_rate_ratio DESC
```

```sql squad_specialists
SELECT 
    o.current_display_name AS player_name,
    a.agent_name,
    a.agent_role,
    a.matches_played,
    a.matches_won,
    a.agent_win_pct / 100.0 AS win_rate_ratio,
    a.kd_ratio,
    ROUND(a.avg_acs, 1) AS avg_acs,
    a.mastery_tier
FROM valorant.gold.gold_agent_performance a
JOIN valorant.gold.gold_player_overall_summary o ON a.player_puuid = o.player_puuid
WHERE a.mastery_tier IN ('Signature', 'Comfort Pick')
ORDER BY a.matches_played DESC
```

---

## 🎯 Inspect Agent Tactical Dossier

Select any agent from the squad's pool to inspect high-resolution Riot artwork, team win rate, combined K/D, and our top specialist:

{% dropdown 
    id="agent_filter" 
    data="agent_dropdown_list" 
    value_column="agent_name" 
    label_column="agent_label"
    select_first=true 
    title="Choose Agent" 
/%}

{% row %}
  {% image 
    data="single_agent_dossier" 
    column="agent_icon_url" 
    filters=["agent_filter"]
    border=true
    max_width=220
    description="Official Riot Agent Artwork" 
  /%}

  {% table data="single_agent_dossier" filters=["agent_filter"] %}
    {% dimension value="agent_name" title="Agent" /%}
    {% dimension value="agent_role" title="Tactical Role" /%}
    {% dimension value="top_specialist" title="🏆 Top Squad Specialist" /%}
    {% measure value="sum(total_picks)" title="Total Picks" fmt="num0" /%}
    {% measure value="sum(total_wins)" title="Wins" fmt="num0" /%}
    {% measure value="avg(win_rate_ratio)" title="Squad Win %" fmt="pct1" /%}
    {% measure value="avg(combined_kd)" title="Team K/D" fmt="num2" /%}
    {% measure value="avg(avg_acs)" title="Team Avg ACS" fmt="num1" /%}
  {% /table %}
{% /row %}

{% row %}
  {% big_value
    data="single_agent_dossier"
    value="sum(total_picks)"
    title="Squad Deployments"
    fmt="num0"
    filters=["agent_filter"]
  /%}

  {% big_value
    data="single_agent_dossier"
    value="avg(win_rate_ratio)"
    title="Agent Win Rate"
    fmt="pct1"
    filters=["agent_filter"]
  /%}

  {% big_value
    data="single_agent_dossier"
    value="avg(combined_kd)"
    title="Combined K/D Ratio"
    fmt="num2"
    filters=["agent_filter"]
  /%}

  {% big_value
    data="single_agent_dossier"
    value="avg(avg_acs)"
    title="Team Avg Combat Score"
    fmt="num1"
    filters=["agent_filter"]
  /%}

  {% big_value
    data="single_agent_dossier"
    value="avg(hs_ratio)"
    title="Headshot Accuracy"
    fmt="pct1"
    filters=["agent_filter"]
  /%}
{% /row %}

### 👥 Teammates Playing this Agent

All squad members who have deployed this agent in competitive matches with individual performance:

{% table data="agent_players_breakdown" filters=["agent_filter"] repeat_values=true %}
  {% dimension value="player_name" title="Teammate" /%}
  {% dimension value="mastery_tier" title="Mastery Tier" /%}
  {% measure value="sum(matches_played)" title="Matches" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="sum(matches_lost)" title="Losses" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(kd_ratio)" title="K/D" fmt="num2" /%}
  {% measure value="avg(avg_acs)" title="ACS" fmt="num1" /%}
  {% measure value="avg(avg_adr)" title="ADR" fmt="num1" /%}
  {% measure value="avg(hs_ratio)" title="HS %" fmt="pct1" /%}
{% /table %}

---

## 🏆 Meta Scorecard (All Matches)

{% row %}
  {% big_value
    data="meta_kpis"
    value="sum(unique_agents_played)"
    title="Agents Fielded"
    fmt="num0"
  /%}

  {% big_value
    data="meta_kpis"
    value="sum(total_agent_deployments)"
    title="Total Agent Picks"
    fmt="num0"
  /%}

  {% big_value
    data="meta_kpis"
    value="avg(overall_agent_win_pct)"
    title="Team Win % Across Picks"
    fmt="num1"
  /%}

  {% big_value
    data="meta_kpis"
    value="avg(overall_team_kd)"
    title="Team Kill/Death Ratio"
    fmt="num2"
  /%}
{% /row %}

---

## ⚖️ Tactical Role Distribution & Efficiency

How our squad allocates team slots between **Sentinel**, **Duelist**, **Controller**, and **Initiator**:

{% row %}
  {% bar_chart
    data="role_summary"
    x="agent_role"
    y="total_picks"
    title="Deployments by Tactical Role"
    order="total_picks desc"
  /%}

  {% bar_chart
    data="role_summary"
    x="agent_role"
    y="win_rate_ratio"
    title="Win Rate % by Role"
    y_fmt="pct1"
    order="win_rate_ratio desc"
  /%}
{% /row %}

{% table data="role_summary" %}
  {% dimension value="agent_role" title="Tactical Role" /%}
  {% measure value="sum(unique_agents)" title="Agents" fmt="num0" /%}
  {% measure value="sum(total_picks)" title="Total Picks" fmt="num0" /%}
  {% measure value="sum(total_wins)" title="Wins" fmt="num0" /%}
  {% measure value="sum(total_losses)" title="Losses" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(role_kd)" title="Role K/D" fmt="num2" /%}
  {% measure value="avg(avg_acs)" title="Avg ACS" fmt="num1" /%}
  {% measure value="avg(hs_ratio)" title="Headshot %" fmt="pct1" /%}
{% /table %}

### 👥 Squad Role Ownership — Who Plays What Role?

Exact breakdown of which teammates account for the picks in each tactical class:

{% table data="player_role_breakdown" repeat_values=true %}
  {% dimension value="player_name" title="Teammate" /%}
  {% measure value="sum(sentinel_picks)" title="🛡️ Sentinel" fmt="num0" /%}
  {% measure value="sum(duelist_picks)" title="⚔️ Duelist" fmt="num0" /%}
  {% measure value="sum(controller_picks)" title="🌑 Controller" fmt="num0" /%}
  {% measure value="sum(initiator_picks)" title="🏹 Initiator" fmt="num0" /%}
  {% measure value="sum(total_picks)" title="Total Matches" fmt="num0" /%}
{% /table %}

### 🏆 Squad Role Win Rate % Matrix

Win conversion percentage when each teammate locks in a specific tactical class:

{% table data="player_role_winrate_matrix" repeat_values=true %}
  {% dimension value="player_name" title="Teammate" /%}
  {% measure value="avg(sentinel_win_ratio)" title="🛡️ Sentinel Win %" fmt="pct1" /%}
  {% measure value="avg(duelist_win_ratio)" title="⚔️ Duelist Win %" fmt="pct1" /%}
  {% measure value="avg(controller_win_ratio)" title="🌑 Controller Win %" fmt="pct1" /%}
  {% measure value="avg(initiator_win_ratio)" title="🏹 Initiator Win %" fmt="pct1" /%}
  {% measure value="avg(overall_win_ratio)" title="Overall Win %" fmt="pct1" /%}
{% /table %}

---

## 📈 Top Win Rate Agents ($\ge 10$ Deployments)

Agents that deliver the highest victory conversion when drafted into our squad's lineup:

{% bar_chart
  data="top_winrate_agents"
  x="agent_name"
  y="win_rate_ratio"
  title="Highest Win Rate Agents in Squad History (Min. 10 Picks)"
  y_fmt="pct1"
  order="win_rate_ratio desc"
/%}

---

## 📋 Complete Squad Agent Tier Rankings

The full team hierarchy ranking all 27 agents by total matches and competitive impact:

{% table data="agent_tier_list" repeat_values=true %}
  {% dimension value="agent_name" title="Agent" /%}
  {% dimension value="agent_role" title="Role" /%}
  {% dimension value="squad_tier" title="Squad Tier" /%}
  {% measure value="sum(total_picks)" title="Picks" fmt="num0" /%}
  {% measure value="sum(total_wins)" title="Wins" fmt="num0" /%}
  {% measure value="sum(total_losses)" title="Losses" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(combined_kd)" title="K/D" fmt="num2" /%}
  {% measure value="avg(team_avg_acs)" title="Team ACS" fmt="num1" /%}
  {% measure value="avg(team_hs_ratio)" title="HS %" fmt="pct1" /%}
{% /table %}

---

## 👑 Squad Signature Specialists & Comfort Picks

Individual teammates who have demonstrated signature mastery on specific agents ($\ge 10$ matches):

{% table data="squad_specialists" repeat_values=true %}
  {% dimension value="player_name" title="Player" /%}
  {% dimension value="agent_name" title="Agent" /%}
  {% dimension value="agent_role" title="Role" /%}
  {% dimension value="mastery_tier" title="Tier" /%}
  {% measure value="sum(matches_played)" title="Played" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(kd_ratio)" title="K/D" fmt="num2" /%}
  {% measure value="avg(avg_acs)" title="ACS" fmt="num1" /%}
{% /table %}
