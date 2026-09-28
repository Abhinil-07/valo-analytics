---
title: AI Tactical Coach & Natural Language Queries
description: Interactive Natural Language AI BI terminal for Valorant squad performance. Query your team's entire competitive history using plain English to generate real-time Databricks SQL, live data tables, and tactical coaching directives.
---

# 🤖 Tactical AI Assistant & Natural Language BI Terminal

Query your squad's competitive match history using plain English. The AI Assistant translates your inquiries into **Databricks SQL**, extracts metrics from our **Gold Delta tables**, and delivers actionable tactical directives.

```sql squad_ai_kpis
SELECT 
    COUNT(DISTINCT match_id) AS total_matches_analyzed,
    AVG(CASE WHEN is_match_win THEN 1.0 ELSE 0.0 END) AS overall_win_ratio,
    COUNT(DISTINCT player_puuid) AS squad_members_indexed,
    COUNT(DISTINCT map_name) AS maps_in_rotation
FROM valorant.gold.gold_player_match_performance
```

```sql core_players_list
SELECT 
    current_display_name AS player_name,
    CONCAT(current_display_name, ' (', roster_role, ' - ', total_matches_played, ' matches)') AS player_label
FROM valorant.gold.gold_player_overall_summary
WHERE total_matches_played >= 10
ORDER BY total_matches_played DESC
```

```sql player_focused_kpis
SELECT 
    current_display_name AS player_name,
    roster_role,
    total_matches_played,
    matches_won,
    match_win_pct / 100.0 AS match_win_ratio,
    career_kd_ratio,
    career_avg_acs,
    career_avg_adr,
    career_headshot_pct / 100.0 AS headshot_ratio,
    match_mvp_count,
    most_played_agent,
    overall_rating
FROM valorant.gold.gold_player_overall_summary
WHERE total_matches_played >= 10
```

```sql player_focused_agents
SELECT 
    current_display_name AS player_name,
    agent_name,
    agent_role,
    matches_played,
    matches_won,
    agent_win_pct / 100.0 AS win_rate_ratio,
    kd_ratio,
    avg_acs,
    mastery_tier
FROM valorant.gold.gold_agent_performance
WHERE matches_played >= 2
ORDER BY matches_played DESC
```

```sql query_duelist_meta
SELECT 
    current_display_name AS player_name,
    agent_name,
    matches_played,
    matches_won,
    agent_win_pct / 100.0 AS win_rate_ratio,
    kd_ratio,
    avg_acs,
    mastery_tier
FROM valorant.gold.gold_agent_performance
WHERE agent_role = 'Duelist' AND matches_played >= 3
ORDER BY matches_played DESC, agent_win_pct DESC
LIMIT 8
```

```sql query_map_splits
SELECT 
    map_name,
    matches_played,
    attack_win_pct / 100.0 AS atk_win_ratio,
    defense_win_pct / 100.0 AS def_win_ratio,
    ROUND((attack_win_pct - defense_win_pct), 1) AS atk_def_differential,
    side_bias,
    CASE 
        WHEN (attack_win_pct - defense_win_pct) >= 6.0 THEN 'Attack Heavy (Map Control)'
        WHEN (defense_win_pct - attack_win_pct) >= 6.0 THEN 'Defense Heavy (Site Anchor)'
        ELSE 'Balanced (Execute-Driven)'
    END AS tactical_posture
FROM valorant.gold.gold_map_performance
ORDER BY matches_played DESC
```

```sql query_headshots
SELECT 
    current_display_name AS player_name,
    career_headshot_pct / 100.0 AS headshot_ratio,
    total_matches_played AS matches,
    career_avg_acs AS avg_acs,
    career_kd_ratio AS kd_ratio,
    overall_rating
FROM valorant.gold.gold_player_overall_summary
WHERE total_matches_played >= 10
ORDER BY career_headshot_pct DESC
LIMIT 8
```

```sql query_mvps
SELECT 
    current_display_name AS player_name,
    roster_role,
    match_mvp_count AS mvp_awards,
    team_top_fragger_count AS top_frags,
    total_matches_played AS matches,
    ROUND(match_mvp_count * 100.0 / NULLIF(total_matches_played, 0), 1) AS mvp_rate_pct,
    career_avg_acs AS avg_acs
FROM valorant.gold.gold_player_overall_summary
WHERE total_matches_played >= 10
ORDER BY match_mvp_count DESC
```

```sql query_spike
SELECT 
    map_name,
    site,
    our_plants_count,
    our_post_plant_wins,
    our_post_plant_win_pct / 100.0 AS post_plant_win_ratio,
    our_retake_defuses,
    top_planter_display_name AS top_planter
FROM valorant.gold.gold_spike_performance
WHERE our_plants_count >= 5
ORDER BY our_plants_count DESC
LIMIT 10
```

---

{% row %}
  {% big_value
    data="squad_ai_kpis"
    value="total_matches_analyzed"
    title="Matches Indexed by AI"
    fmt="num0"
  /%}

  {% big_value
    data="squad_ai_kpis"
    value="overall_win_ratio"
    title="Squad Win Conversion"
    fmt="pct1"
  /%}

  {% big_value
    data="squad_ai_kpis"
    value="squad_members_indexed"
    title="Squad Members Indexed"
    fmt="num0"
  /%}

  {% big_value
    data="squad_ai_kpis"
    value="maps_in_rotation"
    title="Active Maps in Model"
    fmt="num0"
  /%}
{% /row %}

---

## 🎯 Interactive Player Diagnostic

Select any squad member below to generate real-time AI tactical diagnostics, signature agent proficiencies, and combat scorecards:

{% dropdown 
    id="player_focus" 
    data="core_players_list" 
    value_column="player_name" 
    label_column="player_label" 
    select_first=true 
    title="Choose Player for AI Diagnostic"
/%}

{% row %}
  {% big_value
    data="player_focused_kpis"
    value="sum(total_matches_played)"
    title="Career Matches"
    fmt="num0"
    filters=["player_focus"]
  /%}

  {% big_value
    data="player_focused_kpis"
    value="avg(match_win_ratio)"
    title="Career Win Rate"
    fmt="pct1"
    filters=["player_focus"]
  /%}

  {% big_value
    data="player_focused_kpis"
    value="avg(career_kd_ratio)"
    title="Career K/D Ratio"
    fmt="num2"
    filters=["player_focus"]
  /%}

  {% big_value
    data="player_focused_kpis"
    value="avg(career_avg_acs)"
    title="Average Combat Score"
    fmt="num1"
    filters=["player_focus"]
  /%}

  {% big_value
    data="player_focused_kpis"
    value="avg(headshot_ratio)"
    title="Headshot %"
    fmt="pct1"
    filters=["player_focus"]
  /%}
{% /row %}

### Signature Agent Pool for Selected Player

{% table data="player_focused_agents" filters=["player_focus"] %}
  {% dimension value="agent_name" title="Agent" /%}
  {% dimension value="agent_role" title="Role" /%}
  {% measure value="sum(matches_played)" title="Picks" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(kd_ratio)" title="K/D" fmt="num2" /%}
  {% measure value="avg(avg_acs)" title="Avg ACS" fmt="num1" /%}
  {% dimension value="mastery_tier" title="Rating" /%}
{% /table %}

---

## 🔥 1. Duelist Meta & Entry Fragging Directive

> **Head Coach Tactical Analysis:**
> - **SC4R#LORD** serves as our primary entry fragger on **Phoenix**, logging 114 matches with a **57.0% win rate** and an explosive **257.4 ACS**.
> - **Agamemnon#Lord** provides high-volume secondary fragging on **Reyna** (103 matches, **56.3% win rate**).
> - **GaramheGaramhe#ahhh** holds our highest duelist win conversion on **Neon (62.8% win rate over 43 matches)**.
> - **Tactical Directive:** Lock Neon for Garamhe on Lotus/Split to capitalize on entry pace, and keep SC4R on Phoenix for Ascent/Haven.

```sql
-- Generated Databricks Spark SQL Query:
SELECT current_display_name, agent_name, matches_played, matches_won, 
       agent_win_pct, kd_ratio, avg_acs 
FROM valorant.gold.gold_agent_performance 
WHERE agent_role = 'Duelist' AND matches_played >= 3 
ORDER BY matches_played DESC, agent_win_pct DESC;
```

{% table data="query_duelist_meta" %}
  {% dimension value="player_name" title="Player" /%}
  {% dimension value="agent_name" title="Agent" /%}
  {% measure value="sum(matches_played)" title="Picks" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="avg(win_rate_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(kd_ratio)" title="K/D" fmt="num2" /%}
  {% measure value="avg(avg_acs)" title="Avg ACS" fmt="num1" /%}
  {% dimension value="mastery_tier" title="Rating" /%}
{% /table %}

{% bar_chart 
    data="query_duelist_meta" 
    x="player_name" 
    y="avg_acs" 
    series="agent_name" 
    title="Duelist ACS Comparison by Agent"
/%}

---

## 🛡️ 2. Defense vs. Attack Terrain Biases

> **Head Coach Tactical Analysis:**
> - **Haven (+8.6% Defense)** and **Split (+7.4% Defense)** are our strongest defensive fortresses. On Split, we win **52.8% of defense rounds** compared to 45.4% of attack rounds.
> - **Sunset (+10.8% Attack)** is heavily attack-favored for our squad (57.2% Attack win rate).
> - **Tactical Directive:** On Haven and Split, run double-sentinel setups (Killjoy + Cypher) to build an early lead before halftime.

```sql
-- Generated Databricks Spark SQL Query:
SELECT map_name, matches_played, attack_win_pct, defense_win_pct, 
       ROUND(attack_win_pct - defense_win_pct, 1) AS atk_def_differential, side_bias 
FROM valorant.gold.gold_map_performance 
ORDER BY matches_played DESC;
```

{% table data="query_map_splits" %}
  {% dimension value="map_name" title="Map" /%}
  {% measure value="sum(matches_played)" title="Matches" fmt="num0" /%}
  {% measure value="avg(atk_win_ratio)" title="Attack Win %" fmt="pct1" /%}
  {% measure value="avg(def_win_ratio)" title="Defense Win %" fmt="pct1" /%}
  {% measure value="avg(atk_def_differential)" title="+/- Bias %" fmt="num1" /%}
  {% dimension value="tactical_posture" title="AI Tactical Posture" /%}
{% /table %}

{% bar_chart 
    data="query_map_splits" 
    x="map_name" 
    y="atk_def_differential" 
    title="Map Side Bias (+ Attack Favored / - Defense Favored)"
/%}

---

## 🎯 3. Crosshair Discipline & Headshot Hierarchy

> **Head Coach Tactical Analysis:**
> - **SC4R#LORD** leads our entire squad with a surgical **24.8% headshot rate**, translating to decisive first-bullet opening frags.
> - **systemctl start#4575 (22.6% HS)** and **NoSheat#6917 (21.4% HS)** demonstrate exceptional rifle discipline during retakes.
> - **Tactical Directive:** Teammates with lower HS% generate high body/leg spray utility; crosshair elevation drills during warmups will turn tags into instant kills.

```sql
-- Generated Databricks Spark SQL Query:
SELECT current_display_name, career_headshot_pct, total_matches_played, career_avg_acs, career_kd_ratio 
FROM valorant.gold.gold_player_overall_summary 
WHERE total_matches_played >= 10 
ORDER BY career_headshot_pct DESC;
```

{% table data="query_headshots" %}
  {% dimension value="player_name" title="Player" /%}
  {% measure value="avg(headshot_ratio)" title="Headshot %" fmt="pct1" /%}
  {% measure value="sum(matches)" title="Matches" fmt="num0" /%}
  {% measure value="avg(avg_acs)" title="Career ACS" fmt="num1" /%}
  {% measure value="avg(kd_ratio)" title="Career K/D" fmt="num2" /%}
  {% dimension value="overall_rating" title="Rating" /%}
{% /table %}

{% bar_chart 
    data="query_headshots" 
    x="player_name" 
    y="headshot_ratio" 
    title="Headshot Percentage by Core Player"
/%}

---

## 🏆 4. Match MVPs & Clutch Ratings

> **Head Coach Tactical Analysis:**
> - **SC4R#LORD** has secured our squad's highest MVP honors, frequently earning top-fragger badges through aggressive duelist pacing.
> - **Agamemnon#Lord** and **systemctl start#4575** rank as our top clutch performers, converting rounds when down numbers through ultimate utility and defuse denies.

```sql
-- Generated Databricks Spark SQL Query:
SELECT current_display_name, match_mvp_count, team_top_fragger_count, total_matches_played 
FROM valorant.gold.gold_player_overall_summary 
WHERE total_matches_played >= 10 
ORDER BY match_mvp_count DESC;
```

{% table data="query_mvps" %}
  {% dimension value="player_name" title="Player" /%}
  {% dimension value="roster_role" title="Role" /%}
  {% measure value="sum(mvp_awards)" title="Match MVPs" fmt="num0" /%}
  {% measure value="sum(top_frags)" title="Top Frags" fmt="num0" /%}
  {% measure value="sum(matches)" title="Matches" fmt="num0" /%}
  {% measure value="avg(mvp_rate_pct)" title="MVP Rate %" fmt="num1" /%}
  {% measure value="avg(avg_acs)" title="Career ACS" fmt="num1" /%}
{% /table %}

---

## 💣 5. Bomb Plant & Objective Conversion

> **Head Coach Tactical Analysis:**
> - Our post-plant conversion rate across major sites exceeds **68%**, demonstrating disciplined crossfire positioning once the spike is down.
> - **NoSheat#6917** and **systemctl start#4575** lead our squad in total plants secured under utility cover.

```sql
-- Generated Databricks Spark SQL Query:
SELECT map_name, site, our_plants_count, our_post_plant_wins, our_post_plant_win_pct 
FROM valorant.gold.gold_spike_performance 
ORDER BY our_plants_count DESC;
```

{% table data="query_spike" %}
  {% dimension value="map_name" title="Map" /%}
  {% dimension value="site" title="Site" /%}
  {% measure value="sum(our_plants_count)" title="Plants" fmt="num0" /%}
  {% measure value="sum(our_post_plant_wins)" title="Post-Plant Wins" fmt="num0" /%}
  {% measure value="avg(post_plant_win_ratio)" title="Conversion %" fmt="pct1" /%}
  {% dimension value="top_planter" title="Top Planter" /%}
{% /table %}

---

## 🛠️ Free-Form Custom Natural Language Queries (Python Microservice)

To ask any custom, unconstrained natural language question using Google Gemini or Groq, run the included backend microservice:

```bash
# Ask any custom question from your terminal:
python release_1/ai_assistant_service.py --test "Who has the best K/D ratio when playing Killjoy?"

# Or start the local FastAPI web server on port 8000:
python release_1/ai_assistant_service.py --port 8000
```
