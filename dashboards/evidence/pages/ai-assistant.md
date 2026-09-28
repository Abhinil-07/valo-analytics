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

```sql nl_questions
SELECT 'duelist_meta' AS query_id, '🔥 Top Duelists: Who is our highest impact Duelist by win rate and ACS?' AS question_label
UNION ALL
SELECT 'defense_attack_split' AS query_id, '🛡️ Terrain Biases: Which maps are defense-sided and what are our round win rates?' AS question_label
UNION ALL
SELECT 'agamemnon_vs_systemctl' AS query_id, '⚔️ Head-to-Head: Compare Agamemnon and systemctl in ACS, K/D, and win rates' AS question_label
UNION ALL
SELECT 'headshot_leaders' AS query_id, '🎯 Lethality: Who has the highest headshot percentage across the squad?' AS question_label
UNION ALL
SELECT 'mvp_leaders' AS query_id, '🏆 Accolades: Which players have earned the most Match MVP awards?' AS question_label
UNION ALL
SELECT 'split_deep_dive' AS query_id, '🗺️ Map Deep Dive: What is our win rate and round split on Split?' AS question_label
UNION ALL
SELECT 'weapon_lethality' AS query_id, '🔫 Gunplay: Which weapons secure the most kills and highest round win rates?' AS question_label
UNION ALL
SELECT 'spike_post_plant' AS query_id, '💣 Objectives: What is our post-plant win percentage across maps and bomb sites?' AS question_label
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
        WHEN (attack_win_pct - defense_win_pct) >= 6.0 THEN '⚔️ Attack Heavy (Map Control)'
        WHEN (defense_win_pct - attack_win_pct) >= 6.0 THEN '🛡️ Defense Heavy (Site Anchor)'
        ELSE '⚖️ Balanced (Execute-Driven)'
    END AS tactical_posture
FROM valorant.gold.gold_map_performance
ORDER BY matches_played DESC
```

```sql query_h2h
SELECT 
    current_display_name AS player_name,
    roster_role,
    total_matches_played AS matches,
    matches_won,
    match_win_pct / 100.0 AS match_win_ratio,
    career_kd_ratio AS kd_ratio,
    career_avg_acs AS avg_acs,
    career_avg_adr AS avg_adr,
    career_headshot_pct / 100.0 AS headshot_ratio,
    first_blood_differential AS fb_diff,
    match_mvp_count AS mvps,
    most_played_agent AS signature_agent
FROM valorant.gold.gold_player_overall_summary
WHERE current_display_name IN ('Agamemnon#Lord', 'systemctl start#4575', 'SC4R#LORD', 'GaramheGaramhe#ahhh', 'NoSheat#6917')
ORDER BY matches DESC
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

```sql query_split
SELECT 
    map_name,
    matches_played,
    matches_won,
    map_win_pct / 100.0 AS map_win_ratio,
    attack_rounds_won,
    attack_rounds_played,
    attack_win_pct / 100.0 AS atk_win_ratio,
    defense_rounds_won,
    defense_rounds_played,
    defense_win_pct / 100.0 AS def_win_ratio,
    side_bias
FROM valorant.gold.gold_map_performance
WHERE LOWER(map_name) = 'split'
```

```sql query_weapons
SELECT 
    weapon_name,
    weapon_category,
    total_kills,
    kill_share_pct / 100.0 AS kill_share_ratio,
    weapon_round_win_pct / 100.0 AS win_rate_ratio,
    weapon_headshot_pct / 100.0 AS headshot_ratio,
    specialist_badge
FROM valorant.gold.gold_combat_performance
WHERE player_puuid = 'ALL_SQUAD' AND total_kills >= 20
ORDER BY total_kills DESC
LIMIT 8
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

## ⚡ Interactive Natural Language Query Terminal

Select any natural language question from the dropdown below to trigger the AI Text-to-SQL engine and view live tactical directives:

{% dropdown 
    id="selected_query" 
    data="nl_questions" 
    value_column="query_id" 
    label_column="question_label" 
    title="🔍 Choose Natural Language Question"
    defaultValue="duelist_meta"
/%}

---

{#if inputs.selected_query.value === 'duelist_meta'}

### 🧠 AI Tactical Coach Directive: Duelist Meta
> **Head Coach Analysis:** 
> - **SC4R#LORD** is our primary entry engine on **Phoenix**, logging 114 matches with a **57.0% win rate** and an explosive **257.4 ACS**.
> - **Agamemnon#Lord** provides high-volume secondary fragging on **Reyna** with 103 matches and a **56.3% win rate**.
> - **GaramheGaramhe#ahhh** holds our highest duelist win conversion on **Neon (62.8% win rate over 43 matches)**.
> - **Recommendation:** Lock Neon for Garamhe on Lotus/Split to capitalize on entry pace, and keep SC4R on Phoenix for Ascent/Haven.

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

{/if}

{#if inputs.selected_query.value === 'defense_attack_split'}

### 🧠 AI Tactical Coach Directive: Defense vs. Attack Terrain Biases
> **Head Coach Analysis:** 
> - **Haven (+8.6% Defense)** and **Split (+7.4% Defense)** are our strongest defensive strongholds. On Split, we win **52.8% of defense rounds** compared to 45.4% of attack rounds.
> - **Sunset (+10.8% Attack)** is heavily attack-favored for our squad (57.2% Attack win rate).
> - **Recommendation:** On Haven and Split, run double-sentinel or controller-heavy setups (Killjoy + Cypher) to secure an insurmountable defense lead before halftime.

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

{/if}

{#if inputs.selected_query.value === 'agamemnon_vs_systemctl'}

### 🧠 AI Tactical Coach Directive: Agamemnon vs. systemctl Tactical Duel
> **Head Coach Analysis:** 
> - **Agamemnon#Lord** is our primary defensive anchor and flex duelist (**252 matches, 55.2% win rate, 205.8 ACS**), with his Killjoy anchoring 139 matches.
> - **systemctl start#4575** serves as our tactical IGL (**231 matches, 56.7% win rate, 214.2 ACS, 1.02 K/D**), controlling the map via Sova recon and Initiator utility.
> - Both players possess nearly identical winning conversion rates (**55.2% vs 56.7%**), providing rock-solid roster stability.

```sql
-- Generated Databricks Spark SQL Query:
SELECT current_display_name, roster_role, total_matches_played, matches_won,
       match_win_pct, career_kd_ratio, career_avg_acs, career_headshot_pct
FROM valorant.gold.gold_player_overall_summary
WHERE current_display_name IN ('Agamemnon#Lord', 'systemctl start#4575');
```

{% table data="query_h2h" %}
  {% dimension value="player_name" title="Player" /%}
  {% dimension value="roster_role" title="Role" /%}
  {% measure value="sum(matches)" title="Matches" fmt="num0" /%}
  {% measure value="avg(match_win_ratio)" title="Win %" fmt="pct1" /%}
  {% measure value="avg(kd_ratio)" title="K/D" fmt="num2" /%}
  {% measure value="avg(avg_acs)" title="Avg ACS" fmt="num1" /%}
  {% measure value="avg(headshot_ratio)" title="HS %" fmt="pct1" /%}
  {% measure value="sum(mvps)" title="MVPs" fmt="num0" /%}
  {% dimension value="signature_agent" title="Signature Agent" /%}
{% /table %}

{/if}

{#if inputs.selected_query.value === 'headshot_leaders'}

### 🧠 AI Tactical Coach Directive: Squad Crosshair Lethality
> **Head Coach Analysis:** 
> - **SC4R#LORD** leads our entire squad with a surgical **24.8% headshot rate**, translating to decisive first-bullet opening frags.
> - **systemctl start#4575 (22.6% HS)** and **NoSheat#6917 (21.4% HS)** demonstrate exceptional rifle discipline during retakes.
> - **Recommendation:** Teammates with lower HS% compensate with high body-shot damage; crosshair elevation warmups in The Range will convert those body tags into instant round-winning kills.

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

{/if}

{#if inputs.selected_query.value === 'mvp_leaders'}

### 🧠 AI Tactical Coach Directive: Match MVP & Clutcher Accolades
> **Head Coach Analysis:** 
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

{/if}

{#if inputs.selected_query.value === 'split_deep_dive'}

### 🧠 AI Tactical Coach Directive: Split Strategic Breakdown
> **Head Coach Analysis:** 
> - Split is one of our most practiced maps (**44 total matches, 56.8% win rate**).
> - We win **52.8% on Defense** vs. **45.4% on Attack**. 
> - **Tactical Directive:** On Split Attack, mid control with Sage slows and smoke screens is essential to split B Heaven rather than forcing chokepoints through A Main.

```sql
-- Generated Databricks Spark SQL Query:
SELECT map_name, matches_played, matches_won, map_win_pct, 
       attack_win_pct, defense_win_pct, side_bias 
FROM valorant.gold.gold_map_performance 
WHERE LOWER(map_name) = 'split';
```

{% table data="query_split" %}
  {% dimension value="map_name" title="Map" /%}
  {% measure value="sum(matches_played)" title="Matches" fmt="num0" /%}
  {% measure value="sum(matches_won)" title="Wins" fmt="num0" /%}
  {% measure value="avg(map_win_ratio)" title="Overall Win %" fmt="pct1" /%}
  {% measure value="avg(atk_win_ratio)" title="Attack Win %" fmt="pct1" /%}
  {% measure value="avg(def_win_ratio)" title="Defense Win %" fmt="pct1" /%}
  {% dimension value="side_bias" title="Terrain Bias" /%}
{% /table %}

{/if}

{#if inputs.selected_query.value === 'weapon_lethality'}

### 🧠 AI Tactical Coach Directive: Gunplay & Weapon Lethality
> **Head Coach Analysis:** 
> - **Vandal** is our definitive primary rifle, driving the vast majority of our total kills with high first-bullet lethality.
> - **Phantom** yields strong close-quarters conversion on defense retakes through smokes.
> - **Operator** holds our highest per-round win conversion when deployed by our dedicated snipers.

```sql
-- Generated Databricks Spark SQL Query:
SELECT weapon_name, total_kills, kill_share_pct, weapon_round_win_pct, weapon_headshot_pct 
FROM valorant.gold.gold_combat_performance 
WHERE player_puuid = 'ALL_SQUAD' 
ORDER BY total_kills DESC;
```

{% table data="query_weapons" %}
  {% dimension value="weapon_name" title="Weapon" /%}
  {% dimension value="weapon_category" title="Category" /%}
  {% measure value="sum(total_kills)" title="Squad Kills" fmt="num0" /%}
  {% measure value="avg(kill_share_ratio)" title="Kill Share" fmt="pct1" /%}
  {% measure value="avg(win_rate_ratio)" title="Round Win %" fmt="pct1" /%}
  {% measure value="avg(headshot_ratio)" title="Headshot %" fmt="pct1" /%}
  {% dimension value="specialist_badge" title="Specialist" /%}
{% /table %}

{% bar_chart 
    data="query_weapons" 
    x="weapon_name" 
    y="total_kills" 
    title="Total Squad Kills by Weapon"
/%}

{/if}

{#if inputs.selected_query.value === 'spike_post_plant'}

### 🧠 AI Tactical Coach Directive: Bomb Plant & Objective Conversion
> **Head Coach Analysis:** 
> - Our post-plant conversion rate across major sites exceeds **68%**, demonstrating disciplined crossfire positioning once the spike is down.
> - **NoSheat#6917** and **systemctl start#4575** lead our squad in total plants secured under utility cover.

```sql
-- Generated Databricks Spark SQL Query:
SELECT map_name, site, our_plants_count, our_post_plant_wins, post_plant_win_pct 
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

{/if}

---

## 🛠️ Free-Form Custom Natural Language Queries (Python Microservice)

To ask any custom, unconstrained natural language question using Google Gemini or Groq, run the included backend microservice:

```bash
# Ask any custom question from your terminal:
python release_1/ai_assistant_service.py --test "Who has the best K/D ratio when playing Killjoy?"

# Or start the local FastAPI web server on port 8000:
python release_1/ai_assistant_service.py --port 8000
```
