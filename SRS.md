from pathlib import Path

content = r"""# Valorant Team Performance Analytics Platform

## End-to-End Business Requirements & Analytics Specification

## 1. Business Objective

Build a centralized analytics platform for a Valorant team that consolidates historical and ongoing match data into a reliable analytical dataset.

The platform should allow the team to:

- Understand overall team performance.
- Analyze individual player performance.
- Identify strengths and weaknesses.
- Understand performance by map and agent.
- Analyze round-by-round performance.
- Analyze kills and damage at event level.
- Understand attack/defense performance.
- Identify trends over time.
- Compare players, matches, maps, agents, and time periods.
- Investigate individual matches in detail.
- Make performance data accessible through dashboards and reusable analytical datasets.

The solution should support both:

- Historical analysis
- Future/incremental match ingestion

Historical GitHub data and current API data should ultimately support the same analytical model.

---

# 2. Business Questions the Platform Must Answer

## A. Match Performance

The platform should answer:

- How many matches have we played?
- What is our overall win/loss record?
- What is our win rate?
- How did we perform over a selected time period?
- What was the score of each match?
- Which maps did we play?
- How many rounds were played?
- How long were the matches?
- How has our performance changed over time?
- Which matches were particularly close?
- What was our round differential over time?
- What maps have we played most frequently?

### Match-level metrics

At minimum:

- Matches played
- Wins
- Losses
- Win %
- Rounds won
- Rounds lost
- Round win %
- Average rounds per match
- Average match duration
- Map-wise match count
- Map-wise win %
- Time-period win/loss trend

---

# 3. Team Performance Analysis

The primary purpose is to understand why the team wins or loses, rather than only reporting the final result.

Analyze:

- Overall team performance.
- Performance by map.
- Performance by opponent.
- Performance over time.
- Performance by attack/defense.
- Round conversion.
- Opening-kill impact.
- Damage output.
- Kill/death differential.
- Post-plant situations where the available data supports it.

### Team metrics

Calculate where supported:

- Team kills
- Team deaths
- Team assists
- Team damage
- Team average damage per round
- Team kills per round
- Team deaths per round
- Team K/D
- Team damage differential
- Team kill differential
- Team round win %
- Attack round win %
- Defense round win %
- First-kill rate
- First-death rate
- Opening-kill conversion
- Opening-death conversion

---

# 4. Player Performance Analysis

The platform should allow analysis of every player across matches and time.

For each player:

- How many matches did they play?
- How many rounds did they play?
- How many kills?
- How many deaths?
- How much damage?
- How many assists?
- What is their K/D?
- What is their average damage?
- What is their headshot percentage?
- How often did they get the first kill?
- How often were they the first death?
- How consistent is their performance?
- How does their performance change by map?
- How does their performance change by agent?
- How does their performance change over time?

### Player metrics

Where supported:

- Matches played
- Rounds played
- Wins participated in
- Kills
- Deaths
- Assists
- K/D
- Kill %
- Death %
- Damage
- Average damage per round
- Damage per match
- ACS, if source data supports the calculation
- Headshots
- Bodyshots
- Legshots
- Headshot %
- First kills
- First deaths
- First-kill rate
- First-death rate
- Kills per round
- Deaths per round
- Assists per round
- Round participation
- Round survival rate
- Performance by map
- Performance by agent

Do not create metrics where the source data cannot support them.

---

# 5. Player Consistency Analysis

Performance should not only be measured using averages.

For each player, analyze consistency across matches and rounds.

Examples:

- Average damage
- Median damage
- Minimum/maximum damage
- Standard deviation of damage
- Average kills
- Median kills
- Kill variability
- Average ACS where supported
- Performance trend over time

The objective is to distinguish:

> high average performance

from:

> consistently high performance.

---

# 6. Map Analysis

Analyze team and player performance by map.

### Business questions

- Which maps have we played?
- How frequently?
- What is our win rate on each map?
- How many rounds do we win on each map?
- How do players perform on each map?
- Which agents are used on each map?
- How does attack/defense performance vary by map?

### Map metrics

- Matches played
- Wins
- Losses
- Win %
- Rounds won
- Rounds lost
- Round win %
- Average team kills
- Average team deaths
- Average team damage
- Average player damage
- Attack win %
- Defense win %
- Opening-kill rate
- Opening-death rate

---

# 7. Agent Analysis

Analyze agent selection and performance.

### Business questions

- Which agents are being played?
- How frequently?
- What is our win rate when using an agent?
- How does a player's performance vary by agent?
- How does agent usage vary by map?
- Which player-agent combinations are most common?
- How does team performance vary with different agent compositions?

### Agent metrics

- Agent pick count
- Agent pick %
- Matches played
- Wins
- Losses
- Win %
- Player kills
- Player deaths
- Player damage
- K/D
- Average damage
- First kills
- First deaths
- Map-specific agent performance
- Player-agent performance

Avoid interpreting an agent as "better" purely from a small sample size. Expose sample size alongside performance.

---

# 8. Player + Agent + Map Analysis

Analyze the granular dimension:

Player × Agent × Map

### Questions

- How does Player A perform with Agent X?
- How does Player A perform with Agent X on Map Y?
- Which maps does a player perform better/worse on with a particular agent?
- How frequently has a player-agent-map combination been used?

### Metrics

- Matches
- Rounds
- Wins
- Kills
- Deaths
- Assists
- Damage
- K/D
- Damage/round
- First kills
- First deaths
- Win %

Always expose sample size.

---

# 9. Round-Level Analysis

For every round, determine:

- Which team won?
- How did the round end?
- Was the spike planted?
- Was the spike defused?
- Which players participated?
- Which players survived?
- Who got kills?
- Who died?
- How much damage was dealt?
- Who got the opening kill?
- Who suffered the opening death?

### Round metrics

- Round winner
- Our-team round result
- Attack/defense
- Plant indicator
- Defuse indicator
- Kill count
- Damage
- Player survival
- Opening kill
- Opening death

---

# 10. Attack vs Defense Analysis

Attack and defense sides are systematically derived in the Silver layer (`fact_round` and `fact_round_player`) for competitive matches:
- **First Half (Rounds 1–12):** Identified by the team planting the spike (`plant_events.planted_by.team`) or standard competitive starting side.
- **Second Half (Rounds 13–24):** Inverted sides following the halftime swap.
- **Overtime (Rounds 25+):** Alternating round sides.
- Mapped against `OUR_TEAM` to explicitly determine `our_team_side` (`'Attack'` vs `'Defense'`).

### Team

- Attack rounds
- Defense rounds
- Attack wins
- Defense wins
- Attack win %
- Defense win %

### Player

- Attack kills
- Defense kills
- Attack deaths
- Defense deaths
- Attack damage
- Defense damage
- Attack K/D
- Defense K/D
- First kills by side
- First deaths by side

### Map

Analyze attack/defense performance separately by map.

This reveals whether strengths/weaknesses are concentrated on a particular side or map.

---

# 11. Kill Event Analysis

Use individual kill events rather than only aggregate statistics.

### Questions

- Who killed whom?
- In which round?
- With which weapon?
- Which player gets the most opening kills?
- Who is frequently the first death?
- Which weapons generate kills?
- Where do kills occur?
- How do opening kills affect round outcomes?

### Kill analytics

- Total kills
- Kills by player
- Kills by victim
- Kills by weapon
- Kills by map
- Kills by round
- Kills by attack/defense
- Opening kills
- Opening deaths
- Kill differential
- Player-vs-player kill relationships
- Kill locations where supported

---

# 12. Opening Kill Analysis

Opening kills should be derived from the ordered kill events.

For every round:

1. Identify the first valid kill event chronologically recorded within the round.
2. Identify the killer.
3. Identify the victim.
4. Determine their team roles.
5. Determine whether our team obtained the opening kill.
6. Determine whether our team suffered the opening death.
7. Compare the opening result with the final round result.

### Metrics

- Opening kills
- Opening deaths
- Opening-kill rate
- Opening-death rate
- Rounds won after obtaining opening kill
- Rounds lost after obtaining opening kill
- Rounds won after suffering opening death
- Rounds lost after suffering opening death

The purpose is to understand the relationship between opening events and round outcomes.

---

# 13. Damage Analysis

The individual `damage_events[]` data should be retained and analyzed.

### Questions

- How much damage does each player deal?
- Who receives the most damage?
- Who deals damage to whom?
- How much damage is dealt before a kill?
- Which players consistently deal high damage?
- How does damage translate into round wins?
- How does damage vary by map/agent/side?

### Damage metrics

- Total damage
- Damage per round
- Damage per match
- Damage dealt
- Damage received
- Damage differential
- Average damage
- Median damage
- Headshot damage/events where supported
- Bodyshot damage/events
- Legshot damage/events
- Player-vs-player damage
- Team damage
- Damage by map
- Damage by agent
- Damage by attack/defense

---

# 14. Damage vs Kill Analysis

Use damage events and kill events together.

Potential analysis:

- Damage dealt before a kill.
- Damage dealt to eventual victims.
- Damage efficiency.
- Damage without kills.
- Kills with relatively low damage.
- Players generating damage but not converting it into kills.

Do not invent a proprietary "efficiency score" unless explicitly defined later.

Keep the underlying metrics transparent.

---

# 15. Spike Plant Analysis

Where plant events are available, analyze:

- Number of plants
- Plants by player
- Plants by map
- Plants by round
- Plants by attack rounds
- Plant frequency
- Round result after plant
- Plant-to-win relationship

### Potential metrics

- Plant count
- Plant rate on attacking rounds
- Rounds won after planting
- Rounds lost after planting
- Plant conversion rate

The source data should determine exactly which timing/site/player fields are available.

---

# 16. Spike Defuse Analysis

Where defuse events are available, analyze:

- Number of defuses
- Defuses by player
- Defuses by map
- Defuses by round
- Defuses by defense rounds
- Round result after defuse

### Potential metrics

- Defuse count
- Defuse rate where meaningful
- Rounds won with defuse
- Defuse timing where available

Only calculate metrics supported by actual source semantics.

---

# 17. Post-Plant Analysis

Where plant + kill + defuse + round data can reliably be connected, analyze:

- Plant occurred
- Defuse occurred
- Attacking team won/lost
- Defending team won/lost
- Kills after plant
- Damage after plant
- Time between plant and subsequent events where timestamps support it

### Business questions

- How often do we convert planted rounds?
- How often do we lose after planting?
- How often do we successfully defend after an opponent plant?
- Which players contribute most in post-plant situations?

---

# 18. Weapon & Loadout Analysis

Using equipped weapon data from round player stats and weapon details from kill events:

- Equipped weapon by player per round (available for all rounds, even with 0 kills)
- Purchase frequency by weapon
- Kills by weapon
- Weapon kills by player
- Weapon kills by map
- Weapon kills by attack/defense
- Weapon kill share
- Headshot % by weapon
- Armor tier purchased (Light / Heavy / None)

---

# 19. Historical Trend Analysis

Allow analysis over time.

### Dimensions

- Day
- Week
- Month
- Season
- Map
- Player
- Agent

### Metrics

- Win %
- Round win %
- K/D
- Damage/round
- First-kill rate
- First-death rate
- Player performance
- Map performance
- Agent performance

### Questions

- Is team performance improving?
- Is a player's performance improving?
- Has performance changed on a particular map?
- Has agent usage changed?
- Has the team's attack/defense performance changed?

Trend analysis should show both the metric and underlying sample size.

---

# 20. Match Deep Dive

A selected match should be explorable as:

Match → Teams → Rounds → Players → Kills / Damage / Plants / Defuses

### Match summary

- Final result
- Score
- Map
- Duration
- Total rounds

### Team summary

- Team kills
- Team deaths
- Team damage
- Round wins
- Attack/defense performance

### Player summary

- Kills
- Deaths
- Assists
- Damage
- K/D
- Headshots
- First kills
- First deaths
- Agent

### Round timeline

For every round:

- Winner
- End type
- Plant
- Defuse
- Kill sequence
- Opening kill
- Damage events

---

# 21. Player Comparison

Allow two or more players to be compared across the same period.

### Comparison dimensions

- Matches
- Rounds
- Kills
- Deaths
- Assists
- K/D
- Damage
- Damage/round
- Headshot %
- First kills
- First deaths
- Win %
- Map performance
- Agent performance

The underlying sample size should always be visible.

---

# 22. Team Composition Analysis

Where five-player match compositions are available, analyze:

- Player combinations
- Agent combinations
- Map + composition
- Composition win %
- Composition sample size
- Performance metrics under each composition

Potential business question:

> How has the team performed when using a particular player/agent composition?

Do not make causal claims from observational match data.

---

# 23. Economy & Spend Performance

Using round-level economy and credit data from player round stats:

### Core Questions
- How often do we win pistol rounds (Round 1 & Round 13)?
- How do we perform on Eco vs Semi-Buy vs Full-Buy rounds?
- What is our credit efficiency (damage and kills generated per credit spent)?
- Does our team over-spend or under-spend relative to the opponent?

### Economy Metrics
- **Pistol Round Win %:** Win rate on Round 1 and Round 13.
- **Round Buy Type Classification:**
  - *Pistol:* Rounds 1 and 13.
  - *Eco:* Team loadout value < 1,500 credits per player average.
  - *Semi-Buy / Force-Buy:* Team loadout value 1,500 – 3,900 credits average.
  - *Full Buy:* Team loadout value > 3,900 credits average.
- **Buy-Type Round Win %:** Conversion rates across each buy type.
- **Average Loadout Value:** By team and player per round.
- **Spent Credits vs Remaining Credits:** Tracking economy discipline across rounds.

---

# 24. Clutch Situation Analysis

Using player survival and kill sequence timing:

### Core Questions
- How often does a player or team convert a 1vX situation into a round win?
- Who is our most reliable clutch performer?

### Clutch Metrics
- Clutch opportunities (1v1, 1v2, 1v3, etc.).
- Clutch wins and conversion rate %.
- Clutch conversion by player and agent.

---

# 25. Ability & Utility Usage Analysis

Using individual ability casts (`c_casts`, `q_casts`, `e_casts`, `x_casts`) per round:

### Core Questions
- How frequently are abilities and ultimates used?
- Which agents utilize their utility most actively?
- Does high ability usage correlate with round win rate?

### Utility Metrics
- Total ability casts per match / round.
- Ultimate casts (`x_casts`) per match.
- Ability usage rate by agent and player.

---

# 26. Data Quality / Observability Requirements

The analytics platform itself should be observable.

## Ingestion

Track:

- Source
- Ingestion date
- Match count
- Success/failure
- Duplicate matches
- Missing matches

## Bronze

Track:

- Source-to-Bronze row reconciliation
- Duplicate logical keys
- Missing keys
- Referential integrity
- Event counts

## Silver

Track:

- Invalid player references
- Invalid match references
- Invalid round references
- Missing roster mappings
- Unexpected nulls

## Gold

Track:

- Aggregate reconciliation against Silver
- Metric calculation failures
- Unexpected drops/spikes

---

# 27. Team Identity / OUR_TEAM Logic

The platform must distinguish:

- `OUR_TEAM`
- `OPPONENT`

from the source:

- `Red`
- `Blue`

This must NOT be hardcoded into Bronze.

Silver should use a roster configuration/reference mechanism based primarily on:

`player_puuid`

and, where necessary, validity dates.

This allows historical matches to remain correct even if the team's roster changes.

---

# 28. Stable Player Identity

Player identity must primarily use:

`player_puuid`

Names should be treated as match-time attributes rather than permanent identifiers.

This enables:

- Player history
- Name changes
- Historical consistency
- Cross-match aggregation

---

# 29. Power BI / Reporting Requirements

The final Gold layer should support at least six dashboards.

## Dashboard 1 — Team Overview

- Matches
- Wins/losses
- Win %
- Round win %
- Recent performance
- Map performance
- Attack/defense performance

## Dashboard 2 — Player Performance

- Player KPIs
- Player trends
- K/D
- Damage
- Headshot %
- First kills/deaths
- Map performance
- Agent performance

## Dashboard 3 — Map & Agent Analysis

- Map usage
- Map win %
- Agent usage
- Agent win %
- Player-agent-map combinations

## Dashboard 4 — Match Analysis

- Match summary
- Team comparison
- Player comparison
- Round timeline
- Kill events
- Damage events
- Plant/defuse events

## Dashboard 5 — Economy & Utility Analysis

- Pistol round conversion %
- Eco / Semi-Buy / Full-Buy win rates
- Spent credits vs loadout value
- Ability and ultimate usage per agent

## Dashboard 6 — Historical Trends

- Team trends
- Player trends
- Map trends
- Agent trends
- Period-over-period comparisons

---

# 30. Analytical Layering

The intended flow is:

LANDING
→ Canonical one-match JSON

BRONZE
→ Source-oriented normalized relational data

SILVER
→ Clean analytical entities + reusable derived metrics

GOLD
→ Business-level aggregations

POWER BI
→ Dashboards / reporting

### Bronze answers:

> What did the source tell us?

### Silver answers:

> What does the cleaned data mean analytically?

### Gold answers:

> What business question are we answering?

---

# 31. Important Analytical Principle

Do not create metrics merely because they sound useful.

Every metric should have:

1. A clear business definition.
2. A known source.
3. A reproducible calculation.
4. A defined grain.
5. Appropriate sample-size context.
6. A clear distinction between observed facts and derived metrics.

Example:

**First Kill**

> The first valid kill event chronologically recorded within a round.

**Opening Kill Conversion**

> The proportion of rounds won among rounds in which our team obtained the opening kill.

Definitions should be documented alongside Gold metrics.

---

# 32. Final End-to-End Analytical Scope

The overall analytical model should ultimately support:

MATCH
|
+-- TEAM
|
+-- PLAYER
|
+-- MAP
|
+-- AGENT
|
+-- ROUND
|
+-- KILLS
+-- DAMAGE
+-- SPIKE
|
+-- PLANT
+-- DEFUSE
|
+-- ECONOMY
+-- UTILITY

### Major analytical dimensions

- Team
- Player
- Match
- Map
- Agent
- Round
- Attack / Defense
- Kill
- Damage
- Plant
- Defuse
- Weapon & Economy
- Utility
- Time

### Major analytical themes

- Performance
- Consistency
- Trends
- Comparison
- Events
- Map
- Agent
- Team composition
- Opening engagements
- Damage
- Round outcomes
- Post-plant situations
- Economy & Buy types
- Clutch conversions

---

# 33. Implementation Guidance for Codex

Use this document as the business and analytics contract.

Do NOT assume that every requested metric or field is technically possible.

Before implementing Silver or Gold:

1. Inspect the actual Bronze schemas.
2. Inspect actual data values and distributions.
3. Trace every proposed metric to its source data.
4. Identify metrics that cannot be reliably derived.
5. Identify ambiguous source semantics.
6. Propose the physical Silver/Gold model based on the actual data.
7. Preserve lineage from Gold metrics back to source/Bronze.
8. Document metric definitions.
9. Build reconciliation and data-quality checks.
10. Avoid duplicating logic across multiple tables.

The physical data model should be driven by the business requirements above and validated against the actual available data.
"""

path = Path("/mnt/data/valorant_business_requirements_and_analytics.md")
path.write_text(content, encoding="utf-8")
print(path)
