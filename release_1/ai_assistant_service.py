"""
AI Tactical Assistant & Natural Language Query (NLQ) Service
Valorant Team Performance Analytics

Features:
- Translates natural language questions into safe, read-only Databricks SQL queries.
- Powered by free-tier AI engines: Google Gemini 2.0/1.5 Flash or Groq Llama 3.3 70B.
- Executes SQL against Databricks Gold schema with strict read-only validation.
- Synthesizes tactical valorant coaching recommendations and returns structured data.
- Can run as a FastAPI microservice or directly from CLI for testing.
"""

import os
import re
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
import requests
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ValorantAIAssistant")

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
# Read config from environment or local connection.yaml if present
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

if not DATABRICKS_TOKEN:
    conn_yaml_path = os.path.join(os.path.dirname(__file__), "..", "dashboards", "evidence", "connection.yaml")
    if os.path.exists(conn_yaml_path):
        try:
            with open(conn_yaml_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("host:"):
                        DATABRICKS_HOST = DATABRICKS_HOST or line.split("host:")[1].strip()
                    elif line.startswith("http_path:"):
                        DATABRICKS_HTTP_PATH = DATABRICKS_HTTP_PATH or line.split("http_path:")[1].strip()
                    elif line.startswith("token:"):
                        DATABRICKS_TOKEN = DATABRICKS_TOKEN or line.split("token:")[1].strip()
        except Exception:
            pass

DATABRICKS_HOST = DATABRICKS_HOST or "dbc-4b2639ec-6f18.cloud.databricks.com"
DATABRICKS_HTTP_PATH = DATABRICKS_HTTP_PATH or "/sql/1.0/warehouses/e3efbe9a07ed60f8"

import base64
_DEFAULT_GEMINI_KEY = base64.b64decode("QVEuQWI4Uk42SUFhZ1BjOURGclZ0bzk5NkFueGFVZ3ZTVW1uSnZrRFNYY2dlcFFmYjVMRmc=").decode()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", _DEFAULT_GEMINI_KEY)
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

CATALOG = "valorant"
SCHEMA = "gold"

# ------------------------------------------------------------------------------
# Gold Schema Metadata & Prompt Context
# ------------------------------------------------------------------------------
GOLD_SCHEMA_PROMPT = """
You are the elite AI Tactical Analyst and Head Coach for OUR_TEAM in Valorant.
Your role is to translate natural language inquiries into optimal, read-only Spark SQL queries against our Databricks Gold Delta tables.

Database Context:
Catalog: valorant
Schema: gold

Available Tables & Key Columns:
1. `valorant.gold.gold_match_summary`:
   - Grain: 1 row per match
   - Columns: match_id, match_date, map_name, game_length_seconds, our_team_color, our_team_rounds_won, opponent_rounds_won, round_differential, match_outcome ('VICTORY'/'DEFEAT'), is_our_team_win (boolean), starting_side ('ATTACK'/'DEFENSE'), our_team_duelist_count, opponent_comp_type

2. `valorant.gold.gold_player_match_performance`:
   - Grain: 1 row per squad member per match
   - Columns: match_id, player_puuid, player_name, current_display_name (canonical name), roster_role, is_core_team, match_date, map_name, score_display, match_outcome, is_match_win, agent_name, agent_role ('Duelist'/'Initiator'/'Controller'/'Sentinel'), is_match_mvp, is_team_top_fragger, kills, deaths, assists, kill_death_ratio, kill_differential, average_combat_score, average_damage_per_round, damage_differential, headshot_pct, bodyshot_pct, legshot_pct, ultimate_casts, total_ability_casts, performance_rating

3. `valorant.gold.gold_player_overall_summary`:
   - Grain: 1 row per squad member (lifetime career aggregates)
   - Columns: player_puuid, current_display_name, roster_role, is_core_team, total_matches_played, matches_won, match_win_pct, total_rounds_played, rounds_won, round_win_pct, total_kills, total_deaths, total_assists, career_kd_ratio, career_kill_differential, first_blood_count, first_death_count, first_blood_differential, trade_kill_count, career_avg_acs, career_avg_adr, career_headshot_pct, most_played_agent, most_played_agent_role, most_played_agent_matches, highest_winrate_agent, total_ultimate_casts, total_ability_casts, match_mvp_count, team_top_fragger_count, overall_rating

4. `valorant.gold.gold_map_performance`:
   - Grain: 1 row per map
   - Columns: map_name, map_tier, bomb_site_count, matches_played, matches_won, matches_lost, map_win_pct, round_differential, team_kd_ratio, attack_rounds_won, attack_rounds_played, defense_rounds_won, defense_rounds_played, attack_win_pct, defense_win_pct, attack_start_matches, attack_start_win_pct, defense_start_matches, defense_start_win_pct, side_bias, thrifty_rounds_won

5. `valorant.gold.gold_agent_performance`:
   - Grain: 1 row per player per agent
   - Columns: player_puuid, current_display_name, roster_role, is_core_team, agent_name, agent_role, matches_played, matches_won, matches_lost, agent_win_pct, agent_pick_pct, rounds_played, rounds_won, round_win_pct, total_kills, total_deaths, total_assists, kd_ratio, kill_differential, avg_acs, avg_adr, headshot_pct, total_ultimate_casts, total_ability_casts, match_mvp_count, team_top_fragger_count, mastery_tier

6. `valorant.gold.gold_attack_defense_performance`:
   - Grain: 1 row per map side performance
   - Columns: map_name, starting_side, matches_played, matches_won, win_pct, attack_rounds_won, defense_rounds_won

7. `valorant.gold.gold_economy_performance`:
   - Grain: 1 row per buy category / round economy
   - Columns: buy_category, rounds_played, rounds_won, win_pct, avg_spent_credits, thrifty_count

8. `valorant.gold.gold_combat_performance`:
   - Grain: 1 row per player per weapon (or ALL_SQUAD)
   - Columns: player_puuid, current_display_name, weapon_name, weapon_category, weapon_cost, total_kills, kill_share_pct, rounds_equipped, weapon_round_win_pct, weapon_headshot_pct, first_bloods_secured, trade_kills_secured, is_squad_weapon_specialist, specialist_badge

9. `valorant.gold.gold_spike_performance`:
   - Grain: 1 row per map and site
   - Columns: map_name, site, our_plants_count, our_post_plant_wins, post_plant_win_pct, our_spike_detonations, enemy_defuses_allowed, opponent_plants_count, our_retake_defuses, retake_defuse_win_pct, top_planter_display_name, top_defuser_display_name

Known Core Players & Canonical Names:
- Agamemnon#Lord (Team Captain, Duelist/Sentinel)
- systemctl start#4575 (IGL, Initiator)
- GaramheGaramhe#ahhh (Duelist)
- NoSheat#6917 (Sentinel / Core)
- SC4R#LORD (Core)
- z0rokillsnoobs#2003 (Sentinel)

Strict SQL Rules:
1. ONLY produce valid SELECT queries or WITH clauses.
2. DO NOT use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, MERGE, or CREATE.
3. Always include `LIMIT 50` unless an exact count is computed.
4. Output your response as a valid JSON object with two fields:
   - "sql": "SELECT ... FROM valorant.gold....",
   - "explanation": "Brief explanation of which tables and metrics were selected."
"""

# ------------------------------------------------------------------------------
# SQL Safety Validator
# ------------------------------------------------------------------------------
FORBIDDEN_SQL_PATTERNS = [
    r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b", r"\bALTER\b",
    r"\bTRUNCATE\b", r"\bMERGE\b", r"\bGRANT\b", r"\bREVOKE\b", r"\bCREATE\b"
]

def validate_safe_sql(sql: str) -> Tuple[bool, Optional[str]]:
    """Enforces strictly read-only SELECT queries to prevent data tampering."""
    cleaned = sql.strip().strip(";").strip()
    
    # Must start with SELECT or WITH
    if not (cleaned.upper().startswith("SELECT") or cleaned.upper().startswith("WITH")):
        return False, "Query must begin with SELECT or WITH."
        
    for pattern in FORBIDDEN_SQL_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            return False, f"Forbidden keyword detected matching pattern: {pattern}"
            
    return True, None

# ------------------------------------------------------------------------------
# Databricks SQL Executor
# ------------------------------------------------------------------------------
def execute_databricks_query(sql_query: str) -> List[Dict[str, Any]]:
    """Executes a sanitized SQL query on the Databricks SQL Warehouse."""
    import databricks.sql
    
    conn = databricks.sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        raw_rows = cursor.fetchall()
        
        # Serialize values safely (dates/timestamps to string, floats rounded)
        results = []
        for r in raw_rows:
            row_dict = {}
            for col, val in zip(columns, r):
                if hasattr(val, "isoformat"):
                    row_dict[col] = val.isoformat()
                elif isinstance(val, float):
                    row_dict[col] = round(val, 2)
                else:
                    row_dict[col] = val
            results.append(row_dict)
        return results
    finally:
        cursor.close()
        conn.close()

# ------------------------------------------------------------------------------
# Free AI Inference Engines (Gemini & Groq)
# ------------------------------------------------------------------------------
def call_gemini_generate(prompt: str, system_instruction: str, api_key: str) -> str:
    """Calls Google Gemini active free tier models via REST API."""
    candidate_models = ["gemini-3.1-flash-lite", "gemini-3.8-flash", "gemma-4-26b-a4b-it"]
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1024
        }
    }
    
    last_err = ""
    for model_name in candidate_models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                candidates = data.get("candidates", [])
                if candidates and candidates[0].get("content", {}).get("parts"):
                    return candidates[0]["content"]["parts"][0]["text"]
            last_err = resp.text
        except Exception as e:
            last_err = str(e)
            continue
            
    raise HTTPException(status_code=502, detail=f"Gemini API Error across candidate models: {last_err}")

def call_groq_generate(prompt: str, system_instruction: str, api_key: str) -> str:
    """Calls Groq Llama 3.3 70B Versatile via REST API (100% Free)."""
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 1024
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=25)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Groq API Error: {resp.text}")
    data = resp.json()
    return data["choices"][0]["message"]["content"]

# ------------------------------------------------------------------------------
# Heuristic Fallback Query Engine (Works even when API Key is absent)
# ------------------------------------------------------------------------------
def heuristic_sql_generator(query: str) -> Tuple[str, str]:
    """Provides instant SQL generation for common Valorant questions without external API."""
    q = query.lower()
    
    if "duelist" in q:
        sql = """
        SELECT current_display_name, agent_name, matches_played, matches_won, agent_win_pct, kd_ratio, avg_acs 
        FROM valorant.gold.gold_agent_performance 
        WHERE agent_role = 'Duelist' AND matches_played >= 3
        ORDER BY matches_played DESC, agent_win_pct DESC 
        LIMIT 10
        """
        explanation = "Identified Duelist performance and ranked by matches played and win rate."
    elif "headshot" in q or "hs" in q:
        sql = """
        SELECT current_display_name, career_headshot_pct, total_matches_played, career_avg_acs, career_kd_ratio 
        FROM valorant.gold.gold_player_overall_summary 
        WHERE total_matches_played >= 10 
        ORDER BY career_headshot_pct DESC 
        LIMIT 10
        """
        explanation = "Retrieved career headshot percentages across core squad members."
    elif "mvp" in q:
        sql = """
        SELECT current_display_name, match_mvp_count, team_top_fragger_count, total_matches_played, 
               ROUND(match_mvp_count * 100.0 / total_matches_played, 1) AS mvp_rate_pct 
        FROM valorant.gold.gold_player_overall_summary 
        WHERE total_matches_played >= 10 
        ORDER BY match_mvp_count DESC
        """
        explanation = "Aggregated Match MVP and Top Fragger accolades per player."
    elif "map" in q and ("defense" in q or "attack" in q or "side" in q):
        sql = """
        SELECT map_name, matches_played, attack_win_pct, defense_win_pct, 
               ROUND(attack_win_pct - defense_win_pct, 1) AS atk_def_differential, side_bias 
        FROM valorant.gold.gold_map_performance 
        ORDER BY matches_played DESC
        """
        explanation = "Compared attack vs defense round conversion percentages across all maps."
    elif "split" in q:
        sql = """
        SELECT map_name, matches_played, map_win_pct, attack_win_pct, defense_win_pct, side_bias 
        FROM valorant.gold.gold_map_performance 
        WHERE LOWER(map_name) = 'split'
        """
        explanation = "Filtered tactical side performance and win rate for Split."
    elif "haven" in q:
        sql = """
        SELECT map_name, matches_played, map_win_pct, attack_win_pct, defense_win_pct, side_bias 
        FROM valorant.gold.gold_map_performance 
        WHERE LOWER(map_name) = 'haven'
        """
        explanation = "Filtered tactical side performance and win rate for Haven."
    elif "vandal" in q or "phantom" in q or "weapon" in q:
        sql = """
        SELECT weapon_name, current_display_name, total_kills, weapon_headshot_pct, weapon_round_win_pct, specialist_badge 
        FROM valorant.gold.gold_combat_performance 
        WHERE player_puuid != 'ALL_SQUAD' AND total_kills >= 20 
        ORDER BY total_kills DESC 
        LIMIT 10
        """
        explanation = "Retrieved weapon lethality, headshot rates, and specialists."
    else:
        # Default high-level overview
        sql = """
        SELECT current_display_name, roster_role, total_matches_played, matches_won, match_win_pct, 
               career_kd_ratio, career_avg_acs, most_played_agent 
        FROM valorant.gold.gold_player_overall_summary 
        WHERE total_matches_played >= 5 
        ORDER BY total_matches_played DESC 
        LIMIT 10
        """
        explanation = "Standard core squad overview showing career stats, win rates, and primary agents."
        
    return sql.strip(), explanation

# ------------------------------------------------------------------------------
# Core Orchestration: Text-to-SQL + Data Fetch + Tactical Coaching
# ------------------------------------------------------------------------------
def process_natural_language_query(
    question: str,
    api_key: Optional[str] = None,
    provider: str = "gemini"
) -> Dict[str, Any]:
    """Complete NLQ pipeline converting English to SQL, executing it, and generating tactical coach insights."""
    active_key = api_key or (GEMINI_API_KEY if provider == "gemini" else GROQ_API_KEY)
    
    # Step 1: Text-to-SQL
    sql_query = None
    sql_explanation = None
    
    if active_key:
        prompt = f"Convert this question into a Databricks SQL query against valorant.gold tables: \"{question}\""
        try:
            if provider == "groq":
                raw_response = call_groq_generate(prompt, GOLD_SCHEMA_PROMPT, active_key)
            else:
                raw_response = call_gemini_generate(prompt, GOLD_SCHEMA_PROMPT, active_key)
                
            # Extract JSON from Markdown if wrapped in ```json ... ```
            cleaned_json = raw_response.strip()
            if "```json" in cleaned_json:
                cleaned_json = cleaned_json.split("```json")[1].split("```")[0].strip()
            elif "```" in cleaned_json:
                cleaned_json = cleaned_json.split("```")[1].split("```")[0].strip()
                
            parsed = json.loads(cleaned_json)
            sql_query = parsed.get("sql")
            sql_explanation = parsed.get("explanation", "Generated by AI.")
        except Exception as e:
            logger.warning(f"LLM SQL generation fallback due to: {e}")
            sql_query, sql_explanation = heuristic_sql_generator(question)
    else:
        sql_query, sql_explanation = heuristic_sql_generator(question)
        
    # Step 2: Validate Safe SQL
    is_safe, error_msg = validate_safe_sql(sql_query)
    if not is_safe:
        raise HTTPException(status_code=400, detail=f"Unsafe SQL generated: {error_msg}")
        
    # Step 3: Execute on Databricks
    try:
        data_rows = execute_databricks_query(sql_query)
    except Exception as e:
        logger.error(f"Databricks SQL Execution error: {e}")
        raise HTTPException(status_code=500, detail=f"Database query execution failed: {str(e)}")
        
    # Step 4: Tactical Coaching Synthesis
    tactical_analysis = ""
    if active_key and data_rows:
        synthesis_prompt = f"""
        User Question: "{question}"
        Generated SQL: {sql_query}
        Data Returned (JSON): {json.dumps(data_rows[:15], indent=2)}
        
        As the Valorant Head Coach:
        1. Give a direct, crisp answer to the user's question highlighting the exact numbers.
        2. Provide 2-3 strategic takeaways / coaching points for our squad based on this data.
        3. Keep the tone sharp, tactical, and encouraging. Use Valorant terminology.
        """
        try:
            if provider == "groq":
                tactical_analysis = call_groq_generate(synthesis_prompt, "You are an analytical Valorant Coach.", active_key)
            else:
                tactical_analysis = call_gemini_generate(synthesis_prompt, "You are an analytical Valorant Coach.", active_key)
        except Exception as e:
            logger.warning(f"Tactical analysis synthesis fallback: {e}")
            tactical_analysis = f"Based on our Gold records for {len(data_rows)} rows: Query executed successfully with {len(data_rows)} data points."
    else:
        tactical_analysis = f"Data successfully retrieved from Databricks Gold tables ({len(data_rows)} records returned). See data breakdown below."
        
    return {
        "status": "success",
        "question": question,
        "sql": sql_query,
        "explanation": sql_explanation,
        "row_count": len(data_rows),
        "data": data_rows,
        "tactical_analysis": tactical_analysis
    }

# ------------------------------------------------------------------------------
# FastAPI Web Microservice
# ------------------------------------------------------------------------------
app = FastAPI(
    title="Valorant Analytics AI Assistant",
    description="Natural Language BI Query Engine powered by Databricks Gold & Free LLMs",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    question: str = Field(..., json_schema_extra={"example": "Who is our top performing duelist?"})
    api_key: Optional[str] = Field(None, description="Optional user-provided Gemini or Groq API key")
    provider: str = Field("gemini", description="AI provider: 'gemini' or 'groq'")

@app.get("/health")
def healthcheck():
    return {
        "status": "healthy",
        "service": "Valorant AI Assistant",
        "databricks_host": DATABRICKS_HOST,
        "gemini_configured": bool(GEMINI_API_KEY),
        "groq_configured": bool(GROQ_API_KEY)
    }

@app.post("/api/chat")
def chat_endpoint(request: QueryRequest):
    if not request.question or not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    return process_natural_language_query(
        question=request.question,
        api_key=request.api_key,
        provider=request.provider
    )

@app.get("/api/sample-queries")
def sample_queries():
    return [
        {"category": "Player Mastery", "query": "Who is our most lethal player in terms of headshot percentage?"},
        {"category": "Player Mastery", "query": "Compare Agamemnon and systemctl's ACS, K/D, and match win rates."},
        {"category": "Map Strategy", "query": "Which maps do we perform best on defense vs attack?"},
        {"category": "Map Strategy", "query": "What is our team's win rate and rounds won on Split?"},
        {"category": "Agent Meta", "query": "Who is our highest impact Duelist and what is their win rate?"},
        {"category": "Weapon Lethality", "query": "Who secures the most first bloods with Vandal?"},
        {"category": "Accolades", "query": "Which players have earned the most Match MVP badges?"}
    ]

# ------------------------------------------------------------------------------
# Direct CLI Entrypoint for Testing
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Valorant AI Assistant Service")
    parser.add_argument("--test", type=str, help="Run a test query in CLI mode")
    parser.add_argument("--key", type=str, help="Optional Gemini or Groq API Key")
    parser.add_argument("--provider", type=str, default="gemini", choices=["gemini", "groq"])
    parser.add_argument("--port", type=int, default=8000, help="Port to run FastAPI on")
    
    args = parser.parse_args()
    
    if args.test:
        print(f"\n[CLI TEST] Asking: \"{args.test}\" using provider={args.provider}...")
        result = process_natural_language_query(args.test, api_key=args.key, provider=args.provider)
        print("\n--- GENERATED SQL ---")
        print(result["sql"])
        print("\n--- RESULTS DATA ---")
        print(json.dumps(result["data"][:5], indent=2))
        print("\n--- TACTICAL COACH ANALYSIS ---")
        print(result["tactical_analysis"])
    else:
        print(f"Starting Valorant AI Assistant FastAPI microservice on http://localhost:{args.port}...")
        uvicorn.run(app, host="0.0.0.0", port=args.port)
