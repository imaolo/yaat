import os
import asyncio
import logging
import inspect
from typing import Optional
from agents import enable_verbose_stdout_logging
enable_verbose_stdout_logging()

from dotenv import load_dotenv

from agents import Agent, Runner , set_tracing_disabled
from agents.mcp import MCPServerStreamableHttp  # SSE-capable MCP transport

set_tracing_disabled(True)

load_dotenv()


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger("mcp-agent")

ALPACA_MCP_URL = os.environ.get("ALPACA_MCP_URL", "http://127.0.0.1:8000/mcp")
MODEL_ID = os.environ.get("MODEL_ID", "gpt-4o-mini")

if not os.environ.get("OPENAI_API_KEY"):
    raise SystemExit("Set OPENAI_API_KEY in your environment.")

AGENT_INSTRUCTIONS = """
🔹 Agent Instructions (system-level behavior)

You are an autonomous options trading assistant.

Tool Use
---------
- Use only MCP tools from the Alpaca server.
- Always fetch current account status and open positions first before proposing trades.
- Work in PAPER trading mode by default.

Trading Focus
--------------
- Trade options only (calls, puts, spreads).
- Prioritize liquid tickers (high volume, tight spreads).
- Target short-term opportunities from intra-day and inter-day volatility.

Risk Management
----------------
- Never risk more than 2–5% of account equity on a single trade.
- Use defined-risk strategies (spreads, debit/credit, protective stops) unless conviction is very high.
- Evaluate each trade with a risk/reward ratio and note potential drawdowns.

Decision Process
-----------------
1. Summarize current portfolio and market context.
2. Identify potential trades and justify with reasoning (volatility, catalysts, technical signals, option Greeks).
3. Present risks, expected reward, and confidence score (0–100%).
4. AUTONOMY: If the rationale is valid and risk constraints are satisfied, immediately place a SMALL paper options order using Alpaca MCP tools. Do NOT ask the user for confirmation.
5. After placing the order, display the order ticket (symbol, contract_id, side, qty, type, limit/market, time_in_force) and the updated portfolio snapshot (positions, cash, buying power, P&L).
6. If a tool call fails or required data is missing, provide debug info AND choose a reasonable fallback (e.g., pick the nearest liquid contract) to attempt execution once. Only skip execution if it violates risk limits; in that case, explain why.
7. End each turn with: (a) actions taken (or precisely why none), (b) open positions, (c) next monitoring trigger (price/time/volatility) and what action you will take on that trigger.

Behavior Under Errors
----------------------
- If a tool call fails (e.g., get_option_contracts timeout), provide debug info and still state what you would have done.
"""

AGENT_GOALS = """
🔹 Agent Goals (user-facing objectives)

Maximize Profits Through Volatility
------------------------------------
- Exploit short-term market swings with option trades (day trades, overnight holds).
- Capture both directional and non-directional moves (spreads, straddles, strangles).

Mitigate Risk
--------------
- Prioritize capital preservation.
- Diversify across tickers/strategies when appropriate.
- Avoid overexposure to a single ticker or sector.

Stay Adaptive
--------------
- React to both scheduled events (earnings, Fed announcements) and unscheduled volatility.
- Continuously reassess open trades, cutting losers early if justified.

Explain Reasoning Clearly
--------------------------
- Always show why a trade is being considered.
- Explain Greeks impact (delta, gamma, theta, vega).
- Provide a confidence score and clear risk/reward math.

Transparent Execution
----------------------
- Clearly display orders placed, fills, and positions.
- Maintain a running PnL summary (realized & unrealized).
"""

def _agent_kwargs():
    """Build kwargs compatible with your Agent signature."""
    params = inspect.signature(Agent).parameters
    base = dict(name="Autonomous Trader", instructions=AGENT_INSTRUCTIONS)

    # Attach MCP servers at construction time if supported
    if "mcp_servers" in params:
        base["mcp_servers"] = []  # fill later after we create the handle

    # Optional model knobs (common variants)
    if "model" in params:
        base["model"] = MODEL_ID
    elif "model_id" in params:
        base["model_id"] = MODEL_ID
    # If neither is present, we rely on your framework's default (env-driven)

    return base

async def main() -> None:
    mcp_params = {
        "url": ALPACA_MCP_URL,
        "headers": {"Accept": "text/event-stream, application/json"},
        "timeout": 30.0
    }

    async with MCPServerStreamableHttp(
        params=mcp_params,
        name="alpaca",
        cache_tools_list=True,
        client_session_timeout_seconds=30.0
    ) as alpaca_mcp:

        # Sanity: list tools once
        try:
            tools_resp = await alpaca_mcp.list_tools()
            tools = getattr(tools_resp, "tools", tools_resp) or []
            log.info("=== Tools on 'alpaca' MCP ===")
            for t in tools:
                desc = getattr(t, "description", "") or ""
                log.info(" - %s%s", t.name, f": {desc}" if desc else "")
        except Exception as e:
            log.warning("Tool listing failed (continuing): %s", e)

        # Build Agent with signature-safe kwargs
        kwargs = _agent_kwargs()
        agent = Agent(**kwargs)

        # If Agent didn’t accept mcp_servers in __init__, attach post-hoc if possible
        if "mcp_servers" in inspect.signature(Agent).parameters:
            agent.mcp_servers = [alpaca_mcp]
        else:
            # Some frameworks expose a setter or register method
            if hasattr(agent, "register_mcp_server"):
                agent.register_mcp_server(alpaca_mcp)
            elif hasattr(agent, "mcp") and isinstance(getattr(agent, "mcp"), list):
                agent.mcp.append(alpaca_mcp)
            else:
                # Last resort: set attribute (many simple frameworks allow this)
                setattr(agent, "mcp_servers", [alpaca_mcp])

        # Run the plan
        result = await Runner.run(agent, AGENT_GOALS)

        print("\n===== FINAL OUTPUT =====\n")
        final: Optional[str] = getattr(result, "final_output", None) or getattr(result, "text", None)
        print(final or str(result))

if __name__ == "__main__":
    asyncio.run(main())
