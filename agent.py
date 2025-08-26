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

AGENT_INSTRUCTIONS = (
    "You are an autonomous trading assistant.\n"
    "- Use only MCP tools from the 'alpaca' server.\n"
    "- Prefer PAPER trading and small/conservative position sizes.\n"
    "- take advantage of options trading only. We want big hits"
    "- First, summarize the current positions, even if there are none, say this"
    "- Then, Explain your plan, risks, and reasoning before placing any order.\n"
    "- Summarize actions and current positions before finishing."
    "- if there are issues or errors, please respond with any debug info you have access to"
    "- Thoroughly justify and explain each decision, provide a confidence score for each, even if the trade couldnt be executed, explain why you WOULD have done something"
)

USER_GOAL = (
    "Maximize profits on paper today using liquid tickers. "
    "Fetch current data, propose a plan with risk notes, "
    "then place SMALL paper orders if justified and show positions."
    "Thoroughly explain why each decision was made."
)

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
    }

    async with MCPServerStreamableHttp(
        params=mcp_params,
        name="alpaca",
        cache_tools_list=True,
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
        result = await Runner.run(agent, USER_GOAL)

        print("\n===== FINAL OUTPUT =====\n")
        final: Optional[str] = getattr(result, "final_output", None) or getattr(result, "text", None)
        print(final or str(result))

if __name__ == "__main__":
    asyncio.run(main())
