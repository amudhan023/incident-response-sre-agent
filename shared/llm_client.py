"""Anthropic Claude wrapper with structured tool-use support."""
from __future__ import annotations
import logging
import os
import time
from typing import Any

import anthropic

logger = logging.getLogger(__name__)

_client: anthropic.Anthropic | None = None

SONNET = "claude-sonnet-4-6"
HAIKU  = "claude-haiku-4-5-20251001"


def get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client


def chat(
    system: str,
    user: str,
    model: str = SONNET,
    max_tokens: int = 2048,
    temperature: float = 0.2,
    max_retries: int = 3,
) -> str:
    """Simple single-turn chat. Returns assistant text."""
    client = get_client()
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
            return resp.content[0].text
        except anthropic.RateLimitError:
            wait = 60 * attempt
            logger.warning("Rate limited by Anthropic — waiting %ds (attempt %d)", wait, attempt)
            time.sleep(wait)
        except anthropic.APIError as exc:
            if attempt == max_retries:
                raise
            logger.warning("Anthropic API error (attempt %d): %s", attempt, exc)
            time.sleep(5 * attempt)
    raise RuntimeError("LLM call failed after all retries.")


def chat_with_tools(
    system: str,
    messages: list[dict],
    tools: list[dict],
    model: str = SONNET,
    max_tokens: int = 4096,
    max_rounds: int = 10,
) -> tuple[str, list[dict]]:
    """
    Multi-turn tool-use conversation.
    Returns (final_text, tool_call_log).
    The caller provides initial messages and tool definitions.
    This function handles the tool-use loop until the model stops calling tools.
    """
    client = get_client()
    conversation = list(messages)
    tool_log: list[dict] = []

    for _round in range(max_rounds):
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            tools=tools,
            messages=conversation,
        )

        # Collect all content items from this response
        assistant_content = resp.content
        conversation.append({"role": "assistant", "content": assistant_content})

        if resp.stop_reason == "end_turn":
            # Extract text from last assistant message
            text = next(
                (b.text for b in assistant_content if hasattr(b, "text")),
                "",
            )
            return text, tool_log

        if resp.stop_reason == "tool_use":
            # Process each tool call
            tool_results = []
            for block in assistant_content:
                if block.type != "tool_use":
                    continue
                tool_name   = block.name
                tool_input  = block.input
                tool_use_id = block.id

                logger.info("LLM calling tool: %s(%s)", tool_name, tool_input)

                # Signal to caller — actual execution happens via callback
                tool_log.append({"tool": tool_name, "input": tool_input, "id": tool_use_id})

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": f"__PENDING__{tool_use_id}",
                })

            # Return early with special signal for caller to fill results
            return "__TOOL_CALL__", tool_log

        # Unexpected stop reason
        logger.warning("Unexpected stop_reason: %s", resp.stop_reason)
        break

    return "", tool_log


def run_tool_use_agent(
    system: str,
    initial_prompt: str,
    tools: list[dict],
    tool_executor: Any,  # callable(tool_name, tool_input) -> str
    model: str = SONNET,
    max_tokens: int = 4096,
    max_rounds: int = 12,
) -> str:
    """
    Full agentic loop: handles tool calls automatically via tool_executor.
    Returns final assistant text.
    """
    client = get_client()
    messages = [{"role": "user", "content": initial_prompt}]

    for _round in range(max_rounds):
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            tools=tools,
            messages=messages,
        )

        assistant_content = resp.content
        messages.append({"role": "assistant", "content": assistant_content})

        if resp.stop_reason == "end_turn":
            return next((b.text for b in assistant_content if hasattr(b, "text")), "")

        if resp.stop_reason == "tool_use":
            tool_results = []
            for block in assistant_content:
                if block.type != "tool_use":
                    continue
                logger.info("Tool call: %s | input: %s", block.name, block.input)
                try:
                    result = tool_executor(block.name, block.input)
                except Exception as exc:
                    result = f"ERROR: {exc}"
                    logger.exception("Tool %s failed: %s", block.name, exc)

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": str(result),
                })

            messages.append({"role": "user", "content": tool_results})

    return "Investigation reached maximum rounds without conclusion."
