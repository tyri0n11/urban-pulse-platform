"""Ollama LLM client — streaming and non-streaming."""

import json
import logging
import os
from typing import AsyncGenerator

import httpx

logger = logging.getLogger(__name__)

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")


async def stream_ollama(
    system: str,
    prompt: str,
    *,
    temperature: float = 0.4,
    num_predict: int = 300,
) -> AsyncGenerator[str, None]:
    """Stream SSE chunks from Ollama generate API (single-turn)."""
    payload = {
        "model": _MODEL,
        "system": system,
        "prompt": prompt,
        "stream": True,
        "options": {"temperature": temperature, "num_predict": num_predict},
    }
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", f"{_OLLAMA_URL}/api/generate", json=payload) as resp:
                if resp.status_code != 200:
                    yield f"data: {json.dumps({'error': f'Ollama {resp.status_code}'})}\n\n"
                    return
                async for line in resp.aiter_lines():
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    chunk = data.get("response", "")
                    if chunk:
                        yield f"data: {json.dumps({'chunk': chunk})}\n\n"
                    if data.get("done"):
                        yield f"data: {json.dumps({'done': True})}\n\n"
                        return
    except httpx.TimeoutException:
        yield f"data: {json.dumps({'error': 'LLM timeout'})}\n\n"
    except httpx.HTTPError as exc:
        logger.error("llm: Ollama connection error: %s", exc)
        yield f"data: {json.dumps({'error': 'Không thể kết nối tới Ollama'})}\n\n"


async def stream_ollama_chat(
    system: str,
    history: list[dict[str, str]],
    user_message: str,
    *,
    temperature: float = 0.4,
    num_predict: int = 300,
) -> AsyncGenerator[str, None]:
    """Stream SSE chunks from Ollama chat API (multi-turn with session history)."""
    messages = (
        [{"role": "system", "content": system}]
        + history
        + [{"role": "user", "content": user_message}]
    )
    payload = {
        "model": _MODEL,
        "messages": messages,
        "stream": True,
        "options": {"temperature": temperature, "num_predict": num_predict},
    }
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", f"{_OLLAMA_URL}/api/chat", json=payload) as resp:
                if resp.status_code != 200:
                    yield f"data: {json.dumps({'error': f'Ollama {resp.status_code}'})}\n\n"
                    return
                async for line in resp.aiter_lines():
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    chunk = data.get("message", {}).get("content", "")
                    if chunk:
                        yield f"data: {json.dumps({'chunk': chunk})}\n\n"
                    if data.get("done"):
                        yield f"data: {json.dumps({'done': True})}\n\n"
                        return
    except httpx.TimeoutException:
        yield f"data: {json.dumps({'error': 'LLM timeout'})}\n\n"
    except httpx.HTTPError as exc:
        logger.error("llm: Ollama chat connection error: %s", exc)
        yield f"data: {json.dumps({'error': 'Không thể kết nối tới Ollama'})}\n\n"


async def ask_llm(
    system: str,
    prompt: str,
    *,
    temperature: float = 0.4,
    num_predict: int = 350,
) -> str:
    """Non-streaming Ollama call — returns full response text."""
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
        ) as client:
            resp = await client.post(
                f"{_OLLAMA_URL}/api/generate",
                json={
                    "model": _MODEL,
                    "system": system,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": temperature, "num_predict": num_predict},
                },
            )
            resp.raise_for_status()
            return resp.json().get("response", "").strip()  # type: ignore[no-any-return]
    except Exception as exc:
        logger.error("llm: ask_llm failed — %s", exc)
        return "Xin lỗi, tôi không thể kết nối tới LLM lúc này."


async def check_ollama_status(model: str | None = None) -> dict[str, object]:
    """Return Ollama reachability and loaded model status."""
    target = model or _MODEL
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{_OLLAMA_URL}/api/tags")
            tags = resp.json()
            models = [m["name"] for m in tags.get("models", [])]
            return {
                "ollama_reachable": True,
                "model": target,
                "model_ready": any(target in m for m in models),
                "available_models": models,
            }
    except Exception as exc:
        return {"ollama_reachable": False, "model": target, "model_ready": False, "error": str(exc)}
