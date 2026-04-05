"""Router: Telegram bot webhook — lets you chat with Urban Pulse AI via Telegram.

Setup (run once):
    POST https://api.telegram.org/bot{TOKEN}/setWebhook
         {"url": "https://your-domain/telegram/webhook"}

For local dev, use ngrok:
    ngrok http 8001
    curl -X POST "https://api.telegram.org/bot{TOKEN}/setWebhook" \
         -d "url=https://<ngrok-id>.ngrok-free.app/telegram/webhook"

Incoming messages are routed to the same /chat logic (with snapshot context).
"""

import json
import logging
import os
from typing import Any

import asyncpg
import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from serving.dependencies import get_db

router = APIRouter(prefix="/telegram", tags=["telegram"])
logger = logging.getLogger(__name__)

_TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
_TELEGRAM_API = f"https://api.telegram.org/bot{_TELEGRAM_BOT_TOKEN}"


async def _send_message(chat_id: int, text: str) -> None:
    """Send a Telegram message (plain text, max 4096 chars)."""
    if not _TELEGRAM_BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{_TELEGRAM_API}/sendMessage",
                json={"chat_id": chat_id, "text": text[:4096], "parse_mode": "HTML"},
            )
    except Exception as exc:
        logger.error("telegram: send failed — %s", exc)


async def _send_typing(chat_id: int) -> None:
    if not _TELEGRAM_BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{_TELEGRAM_API}/sendChatAction",
                json={"chat_id": chat_id, "action": "typing"},
            )
    except Exception:
        pass


async def _ask_llm(system: str, prompt: str) -> str:
    """Non-streaming Ollama call — returns full response text."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)) as client:
            resp = await client.post(
                f"{_OLLAMA_URL}/api/generate",
                json={
                    "model": _MODEL,
                    "system": system,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.4, "num_predict": 350},
                },
            )
            resp.raise_for_status()
            return resp.json().get("response", "").strip()
    except Exception as exc:
        logger.error("telegram: LLM call failed — %s", exc)
        return "Xin lỗi, tôi không thể kết nối tới LLM lúc này."


async def _fetch_snapshot(conn: asyncpg.Connection) -> dict[str, Any]:
    """Reuse the same snapshot logic as /chat."""
    # Import here to avoid circular — this mirrors chat.py's _fetch_system_snapshot
    from serving.routers.chat import _fetch_system_snapshot
    return await _fetch_system_snapshot(conn)


def _build_prompt(snapshot: dict[str, Any], message: str) -> tuple[str, str]:
    """Build (system, user_prompt) for the Telegram chat context."""
    from serving.geo_knowledge import build_system_prompt
    from serving.routers.chat import _build_user_prompt

    system = build_system_prompt(
        "You are Urban Pulse AI, a traffic assistant for Ho Chi Minh City. "
        "You receive live congestion data and answer questions concisely. "
        "Answer in the same language the user writes in. "
        "Keep answers under 5 sentences unless more detail is explicitly requested. "
        "No greetings or sign-offs."
    )
    # Detect language — default Vietnamese
    lang = "en" if any(c.isascii() and c.isalpha() for c in message[:20]) and not any(
        c in message for c in "àáảãạăắặẵặâấầẩẫậđèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵ"
    ) else "vi"

    user_prompt = _build_user_prompt(snapshot, message, lang)
    return system, user_prompt


@router.post("/webhook")
async def telegram_webhook(
    request: Request,
    conn: asyncpg.Connection = Depends(get_db),
) -> JSONResponse:
    """Receive Telegram updates and reply with LLM-powered responses."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(content={"ok": True})

    # Handle regular messages only
    message = body.get("message", {})
    if not message:
        return JSONResponse(content={"ok": True})

    chat_id: int = message.get("chat", {}).get("id", 0)
    text: str = message.get("text", "").strip()

    if not chat_id or not text:
        return JSONResponse(content={"ok": True})

    # Ignore bot commands for now (could extend later)
    if text.startswith("/start"):
        await _send_message(
            chat_id,
            "👋 Xin chào! Tôi là <b>Urban Pulse AI</b> — trợ lý phân tích giao thông TPHCM.\n\n"
            "Hỏi tôi về tình trạng giao thông hiện tại, bất thường, hoặc bất kỳ tuyến đường nào.\n"
            "Ví dụ: <i>Tại sao tuyến zone1 lên zone4 đang bị tắc?</i>",
        )
        return JSONResponse(content={"ok": True})

    # Show typing indicator then generate response
    await _send_typing(chat_id)

    try:
        snapshot = await _fetch_snapshot(conn)
        system, prompt = _build_prompt(snapshot, text)
        reply = await _ask_llm(system, prompt)
    except Exception as exc:
        logger.error("telegram: handler error — %s", exc)
        reply = "Có lỗi xảy ra khi xử lý câu hỏi của bạn."

    await _send_message(chat_id, reply)
    logger.info("telegram: chat_id=%d msg_len=%d replied", chat_id, len(text))

    return JSONResponse(content={"ok": True})


@router.get("/set-webhook")
async def set_webhook(url: str) -> dict[str, Any]:
    """Convenience endpoint to register the Telegram webhook URL."""
    if not _TELEGRAM_BOT_TOKEN:
        return {"error": "TELEGRAM_BOT_TOKEN not configured"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{_TELEGRAM_API}/setWebhook",
            json={"url": f"{url}/telegram/webhook"},
        )
        return resp.json()


@router.get("/info")
async def bot_info() -> dict[str, Any]:
    """Return bot info (to verify token works)."""
    if not _TELEGRAM_BOT_TOKEN:
        return {"error": "TELEGRAM_BOT_TOKEN not configured"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{_TELEGRAM_API}/getMe")
        return resp.json()
