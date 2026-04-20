"""Router: Telegram bot webhook integration."""

import logging
import os
from typing import Any

import asyncpg
import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from serving.controllers.chat_controller import fetch_system_snapshot, build_user_prompt
from serving.dependencies import get_db
from serving.geo_knowledge import build_system_prompt as build_geo_system_prompt
from serving.services.llm_service import ask_llm

router = APIRouter(prefix="/telegram", tags=["telegram"])
logger = logging.getLogger(__name__)

_TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TELEGRAM_API = f"https://api.telegram.org/bot{_TELEGRAM_BOT_TOKEN}"


async def _send_message(chat_id: int, text: str) -> None:
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


def _detect_lang(message: str) -> str:
    vi_chars = "àáảãạăắặẵặâấầẩẫậđèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵ"
    is_ascii_dominant = any(c.isascii() and c.isalpha() for c in message[:20])
    has_vi = any(c in message for c in vi_chars)
    return "vi" if not is_ascii_dominant or has_vi else "en"


def _build_telegram_system() -> str:
    return build_geo_system_prompt(
        "You are Urban Pulse AI, a traffic assistant for Ho Chi Minh City. "
        "You receive live congestion data and answer questions concisely. "
        "Answer in the same language the user writes in. "
        "Keep answers under 5 sentences unless more detail is explicitly requested. "
        "No greetings or sign-offs."
    )


@router.post("/webhook")
async def telegram_webhook(
    request: Request,
    conn: asyncpg.Connection = Depends(get_db),
) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(content={"ok": True})

    message = body.get("message", {})
    if not message:
        return JSONResponse(content={"ok": True})

    chat_id: int = message.get("chat", {}).get("id", 0)
    text: str = message.get("text", "").strip()

    if not chat_id or not text:
        return JSONResponse(content={"ok": True})

    if text.startswith("/start"):
        await _send_message(
            chat_id,
            "👋 Xin chào! Tôi là <b>Urban Pulse AI</b> — trợ lý phân tích giao thông TPHCM.\n\n"
            "Hỏi tôi về tình trạng giao thông hiện tại, bất thường, hoặc bất kỳ tuyến đường nào.\n"
            "Ví dụ: <i>Tại sao tuyến zone1 lên zone4 đang bị tắc?</i>",
        )
        return JSONResponse(content={"ok": True})

    await _send_typing(chat_id)

    try:
        snapshot = await fetch_system_snapshot(conn)
        lang = _detect_lang(text)
        user_prompt = build_user_prompt(snapshot, text, lang)
        system = _build_telegram_system()
        reply = await ask_llm(system, user_prompt, temperature=0.4, num_predict=350)
    except Exception as exc:
        logger.error("telegram: handler error — %s", exc)
        reply = "Có lỗi xảy ra khi xử lý câu hỏi của bạn."

    await _send_message(chat_id, reply)
    logger.info("telegram: chat_id=%d msg_len=%d replied", chat_id, len(text))
    return JSONResponse(content={"ok": True})


@router.get("/set-webhook")
async def set_webhook(url: str) -> dict[str, Any]:
    if not _TELEGRAM_BOT_TOKEN:
        return {"error": "TELEGRAM_BOT_TOKEN not configured"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{_TELEGRAM_API}/setWebhook",
            json={"url": f"{url}/telegram/webhook"},
        )
        return resp.json()  # type: ignore[no-any-return]


@router.get("/info")
async def bot_info() -> dict[str, Any]:
    if not _TELEGRAM_BOT_TOKEN:
        return {"error": "TELEGRAM_BOT_TOKEN not configured"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{_TELEGRAM_API}/getMe")
        return resp.json()  # type: ignore[no-any-return]
