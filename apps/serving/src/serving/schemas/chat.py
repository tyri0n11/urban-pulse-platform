from typing import Literal

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(..., min_length=1, max_length=2000)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    lang: str = Field(default="vi", pattern="^(vi|en)$")
    history: list[ChatMessage] = Field(default_factory=list, max_length=20)
