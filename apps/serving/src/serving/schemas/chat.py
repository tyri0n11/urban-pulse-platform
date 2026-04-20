from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    lang: str = Field(default="vi", pattern="^(vi|en)$")
