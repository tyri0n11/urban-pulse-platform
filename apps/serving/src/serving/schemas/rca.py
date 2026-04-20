from pydantic import BaseModel, Field


class RCARequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    route_id: str | None = Field(default=None)
    lang: str = Field(default="vi", pattern="^(vi|en)$")


class FeedbackRequest(BaseModel):
    log_id: int
    feedback: int = Field(..., ge=-1, le=1)
