from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class EventCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    seat_count: int = Field(ge=1, le=100000)


class EventOut(BaseModel):
    id: str
    name: str
    seat_count: int


class SeatOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    event_id: str
    number: int
    status: str
    hold_id: str | None
    hold_expires_at: datetime | None


class WebhookIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=255)
    hold_id: str = Field(min_length=1, max_length=36)
    status: Literal["succeeded", "failed"]


class WebhookOut(BaseModel):
    accepted: bool
    duplicate: bool = False
    queued: bool = False
