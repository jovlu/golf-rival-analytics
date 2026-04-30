from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
)


class StrictEventModel(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)


class RegistrationEventData(StrictEventModel):
    country: StrictStr
    username: StrictStr
    device_os: Literal["Android", "iOS"]


class RegistrationEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["registration"]
    user_id: StrictStr
    event_data: RegistrationEventData


class SessionPingEventData(StrictEventModel):
    state: Literal["started", "in_progress", "ended"]
    device_os: StrictStr


class SessionPingEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["session_ping"]
    user_id: StrictStr
    event_data: SessionPingEventData


class MatchStartEventData(StrictEventModel):
    map_id: StrictStr
    opponent_id: StrictStr


class MatchStartEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["match_start"]
    user_id: StrictStr
    event_data: MatchStartEventData


class MatchFinishEventData(MatchStartEventData):
    outcome: StrictInt | StrictFloat

    @field_validator("outcome")
    @classmethod
    def validate_outcome(cls, outcome):
        if outcome not in {0, 0.5, 1}:
            raise ValueError("outcome must be 0, 0.5, or 1")

        return outcome


class MatchFinishEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["match_finish"]
    user_id: StrictStr
    event_data: MatchFinishEventData


MATCH_EVENT_MODELS = {
    "match_start": MatchStartEvent,
    "match_finish": MatchFinishEvent,
}


def parse_event(row, event_model):
    try:
        return event_model.model_validate(row)
    except ValidationError:
        return None
