from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Literal


class Player(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    player_code: int = Field(gt=0, description="FPL unique player code")
    player_id: int = Field(gt=0, description="Unique player id")
    first_name: str
    second_name: str
    web_name: str
    team_code: int = Field(gt=0)
    position: Literal["Midfielder", "Unknown", "Goalkeeper", "Defender", "Forward"]

    @field_validator("first_name", "second_name", "web_name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Name fields cannot be empty")
        return v.strip()
