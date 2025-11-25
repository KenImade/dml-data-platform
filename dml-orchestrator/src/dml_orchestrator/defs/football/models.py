from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Literal, Optional


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


class Team(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    code: int = Field(gt=0, description="The team's unique code in the FPL API")
    id: int = Field(
        gt=0, description="A unique identifier for each team within this dataset"
    )
    name: str
    short_name: str
    strength: int
    strength_overall_home: int
    strength_overall_away: int
    strength_attack_home: int
    strength_attack_away: int
    strength_defence_home: int
    strength_defence_away: int
    pulse_id: int
    elo: int


class PlayerStats(BaseModel):
    id: int
    status: Literal["u", "a", "d", "i", "s", "n"]

    chance_of_playing_next_round: Optional[float]
    chance_of_playing_this_round: Optional[float]

    now_cost: float
    now_cost_rank: int
    now_cost_rank_type: int
    cost_change_event: int
    cost_change_event_fall: int
    cost_change_start: int
    cost_change_start_fall: int

    form: float
    form_rank: int
    form_rank_type: int

    points_per_game: float
    selected_by_percent: float
    selected_rank: int
    selected_rank_type: int

    influence: float
    influence_rank: int
    influence_rank_type: int

    creativity: float
    creativity_rank: int
    creativity_rank_type: int

    threat: float
    threat_rank: int
    threat_rank_type: int

    ict_index: float
    ict_index_rank: int
    ict_index_rank_type: int

    corners_and_indirect_freekicks_order: Optional[float]
    direct_freekicks_order: Optional[float]
    penalties_order: Optional[float]

    gw: int
    set_piece_threat: Optional[float]
