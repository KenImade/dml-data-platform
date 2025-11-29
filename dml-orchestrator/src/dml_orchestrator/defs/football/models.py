from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Literal, Optional


class Player(BaseModel):
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


class TeamV2(Team):
    pass


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
    selected_by_percent: float
    selected_rank: int
    selected_rank_type: int
    total_points: int
    event_points: int
    points_per_game: float
    points_per_game_rank: int
    points_per_game_rank_type: int
    bonus: int
    bps: int
    form: float
    form_rank: int
    form_rank_type: int
    value_form: float
    value_season: float
    dreamteam_count: int
    transfers_in: int
    transfers_in_event: int
    transfers_out: int
    transfers_out_event: int
    ep_next: float
    ep_this: float
    expected_goals: float
    expected_assists: float
    expected_goal_involvements: float
    expected_goals_conceded: float
    expected_goals_per_90: float
    expected_assists_per_90: float
    expected_goal_involvements_per_90: float
    expected_goals_conceded_per_90: float
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


class PlayerMatchStats(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    player_id: int
    match_id: str

    minutes_played: int
    goals: int
    assists: int

    total_shots: int
    xg: float
    xa: float
    xgot: float
    shots_on_target: int
    successful_dribbles: int
    big_chances_missed: int
    touches_opposition_box: int
    touches: int
    accurate_passes: int
    chances_created: int
    final_third_passes: int
    accurate_crosses: int
    accurate_long_balls: int
    tackles_won: int
    interceptions: int
    recoveries: int
    blocks: int
    clearances: int
    headed_clearances: int
    dribbled_past: int
    duels_won: int
    duels_lost: int
    ground_duels_won: int
    aerial_duels_won: int
    was_fouled: int
    fouls_committed: int
    saves: int
    goals_conceded: int
    xgot_faced: int
    goals_prevented: int
    sweeper_actions: int
    gk_accurate_passes: int
    gk_accurate_long_balls: int
    offsides: int
    high_claim: int
    tackles: int
    accurate_passes_percent: int
    accurate_crosses_percent: int
    accurate_long_balls_percent: int
    ground_duels_won_percent: int
    aerial_duels_won_percent: int
    successful_dribbles_percent: int
    tackles_won_percent: int
    start_min: int
    finish_min: int
    team_goals_conceded: int
    penalties_scored: int
    penalties_missed: int


class Matches(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    gameweek: int
    kickoff_time: str
    home_team: int
    home_team_elo: float
    home_score: int
    away_score: int
    away_team: int
    away_team_elo: float
    finished: bool
    match_id: str
    match_url: str
    home_possession: int
    away_possession: int
    home_expected_goals_xg: float
    away_expected_goals_xg: float
    home_total_shots: int
    away_total_shots: int
    home_shots_on_target: int
    away_shots_on_target: int
    home_big_chances: int
    away_big_chances: int
    home_big_chances_missed: int
    away_big_chances_missed: int
    home_accurate_passes: int
    away_accurate_passes: int
    home_accurate_passes_pct: int
    away_accurate_passes_pct: int
    home_fouls_committed: int
    away_fouls_committed: int
    home_corners: int
    away_corners: int
    home_xg_open_play: float
    away_xg_open_play: float
    home_xg_set_play: float
    away_xg_set_play: float
    home_non_penalty_xg: float
    away_non_penalty_xg: float
    home_xg_on_target_xgot: float
    away_xg_on_target_xgot: float
    home_shots_off_target: int
    away_shots_off_target: int
    home_blocked_shots: int
    away_blocked_shots: int
    home_hit_woodwork: int
    away_hit_woodwork: int
    home_shots_inside_box: int
    away_shots_inside_box: int
    home_shots_outside_box: int
    away_shots_outside_box: int
    home_passes: int
    away_passes: int
    home_own_half: int
    away_own_half: int
    home_opposition_half: int
    away_opposition_half: int
    home_accurate_long_balls: int
    away_accurate_long_balls: int
    home_accurate_long_balls_pct: int
    away_accurate_long_balls_pct: int
    home_accurate_crosses: int
    away_accurate_crosses: int
    home_accurate_crosses_pct: int
    away_accurate_crosses_pct: int
    home_throws: int
    away_throws: int
    home_touches_in_opposition_box: int
    away_touches_in_opposition_box: int
    home_offsides: int
    away_offsides: int
    home_yellow_cards: int
    away_yellow_cards: int
    home_red_cards: int
    away_red_cards: int
    home_tackles_won: int
    away_tackles_won: int
    home_tackles_won_pct: int
    away_tackles_won_pct: int
    home_interceptions: int
    away_interceptions: int
    home_blocks: int
    away_blocks: int
    home_clearances: int
    away_clearances: int
    home_keeper_saves: int
    away_keeper_saves: int
    home_duels_won: int
    away_duels_won: int
    home_ground_duels_won: int
    away_ground_duels_won: int
    home_ground_duels_won_pct: int
    away_ground_duels_won_pct: int
    home_aerial_duels_won: int
    away_aerial_duels_won: int
    home_aerial_duels_won_pct: int
    away_aerial_duels_won_pct: int
    home_successful_dribbles: int
    away_successful_dribbles: int
    home_successful_dribbles_pct: int
    away_successful_dribbles_pct: int
    fotmob_id: int
    stats_processed: bool
    player_stats_processed: bool
