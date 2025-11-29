from typing import Literal, Dict, ClassVar, Set
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GitHubSettings(BaseSettings):
    """GitHub source configuration"""

    ENVIRONMENT: Literal["development", "production"] = "production"
    base_url: str = Field(
        default="https://raw.githubusercontent.com/olbauday/FPL-Elo-Insights/refs/heads/main",
        description="Base url for Football data.",
    )
    season: str = Field(default="2024-2025", description="Football season")
    gameweek: str = Field(default="GW1", description="gameweek")
    retry_attempts: int = Field(default=3, description="Number of retry attempts.")

    OlD_SEASON_SINGLE_FILE: ClassVar[Set[str]] = {"teams", "players", "playerstats"}
    OLD_SEASONS: ClassVar[Set[str]] = {"2024-2025"}

    @computed_field
    @property
    def normalized_gw(self) -> str:
        gw = str(self.gameweek)
        if gw.upper().startswith("GW"):
            return gw.upper()
        return f"GW{gw}"

    def build_gw_path(self, dataset: str, gw_override: str | None = None) -> str:
        season = str(self.season)
        gw = gw_override or self.normalized_gw

        if season in self.OLD_SEASONS:
            if dataset in self.OlD_SEASON_SINGLE_FILE:
                return f"/data/{season}/{dataset}/{dataset}.csv"
            return f"/data/{season}/{dataset}/{gw}/{dataset}.csv"

        return f"/data/{season}/By Gameweek/{gw}/{dataset}.csv"

    def build_gw_url(self, dataset: str, gw_override: str | None = None) -> str:
        return f"{self.base_url}{self.build_gw_path(dataset, gw_override)}"

    @computed_field
    @property
    def gw_data_files(self) -> Dict[str, str]:
        return {
            "matches": self.build_gw_url("matches"),
            "playermatchstats": self.build_gw_url("playermatchstats"),
            "players": self.build_gw_url("players"),
            "playerstats": self.build_gw_url("playerstats"),
            "teams": self.build_gw_url("teams"),
        }


class MinIOSettings(BaseSettings):
    """MinIO object storage configuration"""

    endpoint: str = Field(
        default="http://localhost:9000",
        validation_alias="DAGSTER_MINIO_SERVER",
    )

    access_key: str = Field(
        default="minio",
        validation_alias="MINIO_ROOT_USER",
    )

    secret_key: str = Field(
        default="minio",
        validation_alias="MINIO_ROOT_PASSWORD",
    )

    main_bucket: str = Field(default="dml-dev", description="Main bucket for data")

    secure: bool = Field(default=False, description="Use HTTPS connection")
    region: str = Field(default="us-east-1", description="MinIO region")

    raw_data_path: str = Field(
        default="raw/football", description="Raw data storage path"
    )
    staging_data_path: str = Field(
        default="staging/football", description="Staging data storage path"
    )
    archive_data_path: str = Field(
        default="archived/football", description="Archived data storage path"
    )

    @computed_field
    @property
    def endpoint_url(self) -> str:
        """Get full MinIO endpoint url based on security"""
        url = self.endpoint
        if self.secure:
            url = url.replace("http", "https")
        return url

    @staticmethod
    def normalize_season(season: str) -> str:
        return str(season)

    @staticmethod
    def normalize_gw(gw: str) -> str:
        gw = str(gw)
        if gw.upper().startswith("GW"):
            return gw.upper()
        return f"GW{gw}"

    def build_raw_gw_key(self, dataset: str, season: str, gw: str) -> str:
        season = self.normalize_season(season)
        gw = self.normalize_gw(gw)
        return f"{self.raw_data_path}/{season}/{gw}/{dataset}"

    def build_staging_gw_key(self, dataset: str, season: str, gw: str) -> str:
        season = self.normalize_season(season)
        gw = self.normalize_gw(gw)
        return f"{self.staging_data_path}/{season}/{gw}/{dataset}"

    def build_archive_gw_key(
        self,
        dataset: str,
        season: str,
        gw: str,
    ) -> str:
        season = self.normalize_season(season)
        gw = self.normalize_gw(gw)
        return f"{self.archive_data_path}/{season}/{gw}/{dataset}"


class PipelineSettings(BaseSettings):
    """General pipeline configuration"""

    environment: str = Field(default="development", description="Environment name")
    debug: bool = Field(default=False, description="debug mode")
    log_level: str = Field(default="DEBUG", description="Logging level")

    max_workers: int = Field(default=4, description="Max parallel workers")

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


class Settings(BaseSettings):
    """Master settings combining all configuration settings"""

    github: GitHubSettings = Field(default_factory=GitHubSettings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)


settings = Settings()

github_config = settings.github
minio_config = settings.minio
pipeline_config = settings.pipeline
