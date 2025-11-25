from typing import Literal, Dict
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

    @computed_field
    @property
    def data_files(self) -> Dict[str, str]:
        "Dynamically compute file paths based on season and gameweek."
        return {
            "matches": f"/data/{self.season}/matches/{self.gameweek}/matches.csv",
            "playermatchstats": f"/data/{self.season}/playermatchstats/{self.gameweek}/playermatchstats.csv",
            "players": f"/data/{self.season}/players/players.csv",
            "playerstats": f"/data/{self.season}/playerstats/playerstats.csv",
            "teams": f"/data/{self.season}/teams/teams.csv",
        }


class MinIOSettings(BaseSettings):
    """MinIO object storage configuration"""

    endpoint: str = Field(
        default="http://localhost:9000",
        validation_alias="DAGSTER_MINIO_SERVER",
        description="MinIO endpoint",
    )

    access_key: str = Field(
        default="minio",
        validation_alias="MINIO_ROOT_USER",
        description="MinIO access key",
    )

    secret_key: str = Field(
        default="minio",
        validation_alias="MINIO_ROOT_PASSWORD",
        description="MinIO secret key",
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
