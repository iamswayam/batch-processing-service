from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    # Database
    database_url: str
    test_database_url: str | None = None

    # Redis
    redis_url: str

    # Vendor
    vendor_url: str

    # Worker
    worker_poll_interval: int = 2
    worker_chunk_size: int = 10
    worker_lease_timeout: int = 60
    max_concurrent_requests: int = 20
    max_retry_count: int = 5

    # Rate limiter defaults (per tenant)
    default_rate_limit_capacity: int = 10
    default_rate_limit_refill_rate: int = 5
    tenant_rate_limits: dict[str, dict[str, float]] = {}


settings = Settings()
