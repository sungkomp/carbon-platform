from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://carbon:carbon@localhost:5432/carbon"
    cors_origins: str = "http://localhost:5173"
    jwt_secret: str = "CHANGE_ME_TO_A_LONG_RANDOM_SECRET"

settings = Settings()
