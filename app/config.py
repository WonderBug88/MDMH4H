import os
from os import environ

from dotenv import load_dotenv

load_dotenv()

# Get the absolute path of the parent directory of the directory containing the current file
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

class Config:
    """Base configuration."""

    FLASK_ENV = environ.get("FLASK_ENV")
    FLASK_APP = environ.get("FLASK_APP")
    SECRET_KEY = environ.get("SECRET_KEY")
    TESTING = environ.get("TESTING")
    FLASK_DEBUG = environ.get("FLASK_DEBUG")
    STATIC_FOLDER = "static"
    TEMPLATES_FOLDER = "templates"
    SITEMAP_INCLUDE_RULES_WITHOUT_PARAMS = True
    SESSION_PERMANENT = True
    ALLOWED_EXTENSIONS = {"pdf", "doc", "docx", "xls", "xlsx"}
    UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
    ETL_SCRIPTS_PATH = os.path.join(BASE_DIR, "etl_scripts")


class DevelopmentConfig(Config):
    """Development configuration."""

    # SQLALCHEMY_DATABASE_URI = environ.get("DEV_DATABASE_URL")
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(BASE_DIR, "db.sqlite")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class StagingConfig(Config):
    """Staging configuration."""

    SQLALCHEMY_DATABASE_URI = environ.get("STAGING_DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    """Production configuration."""

    SQLALCHEMY_DATABASE_URI = environ.get("PRODUCTION_DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class TestConfig(Config):
    """Test configuration."""

    TESTING = True
    # Set test DB diffrent from main db
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(
        BASE_DIR, "../tests", "test_db.sqlite"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    IMAGE_SAVE_DIRECTORY = os.path.join(
        BASE_DIR, "../tests" "/images"
    )  # Test Images folder


# Load Config from environment variables
load_config = {
    "development": DevelopmentConfig,
    "staging": StagingConfig,
    "production": ProductionConfig,
    "testing": TestConfig,
    "default": DevelopmentConfig,
}
