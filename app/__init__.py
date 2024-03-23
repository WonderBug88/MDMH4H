import logging
import os
import openai
from flask import Flask
from app.config import Config, load_config

logging.basicConfig(level=logging.INFO, filename='app.log',
                    filemode='a', format='%(name)s - %(levelname)s - %(message)s')


openai.api_key = os.getenv('OPENAI_API_KEY')


def create_app(config_class=load_config[Config.FLASK_ENV]):
    app = Flask(__name__)
    app.secret_key = Config.SECRET_KEY
    # Load Config from environment variables
    app.config.from_object(config_class)

    # Register Blueprints
    from app.main.routes import main_bp
    app.register_blueprint(main_bp)

    return app
