import logging
import os
import openai
from app.config import Config, load_config
from app.utilities.custom_filters import format_images, remove_none_str, round_number

logging.basicConfig(level=logging.INFO, filename='app.log',
                    filemode='a', format='%(name)s - %(levelname)s - %(message)s')


openai.api_key = os.getenv('OPENAI_API_KEY')

def create_app(config_class=None):
    from flask import Flask

    config_class = config_class or load_config.get(Config.FLASK_ENV or "development", load_config["development"])
    app = Flask(__name__)
    app.secret_key = Config.SECRET_KEY
    # Load Config from environment variables
    app.config.from_object(config_class)
    print(f"Active config: {config_class.__name__}")


    # Add Custom template tags
    app.jinja_env.filters['format_images'] = format_images
    app.jinja_env.filters['remove_none_str'] = remove_none_str
    app.jinja_env.filters['round_number'] = round_number

    # Register Blueprints
    from app.main.routes import main_bp
    from app.pam.routes import pam_bp
    from app.users.routes import user_bp
    app.register_blueprint(main_bp)
    app.register_blueprint(pam_bp)
    app.register_blueprint(user_bp)

    return app
