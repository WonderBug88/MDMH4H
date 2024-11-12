import logging
import os
import openai
import atexit
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from app.config import Config, load_config
from etl_scripts.gsc.gsc_weekly_update import gsc_weekly_update_main
from app.utilities.custom_filters import format_images, remove_none_str, round_number

logging.basicConfig(level=logging.INFO, filename='app.log',
                    filemode='a', format='%(name)s - %(levelname)s - %(message)s')


openai.api_key = os.getenv('OPENAI_API_KEY')


def create_app(config_class=load_config[Config.FLASK_ENV]):
    app = Flask(__name__)
    app.secret_key = Config.SECRET_KEY
    # Load Config from environment variables
    app.config.from_object(config_class)

    # Add Custom template tags
    app.jinja_env.filters['format_images'] = format_images
    app.jinja_env.filters['remove_none_str'] = remove_none_str
    app.jinja_env.filters['round_number'] = round_number

    # Register Blueprints
    from app.main.routes import main_bp
    app.register_blueprint(main_bp)

    # Configure APScheduler
    scheduler = BackgroundScheduler()

    # Define a job to run the GSC update function weekly
    scheduler.add_job(func=gsc_weekly_update_main, trigger='cron',
                      day_of_week='sun', hour=0, minute=0)  # Runs every Sunday at midnight

    # Schedule the job to run every minute for testing
    # scheduler.add_job(func=gsc_weekly_update_main, trigger='interval', minutes=1)

    scheduler.start()

    # Ensure that the scheduler shuts down when the app exits
    atexit.register(lambda: scheduler.shutdown(wait=False))

    return app
