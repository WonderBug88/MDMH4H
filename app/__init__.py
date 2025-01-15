import logging
import os
import openai
import atexit
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from app.config import Config, load_config
from etl_scripts.gsc.gsc_weekly_update import gsc_weekly_update_main
from etl_scripts.ganeshmills.deskgmail_latest import start_latest_process
from etl_scripts.ganeshmills.bigapi import big_api_main 
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
    from app.pam.routes import pam_bp
    from app.users.routes import user_bp
    app.register_blueprint(main_bp)
    app.register_blueprint(pam_bp)
    app.register_blueprint(user_bp)

    # Configure APScheduler
    scheduler = BackgroundScheduler()

    # Define a job to run the GSC update function weekly
    scheduler.add_job(func=gsc_weekly_update_main, trigger='cron',
                      day_of_week='sun', hour=0, minute=0)  # Runs every Sunday at midnight

    # Schedule the job to run every minute for testing
    # scheduler.add_job(func=gsc_weekly_update_main, trigger='interval', minutes=1)

    # Schedule a job to run the process for downloading the latest Gmail attachment.
    # This job will execute every Sunday at 11:00 PM.
    scheduler.add_job(func=start_latest_process, trigger='cron', day_of_week='sun', hour=23, minute=0)

    # Schedule a job to run the BigCommerce API function for inventory updates.
    # This job will execute every Sunday at 11:30 PM.
    scheduler.add_job(func=big_api_main, trigger='cron', day_of_week='sun', hour=23, minute=30)

    scheduler.start()

    # Ensure that the scheduler shuts down when the app exits
    atexit.register(lambda: scheduler.shutdown(wait=False))

    return app
