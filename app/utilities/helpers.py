"""Utility functions for the application."""
from app.config import Config

def allowed_file(filename):
    """Check if the file extension is allowed."""
    print("989089980", Config.ALLOWED_EXTENSIONS)
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS
