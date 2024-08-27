"""Functions to use as Template Tags in html file"""

from jinja2 import pass_context

@pass_context
def format_images(context, value):
    """Convert a string representation of a list into an actual list."""
    if isinstance(value, str):
        # Remove leading and trailing brackets
        cleaned_value = value.strip().strip('[]').replace('"', "")
        if cleaned_value:
            try:
                # Split by comma and strip extra whitespace from each URL
                urls = [url.strip() for url in cleaned_value.split(',')]
                return urls
            except (ValueError, SyntaxError):
                # Handle cases where the string cannot be parsed
                pass 
    
    return []

@pass_context
def remove_none_str(_, value):
    """Show - instead of str None"""

    if not value or value == "None":
        return "-"
    return value
    