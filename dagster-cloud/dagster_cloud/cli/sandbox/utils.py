from datetime import datetime


def get_current_display_timestamp():
    return datetime.now().strftime("%H:%M:%S")
