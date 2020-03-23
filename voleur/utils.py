import uuid
from datetime import datetime


def generate_dump_filename() -> str:
    """Generates a unique dump filename.

    Returns:
        str: The filename.

    """
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    return f'{uuid.uuid4().hex}_{timestamp}.dump'
