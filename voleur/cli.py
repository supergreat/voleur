import sys
from typing import Optional


class Env:
    """Simple class providing tools for a CLI environment."""

    RED = '31'
    GREEN = '32'
    BLUE = '34'

    def __init__(self, arguments: dict):
        self._arguments = arguments

    def get_arg(self, name: str, default: str = None):
        return self._arguments.get(name, default)

    def info(self, msg: str):
        print(self._msg(msg, color=self.BLUE))

    def ok(self, msg: str):
        print(self._msg(msg, color=self.GREEN))

    def die(self, msg: str):
        print(self._msg(msg, color=self.RED))
        sys.exit(1)

    def _msg(self, msg: str, color: Optional[str] = None):
        return f'\033[{color}m{msg}\033[0m' if color else msg
