import sys
from typing import Optional
import shutil
import subprocess


class WvletRunner():
    def __init__(self, executable_path: Optional[str] = None):
        if executable_path:
            if shutil.which(executable_path) is None:
                raise ValueError(f"Invalid executable_path: {executable_path}")
            self.path = executable_path
            return
        # To make self.path non-optional type, first declare optional path
        path = shutil.which("wvc")
        if path is None:
            raise NotImplementedError("This binding currently requires wvc executable")
        self.path = path


    def compile(self, query: str) -> str:
        process = subprocess.run([self.path, "-c", f'"{query}"'], capture_output=True, text=True)
        print(process.stdout)
        print(process.stderr, file=sys.stderr)
        if process.returncode != 0:
            raise ValueError("Failed to compile")
        return process.stdout
