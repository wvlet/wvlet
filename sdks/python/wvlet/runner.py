#
# Copyright 2024 wvlet.org
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
from typing import Optional
import shutil
import subprocess


class WvletCompiler():
    """WvletCompiler is for compiling Wvlet queries into SQL queries.

    This class assumes that wvlet is installed and in PATH or that executable_path is specified.
    """

    def __init__(self, executable_path: Optional[str] = None, target: Optional[str] = None):
        """
        Initializes the WvletCompiler with the specified executable path and target.

        Args:
            executable_path (Optional[str]): The path to the wvlet executable. If not provided, it will look for 'wvlet' in the system PATH.
            target (Optional[str]): The target database for the compiled SQL queries.
        
        Raises:
            ValueError: If the provided executable_path is invalid.
            NotImplementedError: If the wvlet executable is not found in the system PATH.
        """
        if executable_path:
            if shutil.which(executable_path) is None:
                raise ValueError(f"Invalid executable_path: {executable_path}")
            self.path = executable_path
            return
        # To make self.path non-optional type, first declare optional path
        path = shutil.which("wvlet")
        if path is None:
            raise NotImplementedError("This binding currently requires wvlet executable")
        self.path = path
        self.target = target

    def compile(self, query: str) -> str:
        """
        Compiles the given Wvlet query into an SQL query.

        Args:
            query (str): The Wvlet query to be compiled.

        Returns:
            str: The compiled SQL query.

        Raises:
            ValueError: If the compilation process fails.
        """
        command = [self.path, "compile"]
        if self.target:
            command.append(f"--target:{self.target}")
        command.append(query)
        process = subprocess.run(command, capture_output=True, text=True)
        print(process.stdout, end="")
        print(process.stderr, file=sys.stderr, end="")
        if process.returncode != 0:
            raise ValueError("Failed to compile")
        return "\n".join(process.stdout.split("\n")[1:])
