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

from typing import Optional
import shutil
import subprocess
import ctypes
import platform
import os
import json


def _load_native_library():
    """
    Load the native wvlet library for the current platform.
    
    Returns:
        ctypes.CDLL: The loaded native library, or None if not available.
    """
    system = platform.system()
    machine = platform.machine()
    
    # Map platform to library path
    lib_map = {
        ('Linux', 'x86_64'): 'linux_x86_64/libwvlet.so',
        ('Linux', 'aarch64'): 'linux_aarch64/libwvlet.so',
        ('Darwin', 'arm64'): 'darwin_arm64/libwvlet.dylib',
    }
    
    key = (system, machine)
    if key not in lib_map:
        return None
    
    # Get the library path relative to this file
    lib_dir = os.path.dirname(os.path.abspath(__file__))
    lib_path = os.path.join(lib_dir, 'libs', lib_map[key])
    
    if not os.path.exists(lib_path):
        return None
    
    try:
        lib = ctypes.CDLL(lib_path)
        # Set the return type for wvlet_compile_query
        lib.wvlet_compile_query.restype = ctypes.c_char_p
        lib.wvlet_compile_query.argtypes = [ctypes.c_char_p]
        return lib
    except Exception:
        return None


# Try to load the native library on module import
_native_lib = _load_native_library()


class WvletCompiler():
    """WvletCompiler is for compiling Wvlet queries into SQL queries.

    This class can use either the native library (if available) or fall back to
    the wvlet executable in PATH.
    """

    def __init__(self, executable_path: Optional[str] = None, target: Optional[str] = None):
        """
        Initializes the WvletCompiler with the specified executable path and target.

        Args:
            executable_path (Optional[str]): The path to the wvlet executable. If not provided, 
                                            it will use the native library if available, or look 
                                            for 'wvlet' in the system PATH.
            target (Optional[str]): The target database for the compiled SQL queries.
        
        Raises:
            ValueError: If the provided executable_path is invalid.
            NotImplementedError: If neither the native library nor the wvlet executable is available.
        """
        self.target = target
        self.use_native = False
        self.path = None
        
        if executable_path:
            if shutil.which(executable_path) is None:
                raise ValueError(f"Invalid executable_path: {executable_path}")
            self.path = executable_path
            return
            
        # Try to use native library first
        if _native_lib is not None:
            self.use_native = True
            return
            
        # Fall back to wvlet executable
        path = shutil.which("wvlet")
        if path is None:
            raise NotImplementedError("This binding requires either the native library or wvlet executable")
        self.path = path

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
        if self.use_native:
            # Use native library
            args = ["-q", query]
            if self.target:
                args.extend(["--target", self.target])
            
            # Call native function
            args_json = json.dumps(args).encode('utf-8')
            result = _native_lib.wvlet_compile_query(args_json)
            
            if not result:
                raise ValueError("Failed to compile query")
            
            return result.decode('utf-8')
        else:
            # Use subprocess (existing implementation)
            command = [self.path, "compile"]
            if self.target:
                command.append(f"--target:{self.target}")
            command.append(query)
            process = subprocess.run(command, capture_output=True, text=True)
            
            if process.returncode != 0:
                error_msg = "Failed to compile"
                if process.stderr:
                    error_msg += f": {process.stderr.strip()}"
                raise ValueError(error_msg)
            
            # Return the SQL output (skip the first line which contains query echo)
            lines = process.stdout.strip().split("\n")
            if len(lines) > 1:
                return "\n".join(lines[1:])
            return process.stdout
