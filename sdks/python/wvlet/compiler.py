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

"""
Wvlet Compiler Module
====================

This module provides the core compiler functionality for the Wvlet Python SDK.
It handles native library loading and query compilation.
"""

from typing import Optional
import ctypes
import platform
import os
import json


class CompilationError(Exception):
    """
    Exception raised when a Wvlet query cannot be compiled.
    
    Attributes
    ----------
    message : str
        The error message describing what went wrong.
    line : int, optional
        The line number where the error occurred (if available).
    column : int, optional
        The column number where the error occurred (if available).
    """
    def __init__(self, message: str, line: int = None, column: int = None):
        self.message = message
        self.line = line
        self.column = column
        super().__init__(message)


def _load_native_library():
    """
    Load the native wvlet library for the current platform.
    
    This function attempts to load the platform-specific native library
    from the bundled libs directory. It automatically detects the current
    platform and architecture to load the appropriate library.
    
    Returns
    -------
    ctypes.CDLL or None
        The loaded native library, or None if not available for the platform.
    
    Notes
    -----
    Supported platforms:
    - Linux x86_64: libwvlet.so
    - Linux ARM64: libwvlet.so  
    - macOS ARM64: libwvlet.dylib
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


class WvletCompiler:
    """
    The main compiler class for Wvlet queries.
    
    This class provides the interface for compiling Wvlet queries into SQL
    using the native library for high performance.
    
    Attributes
    ----------
    target : str, optional
        The default target SQL dialect for compilation.
    
    Examples
    --------
    Create a compiler with default settings:
    
    >>> compiler = WvletCompiler()
    >>> sql = compiler.compile("from users select *")
    
    Create a compiler for a specific target:
    
    >>> trino_compiler = WvletCompiler(target="trino")
    >>> sql = trino_compiler.compile("from logs select count(*)")
    """

    def __init__(self, 
                 target: Optional[str] = None,
                 wvlet_home: Optional[str] = None):
        """
        Initialize the WvletCompiler.

        Parameters
        ----------
        target : str, optional
            Default target SQL dialect for compilation. Valid values:
            - "duckdb": DuckDB SQL dialect
            - "trino": Trino/Presto SQL dialect
            - None: Use default SQL dialect
        wvlet_home : str, optional
            Path to Wvlet home directory. Defaults to ~/.wvlet.
            This parameter is reserved for future functionality such as:
            - Caching compiled queries for performance
            - Storing user-defined catalogs and configurations
            - Managing local schema information
            Currently not used by the Python SDK.
        
        Raises
        ------
        NotImplementedError
            If the native library is not available for the current platform.
        """
        self.target = target
        self.wvlet_home = wvlet_home or os.path.expanduser("~/.wvlet")
        
        # Check if native library is available
        if _native_lib is None:
            raise NotImplementedError("The native library is not available for the current platform")

    def compile(self, query: str, target: Optional[str] = None) -> str:
        """
        Compile a Wvlet query to SQL.

        Parameters
        ----------
        query : str
            The Wvlet query string to compile. Supports the full Wvlet syntax
            including models, joins, aggregations, window functions, etc.
        target : str, optional
            Override the default target SQL dialect for this compilation.
            If not specified, uses the target set during initialization.

        Returns
        -------
        str
            The compiled SQL query string.

        Raises
        ------
        CompilationError
            If the query has syntax errors or cannot be compiled.
        ValueError
            If the compilation process fails for other reasons.
            
        Examples
        --------
        Basic compilation:
        
        >>> compiler = WvletCompiler()
        >>> sql = compiler.compile("from users select name, email")
        >>> print(sql)
        SELECT name, email FROM users
        
        Override target for specific query:
        
        >>> compiler = WvletCompiler(target="duckdb")
        >>> sql = compiler.compile("from logs select *", target="trino")
        
        Notes
        -----
        The compilation is stateless - each call to compile() is independent.
        """
        # Use provided target or fall back to instance default
        compilation_target = target or self.target
        
        # Use native library
        args = ["-q", query]
        if compilation_target:
            args.extend(["--target", compilation_target])
        
        # Call native function
        args_json = json.dumps(args).encode('utf-8')
        result = _native_lib.wvlet_compile_query(args_json)
        
        if not result:
            raise CompilationError("Failed to compile query")
        
        return result.decode('utf-8')
