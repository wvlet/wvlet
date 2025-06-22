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
Wvlet Python SDK
================

The Wvlet Python SDK provides a native Python interface for compiling Wvlet queries 
to SQL. It offers high-performance compilation through a bundled native library 
while maintaining a simple, Pythonic API.

Features
--------
- Fast native compilation using bundled library
- Support for multiple SQL targets (DuckDB, Trino, etc.)
- Zero dependencies
- Cross-platform support (Linux x86_64/ARM64, macOS ARM64)

Quick Start
-----------
    >>> from wvlet import compile
    >>> sql = compile("from users select name, age where age > 18")
    >>> print(sql)
    SELECT name, age FROM users WHERE age > 18

    >>> # Using models (CTEs)
    >>> sql = compile('''
    ... model ActiveUsers = { from users where active = true }
    ... from ActiveUsers select count(*) as active_count
    ... ''')

Advanced Usage
--------------
For more control, use the WvletCompiler class directly:

    >>> from wvlet.compiler import WvletCompiler
    >>> compiler = WvletCompiler(target="trino")
    >>> sql = compiler.compile("from orders select count(*)")

Integration
-----------
The compiled SQL can be used with any SQL execution framework:

    >>> import duckdb
    >>> sql = compile("from 'data.parquet' select * limit 10", target="duckdb")
    >>> duckdb.sql(sql).show()

For more examples and documentation, visit: https://wvlet.org/docs/bindings/python/
"""

from .compiler import WvletCompiler, CompilationError

__version__ = "0.1.0"
__all__ = ["compile", "WvletCompiler", "CompilationError"]


def compile(query: str, target: str = None) -> str:
    """
    Compile a Wvlet query to SQL.
    
    This is the primary interface for compiling Wvlet queries. It uses the
    native library for high-performance compilation.
    
    Parameters
    ----------
    query : str
        The Wvlet query string to compile. Supports the full Wvlet syntax
        including models, joins, aggregations, window functions, etc.
    target : str, optional
        Target SQL dialect. Supported values:
        - "duckdb": DuckDB SQL dialect
        - "trino": Trino/Presto SQL dialect
        - None: Use default SQL dialect
    
    Returns
    -------
    str
        The compiled SQL query string.
    
    Raises
    ------
    CompilationError
        If the query has syntax errors or cannot be compiled.
    NotImplementedError
        If the native library is not available for the current platform.
    
    Examples
    --------
    Basic query compilation:
    
    >>> sql = compile("from employees select name, salary where salary > 50000")
    >>> print(sql)
    SELECT name, salary FROM employees WHERE salary > 50000
    
    Using models (CTEs):
    
    >>> sql = compile('''
    ... model HighEarners = {
    ...     from employees 
    ...     where salary > 100000
    ... }
    ... from HighEarners 
    ... select department, count(*) as count 
    ... group by department
    ... ''')
    
    Target-specific compilation:
    
    >>> sql = compile("from logs select count(*)", target="trino")
    
    See Also
    --------
    WvletCompiler : The compiler class for more advanced usage.
    """
    compiler = WvletCompiler(target=target)
    return compiler.compile(query)
