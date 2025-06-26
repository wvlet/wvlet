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

import pytest
from wvlet.compiler import WvletCompiler, CompilationError
from wvlet import compile as wvlet_compile

def test_wvlet_initialization():
    """Test that WvletCompiler can be initialized with native library"""
    try:
        compiler = WvletCompiler()
        # If we get here, native library is available
        assert compiler is not None
    except NotImplementedError:
        pytest.skip("Native library is not available for this platform")

def test_compile_simple_query():
    """Test compiling a simple Wvlet query"""
    try:
        compiler = WvletCompiler()
    except NotImplementedError:
        pytest.skip("Native library is not available for this platform")
    
    # Test a self-contained query that doesn't depend on external schema
    query = "select 1 as num"
    sql = compiler.compile(query)
    assert sql is not None
    assert len(sql) > 0
    # The compiled SQL should contain the select statement
    assert 'select' in sql.lower()
    assert '1' in sql

def test_compile_function():
    """Test the convenience compile function"""
    try:
        # Test a simple query using the convenience function
        sql = wvlet_compile("select 1 as result")
        assert sql is not None
        assert len(sql) > 0
        assert 'select' in sql.lower()
        assert '1' in sql
    except NotImplementedError:
        pytest.skip("Native library is not available for this platform")

def test_native_library_loading():
    """Test that native library loading doesn't crash"""
    from wvlet.compiler import _load_native_library
    # This should not raise an exception, even if library is not found
    lib = _load_native_library()
    # lib can be None if platform is not supported or library not found
    assert lib is None or hasattr(lib, 'wvlet_compile_query')

def test_compile_error_handling():
    """Test error handling for invalid queries"""
    try:
        compiler = WvletCompiler()
    except NotImplementedError:
        pytest.skip("Native library is not available for this platform")
    
    # Test with an invalid query
    with pytest.raises(CompilationError):
        compiler.compile("invalid query syntax !@#")

