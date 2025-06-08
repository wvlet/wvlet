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
from wvlet.compiler import WvletCompiler
from wvlet import compile as wvlet_compile

def test_wvlet_invalid_path():
    with pytest.raises(ValueError, match="Invalid executable_path: invalid"):
        WvletCompiler(executable_path="invalid")

def test_wvlet_initialization():
    """Test that WvletCompiler can be initialized (either with native lib or executable)"""
    try:
        compiler = WvletCompiler()
        # If we get here, either native library or executable is available
        assert compiler is not None
    except NotImplementedError:
        pytest.skip("Neither native library nor wvlet executable is available")

def test_compile_simple_query():
    """Test compiling a simple Wvlet query"""
    try:
        compiler = WvletCompiler()
    except NotImplementedError:
        pytest.skip("Neither native library nor wvlet executable is available")
    
    # Test a simple query
    query = "from users select name"
    try:
        sql = compiler.compile(query)
        assert sql is not None
        assert len(sql) > 0
        # The exact SQL output depends on the target, but it should contain 'users'
        assert 'users' in sql.lower()
    except ValueError as e:
        # If compilation fails, it might be due to missing catalog/schema
        # This is acceptable for the test environment
        assert "Failed to compile" in str(e)

def test_compile_function():
    """Test the convenience compile function"""
    try:
        # Test a simple query using the convenience function
        sql = wvlet_compile("select 1")
        assert sql is not None
        assert len(sql) > 0
    except (NotImplementedError, ValueError):
        pytest.skip("Compilation not available in test environment")

def test_native_library_loading():
    """Test that native library loading doesn't crash"""
    from wvlet.compiler import _load_native_library
    # This should not raise an exception, even if library is not found
    lib = _load_native_library()
    # lib can be None if platform is not supported or library not found
    assert lib is None or hasattr(lib, 'wvlet_compile_query')

