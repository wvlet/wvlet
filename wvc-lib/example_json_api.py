#!/usr/bin/env python3
"""
Example of using the new JSON-based wvlet_compile_query_json API from Python
"""

import ctypes
import json
import platform
import os

def get_library_path():
    """Get the platform-specific library path"""
    system = platform.system()
    if system == "Darwin":
        return "./libwvlet.dylib"
    elif system == "Linux":
        return "./libwvlet.so"
    elif system == "Windows":
        return "./wvlet.dll"
    else:
        raise RuntimeError(f"Unsupported platform: {system}")

def compile_wvlet_query(query: str, library_path: str = None):
    """
    Compile a Wvlet query using the JSON API
    
    Args:
        query: The Wvlet query string
        library_path: Optional path to the libwvlet library
        
    Returns:
        dict: JSON response with either 'sql' or 'error' field
        
    Error Status Types:
        - Success: Successful compilation
        - UserError: User or query errors (not retryable)
        - InternalError: Internal errors (usually retryable)
        - ResourceExhausted: Insufficient resources (retry after resources available)
    """
    if library_path is None:
        library_path = get_library_path()
    
    # Load the library
    lib = ctypes.CDLL(library_path)
    
    # Define the function signature
    lib.wvlet_compile_query_json.argtypes = [ctypes.c_char_p]
    lib.wvlet_compile_query_json.restype = ctypes.c_char_p
    
    # Prepare arguments as JSON
    args = ["wv", "-q", query]
    args_json = json.dumps(args).encode('utf-8')
    
    # Call the function
    result_ptr = lib.wvlet_compile_query_json(args_json)
    result_json = result_ptr.decode('utf-8')
    
    # Parse and return the JSON response
    return json.loads(result_json)

def main():
    # Example 1: Valid query
    print("=== Example 1: Valid Query ===")
    result = compile_wvlet_query("from users | select name, age | where age > 21")
    if result['success']:
        print(f"Generated SQL:\n{result['sql']}")
    else:
        print(f"Error: {result['error']['message']}")
    print()
    
    # Example 2: Query with syntax error
    print("=== Example 2: Syntax Error ===")
    result = compile_wvlet_query("from users | invalid syntax")
    if result['success']:
        print(f"Generated SQL:\n{result['sql']}")
    else:
        error = result['error']
        print(f"Error Code: {error['code']}")
        print(f"Status Type: {error['statusType']}")
        print(f"Error Message: {error['message']}")
        if 'location' in error and error['location']:
            loc = error['location']
            print(f"Location: {loc['fileName']}:{loc['line']}:{loc['column']}")
            if 'lineContent' in loc and loc['lineContent']:
                print(f"Line: {loc['lineContent']}")
    print()
    
    # Example 3: Complex query
    print("=== Example 3: Complex Query ===")
    complex_query = """
    from orders
    | join customers on orders.customer_id = customers.id
    | where order_date >= '2024-01-01'
    | group by customers.country
    | select country, count(*) as order_count, sum(total_amount) as revenue
    | order by revenue desc
    | limit 10
    """
    result = compile_wvlet_query(complex_query.strip())
    if result['success']:
        print(f"Generated SQL:\n{result['sql']}")
    else:
        print(f"Error: {result['error']['message']}")

if __name__ == "__main__":
    main()