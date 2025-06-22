#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

// Declare the new JSON-based function
char* wvlet_compile_query_json(char* json);

int main(int argc, char** argv) {
  // Example 1: Compile a valid query
  printf("=== Test 1: Valid Query ===\n");
  char* args1 = "[\"wv\", \"-q\", \"from users | select name, age | where age > 21\"]";
  char* result1 = wvlet_compile_query_json(args1);
  printf("Result: %s\n\n", result1);
  
  // Example 2: Compile with syntax error
  printf("=== Test 2: Syntax Error ===\n");
  char* args2 = "[\"wv\", \"-q\", \"from users | invalid syntax here\"]";
  char* result2 = wvlet_compile_query_json(args2);
  printf("Result: %s\n\n", result2);
  
  // Example 3: Missing query
  printf("=== Test 3: Missing Query ===\n");
  char* args3 = "[\"wv\"]";
  char* result3 = wvlet_compile_query_json(args3);
  printf("Result: %s\n\n", result3);
  
  // Example 4: Query from file
  printf("=== Test 4: Query from File ===\n");
  char* args4 = "[\"wv\", \"test.wv\"]";
  char* result4 = wvlet_compile_query_json(args4);
  printf("Result: %s\n\n", result4);
  
  return 0;
}