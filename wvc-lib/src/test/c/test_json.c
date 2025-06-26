#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

// Declare the new JSON-based function
char* wvlet_compile_query_json(char* json);

// Simple JSON field checker
int has_json_field(const char* json, const char* field, const char* value) {
  char search[256];
  if (value) {
    snprintf(search, sizeof(search), "\"%s\":\"%s\"", field, value);
  } else {
    snprintf(search, sizeof(search), "\"%s\":", field);
  }
  return strstr(json, search) != NULL;
}

int main(int argc, char** argv) {
  int passed = 0;
  int failed = 0;
  
  // Example 1: Compile a valid query
  printf("=== Test 1: Valid Query ===\n");
  char* args1 = "[\"wv\", \"-q\", \"from users | select name, age | where age > 21\"]";
  char* result1 = wvlet_compile_query_json(args1);
  printf("Result: %s\n", result1);
  
  // Verify success response
  assert(has_json_field(result1, "success", "true"));
  assert(has_json_field(result1, "sql", NULL));
  assert(!has_json_field(result1, "error", NULL));
  printf("✓ Valid query compiled successfully\n\n");
  passed++;
  
  // Example 2: Compile with syntax error
  printf("=== Test 2: Syntax Error ===\n");
  char* args2 = "[\"wv\", \"-q\", \"from users | invalid syntax here\"]";
  char* result2 = wvlet_compile_query_json(args2);
  printf("Result: %s\n", result2);
  
  // Verify error response
  assert(has_json_field(result2, "success", "false"));
  assert(has_json_field(result2, "error", NULL));
  assert(has_json_field(result2, "statusType", NULL));
  assert(!has_json_field(result2, "sql", NULL));
  printf("✓ Syntax error handled correctly\n\n");
  passed++;
  
  // Example 3: Missing query
  printf("=== Test 3: Missing Query ===\n");
  char* args3 = "[\"wv\"]";
  char* result3 = wvlet_compile_query_json(args3);
  printf("Result: %s\n", result3);
  
  // Verify error response
  assert(has_json_field(result3, "success", "false"));
  assert(has_json_field(result3, "error", NULL));
  printf("✓ Missing query handled as error\n\n");
  passed++;
  
  // Example 4: Empty query
  printf("=== Test 4: Empty Query ===\n");
  char* args4 = "[\"wv\", \"-q\", \"\"]";
  char* result4 = wvlet_compile_query_json(args4);
  printf("Result: %s\n", result4);
  
  // Verify error response for empty query
  assert(has_json_field(result4, "success", "false"));
  assert(has_json_field(result4, "error", NULL));
  printf("✓ Empty query handled as error\n\n");
  passed++;
  
  printf("\n=== Test Summary ===\n");
  printf("Passed: %d\n", passed);
  printf("Failed: %d\n", failed);
  printf("Total:  %d\n", passed + failed);
  
  return failed > 0 ? 1 : 0;
}