#include <assert.h>
#include <stdio.h>
#include <string.h>

int wvlet_compile_main(char* json);

int main(int argc, char** argv) {
  // Convert argv to json array string
  char json[1024];
  size_t pos = 0;
  size_t remaining = sizeof(json) - 1; // Reserve space for null terminator

  // Start with opening bracket
  pos += snprintf(json + pos, remaining, "[");
  remaining = sizeof(json) - pos - 1;

  for (int i = 1; i < argc; i++) {
    // Add comma if not the first element
    if(i != 1) {
      pos += snprintf(json + pos, remaining, ",");
      remaining = sizeof(json) - pos - 1;
    }

    // Add the quoted argument
    pos += snprintf(json + pos, remaining, "\"%s\"", argv[i]);
    remaining = sizeof(json) - pos - 1;

    // Check if we're running out of space
    if (remaining <= 1) {
      fprintf(stderr, "Error: JSON buffer full\n");
      return 1;
    }
  }

  // Add closing bracket
  pos += snprintf(json + pos, remaining, "]");

  wvlet_compile_main(json);
  return 0;
}
