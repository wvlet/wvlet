#include <assert.h>
#include <stdio.h>

int wvlet_compile_main(char* json);

int main(int argc, char** argv) {
  // Convert argv to json array string
  char json[1024];
  sprintf(json, "[");
  for (int i = 1; i < argc; i++) {
    if(i != 1) {
      sprintf(json, "%s,", json);
    }
    sprintf(json, "%s\"%s\"", json, argv[i]);
  }
  sprintf(json, "%s]", json);

  wvlet_compile_main(json);
  return 0;
}
