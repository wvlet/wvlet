#include <assert.h>
#include <stdio.h>

int ScalaNativeInit(void);
int wvlet_compile_main();

int main(int argc, char** argv) {
  printf("Hello from wvc_test\n");
  wvlet_compile_main();
  return 0;
}
