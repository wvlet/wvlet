#include "wvc.h"
#include <assert.h>

int main(int argc, char** argv) {
  assert(ScalaNativeInit() == 0);
  wvlet_compile_main(argv);
}
