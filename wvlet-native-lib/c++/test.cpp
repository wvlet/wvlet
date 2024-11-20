#ifdef __cplusplus
extern "C"{
#endif

int ScalaNativeInit(void);
int wvlet_compile_main();

#ifdef __cplusplus
}
#endif

int main(int argc, char** argv) {
  wvlet_compile_main();
  return 0;
}
