#ifdef __cplusplus
extern "C"{
#endif

int wvlet_compile_main(const char*);

#ifdef __cplusplus
}
#endif

#include <sstream>

int main(int argc, char** argv) {
  std::stringstream ss;
  ss << "[";
  // create a json string from argv
  for (int i = 1; i < argc; i++) {
    if(i != 1) {
      ss << ",";
    }
    ss << "\"" << argv[i] << "\"";
  }
  ss << "]";
  std::string json = ss.str();
  wvlet_compile_main(json.c_str());
  return 0;
}
