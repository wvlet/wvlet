TARGET=target
SCALA_VERSION=$(shell cat ../SCALA_VERSION | xargs)
OUT_DIR=$(TARGET)/scala-$(SCALA_VERSION)

ARGS:="--help"

test: $(OUT_DIR)/libwvlet.dylib rust c++ c

test-select-rust:
    $(MAKE) rust ARGS="-q 'select 1'"

clean:
    rm -rf $(TARGET)/test-* $(OUT_DIR)/*.a

# check is Mac OS
ifeq ($(shell uname),Darwin)
LIB_DIRS:=$(OUT_DIR):/opt/homebrew/lib:/usr/lib
RUN_OPTS:=DYLD_LIBRARY_PATH=$(LIB_DIRS):${DYLD_LIBRARY_PATH}
LIB_SHARED:=$(OUT_DIR)/libwvlet.dylib
LIB_STATIC:=$(OUT_DIR)/libwvlet.a
else
LIB_DIRS:=$(OUT_DIR)
RUN_OPTS:=LD_LIBRARY_PATH=$(LIB_DIRS):${LD_LIBRARY_PATH}
LIB_SHARED:=$(OUT_DIR)/libwvlet.so
LIB_STATIC:=$(OUT_DIR)/libwvlet.a
endif

LIB_NAME:=wvlet
LINKER_OPTS:=$(addprefix -L, $(subst :, ,$(LIB_DIRS))) -l$(LIB_NAME) -lpthread -lcrypto

$(LIB_SHARED):
    cd .. && ./sbt "wvcLib/nativeLink"

# Ensure the static library is built
$(STATIC_LIB):
    cd .. && ./sbt "wvcLib/nativeLinkStatic"

# Add static library to the test targets
test: $(OUT_DIR)/libwvlet.dylib $(STATIC_LIB) rust c++ c

# Add static library to the test targets
test: $(OUT_DIR)/libwvlet.dylib $(STATIC_LIB) rust c++ c

rust: $(TARGET)/test-rust
    $(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-rust: src/test/rust/test.rs $(LIB) $(STATIC_LIB)
    mkdir -p $(TARGET)
    rustc $(LINKER_OPTS) $< -o $@

c++: $(TARGET)/test-c++
    $(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-c++: src/test/c++/test.cpp
    g++ $(LINKER_OPTS) $< -o $@

c: $(TARGET)/test-c
    $(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-c: src/test/c/test.c
    gcc $(LINKER_OPTS) $< -o $@

# Clean both dynamic and static libraries
clean:
    rm -rf $(TARGET)/test-* $(OUT_DIR)/libwvlet.*
