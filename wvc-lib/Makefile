TARGET=target
SCALA_VERSION=$(shell cat ../SCALA_VERSION | xargs)
OUT_DIR=$(TARGET)/scala-$(SCALA_VERSION)

ARGS:="--help"

clean:
	rm -rf $(TARGET)/test-* $(OUT_DIR)/*.a $(OUT_DIR)/*.so

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
LINKER_OPTS:=$(addprefix -L, $(subst :, ,$(LIB_DIRS)))
LIBS:=-l$(LIB_NAME) -lpthread -lcrypto

test: $(LIB_SHARED)
	$(MAKE) rust c c++ ARGS="-q 'select 1'"

$(LIB_SHARED):
	$(MAKE) -p $(OUT_DIR)
	cd .. && ./sbt "wvcLib/nativeLink"

# Ensure the static library is built
$(STATIC_LIB):
	cd .. && ./sbt "wvcLib/nativeLinkStatic"

rust: $(TARGET)/test-rust
	$(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-rust: src/test/rust/test.rs $(LIB_SHARED)
	rustc $(LINKER_OPTS) $< $(LIBS) -o $@

c++: $(TARGET)/test-c++
	$(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-c++: src/test/c++/test.cpp $(LIB_SHARED)
	g++ $(LINKER_OPTS) $< $(LIBS) -o $@

c: $(TARGET)/test-c
	$(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-c: src/test/c/test.c $(LIB_SHARED)
	gcc $(LINKER_OPTS) $< $(LIBS) -o $@

