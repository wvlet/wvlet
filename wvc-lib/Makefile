TARGET=target
SCALA_VERSION=$(shell cat ../SCALA_VERSION | xargs)
# sbt 2 emits the linked Scala Native library under the root's
# target/out/native0.5/scala-<ver>/wvc-lib/ tree instead of the sbt 1
# wvc-lib/target/scala-<ver>/ per-project location.
OUT_DIR=../target/out/native0.5/scala-$(SCALA_VERSION)/wvc-lib

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

# sbt 2 writes $(LIB_SHARED) to ../target/out/native0.5/scala-<ver>/wvc-lib/, so nothing
# creates wvc-lib/target/ where rustc / gcc drop their test binaries. Order-only prereq.
$(TARGET):
	mkdir -p $@

$(LIB_SHARED):
	cd .. && ./sbt "wvcLib/nativeLink"

# Ensure the static library is built
$(STATIC_LIB):
	cd .. && ./sbt "wvcLib/nativeLinkStatic"

rust: $(TARGET)/test-rust
	$(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-rust: src/test/rust/test.rs $(LIB_SHARED) | $(TARGET)
	rustc $(LINKER_OPTS) $< $(LIBS) -o $@

c++: $(TARGET)/test-c++
	$(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-c++: src/test/c++/test.cpp $(LIB_SHARED) | $(TARGET)
	g++ $(LINKER_OPTS) $< $(LIBS) -o $@

c: $(TARGET)/test-c
	$(RUN_OPTS) $< $(ARGS)

$(TARGET)/test-c: src/test/c/test.c $(LIB_SHARED) | $(TARGET)
	gcc $(LINKER_OPTS) $< $(LIBS) -o $@

c-json: $(TARGET)/test-c-json
	$(RUN_OPTS) $<

$(TARGET)/test-c-json: src/test/c/test_json.c $(LIB_SHARED) | $(TARGET)
	gcc $(LINKER_OPTS) $< $(LIBS) -o $@
