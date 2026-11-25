// Public C API of libwvlet.
//
// Scala Native normally starts its runtime from a shared-library constructor
// (dylib_init.c). Since 0.5.13 that start-up captures a stack trace
// (OutOfMemory.preallocateFallback -> fillInStackTrace), so libunwind walks
// from the constructor into the dynamic loader's frames (_dl_init,
// _dl_start_user). With Nix's glibc 2.42 loader on x86_64 that walk reads past
// the end of ld.so's .eh_frame, which has no zero terminator, and the host
// process crashes with SIGSEGV before main().
//
// wvcLib therefore builds with -DSCALANATIVE_NO_DYLIB_CTOR and
// -DWVLET_LAZY_INIT: the runtime starts on the first call into this API, from
// the host's own call stack, whose frames unwind cleanly. Builds without
// WVLET_LAZY_INIT (the static library, whose host calls ScalaNativeInit
// itself, or builds that keep the constructor) only forward the calls.

#ifdef _WIN32
#include <windows.h>
#define WVLET_EXPORT __declspec(dllexport)
#else
#include <pthread.h>
#define WVLET_EXPORT __attribute__((visibility("default")))
#endif
#include <stdio.h>
#include <stdlib.h>

extern int wvlet_internal_compile_main(const char *json);
extern char *wvlet_internal_compile_query(const char *json);
extern char *wvlet_internal_compile_query_json(const char *json);

#ifdef WVLET_LAZY_INIT
extern int ScalaNativeInit(void);

static int wvlet_init_status = -1;

static void wvlet_run_init(void) { wvlet_init_status = ScalaNativeInit(); }

#ifdef _WIN32
static INIT_ONCE wvlet_init_once = INIT_ONCE_STATIC_INIT;
static BOOL CALLBACK wvlet_init_callback(PINIT_ONCE once, PVOID param,
                                         PVOID *context) {
    (void)once;
    (void)param;
    (void)context;
    wvlet_run_init();
    return TRUE;
}
#else
static pthread_once_t wvlet_init_once = PTHREAD_ONCE_INIT;
#endif

// Starts the Scala Native runtime once; later and concurrent callers wait for
// the first one. Aborts if the runtime cannot start, as the constructor in
// dylib_init.c does, since no Scala code can run without it.
static void wvlet_ensure_init(void) {
#ifdef _WIN32
    InitOnceExecuteOnce(&wvlet_init_once, wvlet_init_callback, NULL, NULL);
#else
    pthread_once(&wvlet_init_once, wvlet_run_init);
#endif
    if (wvlet_init_status != 0) {
        fprintf(stderr, "Failed to initialize Scala Native\n");
        exit(1);
    }
}
#else
static void wvlet_ensure_init(void) {}
#endif

WVLET_EXPORT int wvlet_compile_main(const char *json) {
    wvlet_ensure_init();
    return wvlet_internal_compile_main(json);
}

WVLET_EXPORT char *wvlet_compile_query(const char *json) {
    wvlet_ensure_init();
    return wvlet_internal_compile_query(json);
}

WVLET_EXPORT char *wvlet_compile_query_json(const char *json) {
    wvlet_ensure_init();
    return wvlet_internal_compile_query_json(json);
}
