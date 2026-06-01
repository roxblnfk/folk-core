/*
 * folk_zts.c — thin C wrappers for PHP ZTS thread lifecycle.
 *
 * These functions call PHP macros/inline functions that cannot
 * be called directly from Rust FFI.
 */

#include "php.h"
#include "SAPI.h"

#ifdef ZTS
#include "TSRM.h"
#endif

#include "php_main.h"
#include "zend_stream.h"
#include "zend_execute.h"

/* Register the calling thread with PHP TSRM.
 * Must be called before any PHP API usage from a new thread.
 * No-op on non-ZTS builds. */
void folk_zts_thread_init(void) {
#ifdef ZTS
    (void)ts_resource(0);
#ifdef PHP_WIN32
    ZEND_TSRMLS_CACHE_UPDATE();
#endif
#endif
}

/* Unregister the calling thread from PHP TSRM.
 * Must be called before the thread exits.
 * No-op on non-ZTS builds. */
void folk_zts_thread_shutdown(void) {
#ifdef ZTS
    ts_free_thread();
#endif
}

/* Start a PHP request on the current thread.
 * Returns 0 (SUCCESS) or -1 (FAILURE). */
int folk_zts_request_startup(void) {
    return php_request_startup();
}

/* Shut down the PHP request on the current thread. */
void folk_zts_request_shutdown(void) {
    php_request_shutdown(NULL);
}

/* Execute a PHP script file on the current thread.
 * Skips the shebang line (#!/...) if present — CLI SAPI does this
 * automatically but ZTS worker threads use the embed context where
 * shebang stripping is not performed.
 * Returns 1 (success) or 0 (failure). */
int folk_zts_execute_script(const char *filename) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) return 0;

    /* Skip shebang line if present. */
    int c1 = fgetc(fp);
    int c2 = fgetc(fp);
    if (c1 == '#' && c2 == '!') {
        int c;
        while ((c = fgetc(fp)) != EOF && c != '\n') {}
    } else {
        rewind(fp);
    }

    zend_file_handle file_handle;
    zend_stream_init_fp(&file_handle, fp, filename);
    int ret = php_execute_script(&file_handle);
    zend_destroy_file_handle(&file_handle);
    /* fp is closed by zend_destroy_file_handle */
    return ret ? 1 : 0;
}

/* Call a PHP function by name with two arguments (method string + params array).
 * Returns 0 (SUCCESS) or non-zero (FAILURE).
 * retval must point to a valid zval (will be overwritten). */
int folk_zts_call_dispatch(const char *func_name, zval *method_zval, zval *params_zval, zval *retval) {
    zval fname;
    ZVAL_STRING(&fname, func_name);

    zval args[2];
    ZVAL_COPY(&args[0], method_zval);
    ZVAL_COPY(&args[1], params_zval);

    int result = call_user_function(
        CG(function_table),
        NULL,
        &fname,
        retval,
        2,
        args
    );

    /* Clean up our copies (decrements refcount). */
    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
    zval_ptr_dtor(&fname);
    return result;
}

/* Evaluate a PHP code string on the current thread.
 * The code must NOT include the opening <?php tag.
 * Returns 1 (success) or 0 (failure). */
int folk_zts_eval_string(const char *code) {
    zval retval;
    int result = zend_eval_string((char *)code, &retval, "folk-warmup");
    zval_ptr_dtor(&retval);
    return (result == SUCCESS) ? 1 : 0;
}

/* Change PHP's virtual CWD (VCWD) to the given path.
 * In ZTS builds, this changes the per-thread working directory.
 * Returns 0 on success, -1 on failure. */
int folk_zts_chdir(const char *path) {
#ifdef VIRTUAL_DIR
    return VCWD_CHDIR(path);
#else
    return chdir(path);
#endif
}

/* Check if the current PHP build has ZTS enabled. */
int folk_zts_is_enabled(void) {
#ifdef ZTS
    return 1;
#else
    return 0;
#endif
}
