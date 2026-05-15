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
 * Returns 0 (SUCCESS) or -1 (FAILURE). */
int folk_zts_execute_script(const char *filename) {
    zend_file_handle file_handle;
    zend_stream_init_filename(&file_handle, filename);
    int ret = php_execute_script(&file_handle);
    zend_destroy_file_handle(&file_handle);
    return ret;
}

/* Call a PHP function by name with an array argument.
 * Returns 0 (SUCCESS) or -1 (FAILURE).
 * retval must point to a valid zval (will be overwritten). */
int folk_zts_call_function(const char *func_name, zval *arg, zval *retval) {
    zval fname;
    ZVAL_STRING(&fname, func_name);

    int result = call_user_function(
        CG(function_table),
        NULL,
        &fname,
        retval,
        1,
        arg
    );

    zval_ptr_dtor(&fname);
    return result;
}

/* Check if the current PHP build has ZTS enabled. */
int folk_zts_is_enabled(void) {
#ifdef ZTS
    return 1;
#else
    return 0;
#endif
}
