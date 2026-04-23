fn main() {
    // On Linux, native test binaries can't link because pgrx_pg_sys references
    // postgres server symbols (errstart, palloc0, etc.) that are only available
    // at runtime when the extension is loaded into postgres. We provide weak stub
    // definitions that satisfy the linker; they are never called in #[test] paths.
    //
    // macOS handles this with -Wl,-undefined,dynamic_lookup in .cargo/config.toml.
    // lld's ELF mode has no equivalent flag for executables, so we use stubs instead.
    #[cfg(target_os = "linux")]
    emit_postgres_stubs();
}

#[cfg(target_os = "linux")]
fn emit_postgres_stubs() {
    use std::path::PathBuf;
    use std::process::Command;

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    let stub_c = out_dir.join("pg_reflex_pg_stubs.c");
    std::fs::write(&stub_c, PG_STUBS_C).expect("write pg_stubs.c");

    let stub_o = out_dir.join("pg_reflex_pg_stubs.o");
    let cc_status = Command::new("cc")
        .args([
            "-c",
            "-fPIC",
            "-w",
            stub_c.to_str().unwrap(),
            "-o",
            stub_o.to_str().unwrap(),
        ])
        .status()
        .expect("invoke cc for pg stubs");
    assert!(cc_status.success(), "failed to compile pg stubs");

    let stub_a = out_dir.join("libpg_reflex_pg_stubs.a");
    let ar_status = Command::new("ar")
        .args(["rcs", stub_a.to_str().unwrap(), stub_o.to_str().unwrap()])
        .status()
        .expect("invoke ar for pg stubs");
    assert!(ar_status.success(), "failed to archive pg stubs");

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=pg_reflex_pg_stubs");
    println!("cargo:rerun-if-changed=build.rs");
}

#[cfg(target_os = "linux")]
const PG_STUBS_C: &str = r#"
/* Weak stubs for postgres server symbols used by pgrx in test binaries.
 * These are never called in #[test] paths — they exist only to satisfy the linker.
 * When loaded into postgres the strong symbols from the server override these. */
#include <stddef.h>
#include <stdarg.h>

__attribute__((weak)) int   errstart(int elevel, const char *domain)    { return 0; }
__attribute__((weak)) void  errfinish(const char *file, int lineno, const char *fn) {}
__attribute__((weak)) int   errcode(int sqlerrcode)                     { return 0; }
__attribute__((weak)) int   errmsg(const char *fmt, ...)                { return 0; }
__attribute__((weak)) int   errdetail(const char *fmt, ...)             { return 0; }
__attribute__((weak)) int   errhint(const char *fmt, ...)               { return 0; }
__attribute__((weak)) int   errcontext_msg(const char *fmt, ...)        { return 0; }
__attribute__((weak)) void  pfree(void *ptr)                            {}
__attribute__((weak)) void *palloc0(size_t size)                        { return NULL; }
__attribute__((weak)) void *CopyErrorData(void)                         { return NULL; }
__attribute__((weak)) void  FreeErrorData(void *edata)                  {}

__attribute__((weak)) void *CurrentMemoryContext  = NULL;
__attribute__((weak)) void *ErrorContext          = NULL;
__attribute__((weak)) void *PG_exception_stack    = NULL;
__attribute__((weak)) void *error_context_stack   = NULL;
"#;
