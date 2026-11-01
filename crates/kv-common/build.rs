//! Compiles the C `long double` shim used by INCRBYFLOAT (DESIGN §8.1, §24.3).

fn main() {
    println!("cargo:rerun-if-changed=c/ldouble.c");
    cc::Build::new()
        .file("c/ldouble.c")
        .std("c11")
        .warnings(true)
        .extra_warnings(true)
        .compile("kvldouble");
}
