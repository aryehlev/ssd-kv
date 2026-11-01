//! Compiles the C shim (DESIGN §8.1, §22, §24.3): the `long double` arithmetic used by
//! INCRBYFLOAT plus Redis `string2d` (`c/ldouble.c`), and the clock reads that `std` does not
//! offer (`c/clock.c`, CLOCK_BOOTTIME). Every function is declared in `src/ldouble.rs`, the
//! crate's only FFI boundary.

fn main() {
    println!("cargo:rerun-if-changed=c/ldouble.c");
    println!("cargo:rerun-if-changed=c/clock.c");
    cc::Build::new()
        .file("c/ldouble.c")
        .file("c/clock.c")
        .std("c11")
        .warnings(true)
        .extra_warnings(true)
        .compile("kvcommonshim");
}
