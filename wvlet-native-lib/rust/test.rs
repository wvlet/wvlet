extern "C" {
    fn wvlet_compile_main() -> i32;
}

fn main() {
    unsafe {
        wvlet_compile_main();
    }
}
