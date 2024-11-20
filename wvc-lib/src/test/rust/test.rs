use std::ffi::CString;
use std::os::raw::c_char;
use std::os::raw::c_void;
use std::env;

extern "C" {
    fn ScalaNativeInit() -> i32;
    fn wvlet_compile_main(json: *const c_char) -> i32;
}

fn main() {
    // skip first element
    let args: Vec<String> = env::args().skip(1).collect();
    // convert args to JSON array wrapped with [ ... ]
    let json_array = format!("[{}]",
                    args.iter()
            .map(|s| format!("\"{}\"", s))
            .collect::<Vec<String>>()
            .join(","));

    let query = CString::new(json_array).unwrap();

    unsafe {
        wvlet_compile_main(query.as_ptr());
    }
}
