#![allow(dead_code)]

mod host {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "conformance",
    });

    struct State;

    impl yah::plugin::logging::Host for State {
        fn log(
            &mut self,
            _level: yah::plugin::logging::LogLevel,
            _message: String,
            _fields: Vec<yah::plugin::logging::LogField>,
        ) {
        }
    }

    impl yah::plugin::cancellation::Host for State {
        fn is_cancelled(&mut self) -> bool {
            false
        }
    }
}

mod guest {
    wit_bindgen::generate!({
        path: "wit",
        world: "conformance",
    });

    struct Guest;

    impl exports::yah::plugin::lifecycle::Guest for Guest {
        fn activate() -> Result<(), exports::yah::plugin::lifecycle::GuestError> {
            Ok(())
        }
    }

    impl exports::yah::plugin::fixture_tool::Guest for Guest {
        fn invoke(
            _input_json: String,
        ) -> Result<String, exports::yah::plugin::fixture_tool::GuestError> {
            Ok("null".to_owned())
        }
    }

    export!(Guest);
}

#[test]
fn pinned_host_and_guest_bindings_compile_from_one_world() {
    assert_eq!(yah_plugin_wasm::WIT_PACKAGE, "yah:plugin@0.1.0");
    assert_eq!(yah_plugin_wasm::WIT_WORLD, "conformance");
}
