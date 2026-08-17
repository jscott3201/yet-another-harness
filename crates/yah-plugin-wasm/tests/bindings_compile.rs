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

    // Without the driver's `with` remap the resource is an uninhabited
    // marker, which is exactly what a compile-proof needs: the trait surface
    // is checked while no entry can ever exist.
    impl yah::plugin::capabilities::HostCapability for State {
        fn invoke(
            &mut self,
            _this: wasmtime::component::Resource<yah::plugin::capabilities::Capability>,
            _input: String,
        ) -> Result<String, yah::plugin::capabilities::CallError> {
            Err(yah::plugin::capabilities::CallError {
                code: yah::plugin::capabilities::CallErrorCode::Failed,
                message: String::new(),
            })
        }

        fn drop(
            &mut self,
            _rep: wasmtime::component::Resource<yah::plugin::capabilities::Capability>,
        ) -> wasmtime::Result<()> {
            Ok(())
        }
    }

    impl yah::plugin::capabilities::Host for State {
        fn acquire(
            &mut self,
            _capability_id: String,
        ) -> Result<
            wasmtime::component::Resource<yah::plugin::capabilities::Capability>,
            yah::plugin::capabilities::AcquireError,
        > {
            Err(yah::plugin::capabilities::AcquireError {
                code: yah::plugin::capabilities::AcquireErrorCode::NotGranted,
                message: String::new(),
            })
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
