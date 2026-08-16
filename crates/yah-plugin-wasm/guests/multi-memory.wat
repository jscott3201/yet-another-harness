;; Fixture guest: declares more than one linear memory.
;;
;; Wasmtime's own store limiter applies its ceiling to each memory separately,
;; so a guest that spreads its allocation across several memories would be
;; admitted well above the total a host believed it had set. This fixture is
;; that guest. Its two memories are declared with initial pages, so the total is
;; claimed at instantiation: the limiter still sees both requests, but there is
;; no `memory.grow` instruction to answer with -1, so the refusal aborts
;; instantiation instead.
;;
;; See `conformant.wat` for the ABI and named-type rules.

(component
  (core module $impl
    (memory (export "memory") 2)
    (memory $second 2)

    (func (export "cabi_realloc")
      (param $old_ptr i32) (param $old_len i32) (param $align i32) (param $new_len i32)
      (result i32)
      ;; Unreachable in practice: the ceiling refuses this guest at
      ;; instantiation, so no lift or lower ever asks it for memory.
      unreachable)

    ;; Reached only if the ceiling admitted both memories.
    (func (export "activate") (result i32)
      (i32.store8 (i32.const 1024) (i32.const 0))
      (i32.const 1024))

    (func (export "invoke") (param $ptr i32) (param $len i32) (result i32)
      unreachable)
  )

  (core instance $i (instantiate $impl))

  (type $lifecycle-code (enum "cancelled" "invalid-state" "failed"))
  (type $lifecycle-error (record (field "code" $lifecycle-code) (field "message" string)))
  (type $activate-result (result (error $lifecycle-error)))
  (type $activate-fn (func (result $activate-result)))

  (func $activate (type $activate-fn)
    (canon lift (core func $i "activate")
      (memory $i "memory")
      (realloc (func $i "cabi_realloc"))))

  (instance $lifecycle
    (export "error-code" (type $lifecycle-code))
    (export "guest-error" (type $lifecycle-error))
    (export "activate" (func $activate)))
  (export "yah:plugin/lifecycle@0.1.0" (instance $lifecycle))

  (type $tool-code (enum "invalid-input" "cancelled" "failed"))
  (type $tool-error (record (field "code" $tool-code) (field "message" string)))
  (type $invoke-result (result string (error $tool-error)))
  (type $invoke-fn (func (param "input-json" string) (result $invoke-result)))

  (func $invoke (type $invoke-fn)
    (canon lift (core func $i "invoke")
      (memory $i "memory")
      (realloc (func $i "cabi_realloc"))
      string-encoding=utf8))

  (instance $fixture-tool
    (export "error-code" (type $tool-code))
    (export "guest-error" (type $tool-error))
    (export "invoke" (func $invoke)))
  (export "yah:plugin/fixture-tool@0.1.0" (instance $fixture-tool))
)
