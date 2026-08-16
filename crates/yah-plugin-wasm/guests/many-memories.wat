;; Fixture guest: declares many linear memories that hold nothing.
;;
;; This is the guest a byte ceiling alone cannot see. Each extra memory is
;; declared with zero pages, so it adds nothing to the total the memory ceiling
;; charges - and yet every one of them costs the host an address-space
;; reservation. A host that bounded only bytes would admit this guest, and a
;; few of them together would exhaust the address space of the authority
;; process while every one stayed under its "memory ceiling".
;;
;; The exported memory carries one page because the ABI needs somewhere to put
;; a return value. Everything above it is empty by construction.
;;
;; See `conformant.wat` for the ABI and named-type rules.

(component
  (core module $impl
    (memory (export "memory") 1)
    (memory $spare1 0)
    (memory $spare2 0)
    (memory $spare3 0)
    (memory $spare4 0)
    (memory $spare5 0)
    (memory $spare6 0)
    (memory $spare7 0)

    (func (export "cabi_realloc")
      (param $old_ptr i32) (param $old_len i32) (param $align i32) (param $new_len i32)
      (result i32)
      ;; Unreachable in practice: `activate` takes no parameters, so the host
      ;; has nothing to lower, and its `ok` arm carries no payload to lift.
      ;; This holds on both paths - the count ceiling refuses this guest under
      ;; a tight count, and admits it under a generous one, where `activate`
      ;; runs and still never needs an allocation.
      unreachable)

    ;; Reached only if the count ceiling admitted all eight memories.
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
