;; Fixture guest: recurses until the host's stack bound stops it.
;;
;; The recursion bound is not the stack size. Wasmtime sizes the stack a call
;; runs on and bounds how deep the guest may go on it with a separate number,
;; and a host that set only the first would be leaving the second at Wasmtime's
;; default. This guest exists so the difference is testable: it does nothing but
;; call itself, so the only thing that can stop it is the depth bound.
;;
;; Each frame carries a few locals so a frame costs more than a return address,
;; which keeps the test from needing millions of calls to reach the bound.
;;
;; See `conformant.wat` for the ABI and named-type rules.

(component
  (core module $impl
    (memory (export "memory") 1)

    (func (export "cabi_realloc")
      (param $old_ptr i32) (param $old_len i32) (param $align i32) (param $new_len i32)
      (result i32)
      ;; Unreachable in practice: `activate` takes no parameters and its `ok`
      ;; arm carries no payload, so nothing is ever lifted or lowered.
      unreachable)

    ;; Descend a fixed number of frames. Nothing here traps on its own: either
    ;; the host's depth bound stops it, or it returns.
    (func $descend (param $remaining i64) (result i64)
      (local $a i64) (local $b i64) (local $c i64) (local $d i64)
      (if (i64.le_s (local.get $remaining) (i64.const 0))
        (then (return (i64.const 0))))
      (local.set $a (i64.sub (local.get $remaining) (i64.const 1)))
      (local.set $b (i64.add (local.get $a) (i64.const 1)))
      (local.set $c (i64.add (local.get $b) (i64.const 1)))
      (local.set $d (i64.add (local.get $c) (i64.const 1)))
      (i64.add (local.get $d) (call $descend (local.get $a))))

    (func (export "activate") (result i32)
      (drop (call $descend (i64.const 20000)))
      ;; Every frame was granted: return ok so a generous bound is observable.
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
