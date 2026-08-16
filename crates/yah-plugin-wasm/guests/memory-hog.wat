;; Fixture guest: grows linear memory until the host stops giving it more.
;;
;; The memory ceiling differs in kind from the call deadline. A refused
;; `memory.grow` returns -1, which the guest can see and handle, so this is not
;; a case of the host terminating a guest against its will. The fixture makes
;; the refusal observable by trapping on it, so the driver still exercises the
;; failure path rather than silently succeeding at a smaller size.
;;
;; It asks for a fixed number of pages so that the same fixture activates
;; cleanly under a ceiling that can afford them. A fixture that always failed
;; would let a test pass without the ceiling doing anything.
;;
;; See `conformant.wat` for the ABI and named-type rules.

(component
  (core module $impl
    (memory (export "memory") 1)

    (func (export "cabi_realloc")
      (param $old_ptr i32) (param $old_len i32) (param $align i32) (param $new_len i32)
      (result i32)
      ;; Unreachable in practice: `activate` takes no parameters, so the host
      ;; has nothing to lower, and its `ok` result is a pointer into the page
      ;; this guest already owns.
      unreachable)

    ;; Ask for a fixed number of pages, one at a time, and trap on the first
    ;; refusal. `memory.grow` answers -1 rather than trapping, so the refusal is
    ;; a value the guest tests.
    ;;
    ;; The count is bounded rather than infinite so that the fixture succeeds
    ;; under a generous ceiling. That is what makes the ceiling, and not some
    ;; unrelated fault, the thing a test can attribute a failure to.
    (func (export "activate") (result i32)
      (local $remaining i32)
      (local.set $remaining (i32.const 8))
      (loop $grow
        (if (i32.eq (memory.grow (i32.const 1)) (i32.const -1))
          (then unreachable))
        (local.set $remaining (i32.sub (local.get $remaining) (i32.const 1)))
        (br_if $grow (i32.gt_s (local.get $remaining) (i32.const 0))))
      ;; Every page was granted: return ok so a generous ceiling is observable.
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
