;; Fixture guest: never returns from `activate`.
;;
;; This is the case cooperative cancellation cannot reach. The world's
;; cancellation import is advisory, and this guest never asks, so the only thing
;; that stops it is the host's call deadline. It exists to prove the host does
;; not need the guest's agreement to stop it.
;;
;; The loop is a plain backedge because that is where the epoch check is
;; compiled in. See `conformant.wat` for the ABI and named-type rules.

(component
  (core module $impl
    (memory (export "memory") 1)

    (func (export "cabi_realloc")
      (param $old_ptr i32) (param $old_len i32) (param $align i32) (param $new_len i32)
      (result i32)
      ;; Unreachable in practice: this fixture is only ever driven through
      ;; `activate`, which takes no parameters, so the host has nothing to
      ;; lower and the guest never returns to allocate a result.
      unreachable)

    ;; Spin forever. The return type is unreachable in both senses.
    (func (export "activate") (result i32)
      (loop $forever (br $forever))
      unreachable)

    ;; Never entered: lowering `invoke`'s string parameter goes through the
    ;; trapping `cabi_realloc` above, so a call traps before reaching this. The
    ;; export exists because the world requires it, not because it runs.
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
