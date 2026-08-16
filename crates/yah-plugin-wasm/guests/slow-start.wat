;; Fixture guest: burns guest time during *instantiation*, then behaves.
;;
;; A core module's start function runs while the host is still inside
;; `instantiate`, which makes this the one fixture that proves instantiation is
;; a guest call rather than host bookkeeping. Everything the host says about
;; guest calls therefore applies to it: it runs on the fiber, it reaches epoch
;; deadlines, and it yields the thread at each one.
;;
;; That is what this fixture is for. Racing a tick against an ordinary
;; instantiation is a coin flip - a small component instantiates in tens of
;; microseconds, so on most runs no tick lands inside it and a test written that
;; way silently stops testing anything. Here the guest holds the fiber for
;; milliseconds, so the yield happens on every run and a test may assert it.
;;
;; The iteration count is sized against two bounds at once. It must exceed one
;; epoch tick by enough that a fast machine still yields at least once, and stay
;; far enough under the conformance harness's poll bound that a slow machine
;; does not exhaust it: at the 1ms tick its case uses, this lands around ten
;; yields here, which leaves room for a machine several times slower or faster
;; in either direction. It burns a fixed number of iterations rather than
;; watching a clock because a guest has no clock.
;;
;; Apart from the start section this is `conformant.wat`. See it for the ABI and
;; named-type rules.

(component
  (core module $impl
    (memory (export "memory") 1)
    (data (i32.const 128) "{\"activated\":true}")

    (global $bump (mut i32) (i32.const 4096))

    ;; Written to from the spin loop so nothing about it is dead. A global,
    ;; rather than a local, because a local the caller never reads is exactly
    ;; the kind of thing a compiler is entitled to delete.
    (global $sink (mut i64) (i64.const 0))

    (func $spin
      (local $remaining i64)
      (local.set $remaining (i64.const 8000000))
      (loop $burn
        (global.set $sink (i64.add (global.get $sink) (local.get $remaining)))
        (local.set $remaining (i64.sub (local.get $remaining) (i64.const 1)))
        (br_if $burn (i64.gt_s (local.get $remaining) (i64.const 0)))))

    (start $spin)

    (func (export "cabi_realloc")
      (param $old_ptr i32) (param $old_len i32) (param $align i32) (param $new_len i32)
      (result i32)
      (local $ptr i32)
      (local $limit i32)
      (local.set $limit (i32.mul (memory.size) (i32.const 65536)))
      (local.set $ptr
        (i32.and
          (i32.add (global.get $bump) (i32.sub (local.get $align) (i32.const 1)))
          (i32.xor (i32.sub (local.get $align) (i32.const 1)) (i32.const -1))))
      (if (i32.or
            (i32.gt_u (local.get $ptr) (local.get $limit))
            (i32.gt_u (local.get $new_len)
                      (i32.sub (local.get $limit) (local.get $ptr))))
        (then unreachable))
      (global.set $bump (i32.add (local.get $ptr) (local.get $new_len)))
      (local.get $ptr))

    (func (export "activate") (result i32)
      (i32.store8 (i32.const 1024) (i32.const 0))
      (i32.const 1024))

    (func (export "invoke") (param $ptr i32) (param $len i32) (result i32)
      (i32.store8 (i32.const 1040) (i32.const 0))
      (i32.store (i32.const 1044) (i32.const 128))
      (i32.store (i32.const 1048) (i32.const 18))
      (i32.const 1040))
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
