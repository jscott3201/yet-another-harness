;; Fixture guest: instantiates, then returns a `guest-error` from `activate`.
;;
;; The failure is a returned error rather than a trap, so the driver exercises
;; the error path the WIT actually declares. See `conformant.wat` for the ABI
;; and named-type rules both fixtures follow.

(component
  (core module $impl
    (memory (export "memory") 1)
    (data (i32.const 128) "fixture refused activation")

    ;; See `conformant.wat` for why this aligns, bounds-checks, and traps.
    (global $bump (mut i32) (i32.const 4096))

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

    ;; result<_, guest-error> -> err. Discriminant 1 at +0, then the record:
    ;; `code` at +4, and the message pointer and length at +8 and +12.
    ;; `failed` is the third enum case, so its discriminant is 2.
    (func (export "activate") (result i32)
      (i32.store8 (i32.const 1024) (i32.const 1))
      (i32.store8 (i32.const 1028) (i32.const 2))
      (i32.store (i32.const 1032) (i32.const 128))
      (i32.store (i32.const 1036) (i32.const 26))
      (i32.const 1024))

    ;; `result<string, guest-error>` err has the same shape. The 2 here is a
    ;; different enum: fixture-tool's cases are (invalid-input, cancelled,
    ;; failed), so `failed` is index 2 for its own reason, not lifecycle's.
    ;; Each return area is 16 bytes; this one starts at 1060 to stay clear of
    ;; the activate area at 1024.
    (func (export "invoke") (param $ptr i32) (param $len i32) (result i32)
      (i32.store8 (i32.const 1060) (i32.const 1))
      (i32.store8 (i32.const 1064) (i32.const 2))
      (i32.store (i32.const 1068) (i32.const 128))
      (i32.store (i32.const 1072) (i32.const 26))
      (i32.const 1060))
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
