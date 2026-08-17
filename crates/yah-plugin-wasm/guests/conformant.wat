;; Fixture guest: activates cleanly and returns a fixed fixture-tool response.
;;
;; Written as component text so the canonical-ABI shape stays reviewable in a
;; diff; a checked-in `.wasm` would not be. Building this from a real language
;; toolchain would need a second Rust target in the gate container, which
;; belongs with the guest SDK work rather than here.
;;
;; Two ABI facts drive the shape below. Both results flatten past one core
;; value, so each core function returns a pointer into linear memory instead of
;; a scalar. And an exported instance may only refer to *named* types, so each
;; interface exports the enum and record its signatures mention, exactly as the
;; WIT interface declares them.
;;
;; This fixture imports nothing. The world's imports are linked by the host
;; and proved to link, but this guest never calls back; the flood and
;; capability-consumer fixtures are the corpus's import callers.

(component
  (core module $impl
    ;; Memory map, all disjoint: the response bytes at 128, a 16-byte return
    ;; area per export at 1024 and 1040, and the bump arena from 4096 up.
    (memory (export "memory") 1)
    (data (i32.const 128) "{\"activated\":true}")

    ;; Bump allocator. Nothing is ever freed: the host lifts values out and
    ;; drops the whole store, so reuse is not a concern for a fixture.
    ;;
    ;; It still has to honour the two things the canonical ABI requires of
    ;; `cabi_realloc`: the result must be aligned to `$align`, and it must lie
    ;; inside this memory. Exhaustion traps rather than returning an
    ;; out-of-range pointer, and `$bump` only advances once a request has been
    ;; accepted, so a refused request cannot strand the allocator.
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
      ;; Compare against the space that remains rather than `$ptr + $new_len`,
      ;; which would wrap for a large enough request and pass the check.
      (if (i32.or
            (i32.gt_u (local.get $ptr) (local.get $limit))
            (i32.gt_u (local.get $new_len)
                      (i32.sub (local.get $limit) (local.get $ptr))))
        (then unreachable))
      (global.set $bump (i32.add (local.get $ptr) (local.get $new_len)))
      (local.get $ptr))

    ;; result<_, guest-error> -> ok. Discriminant 0 at +0; payload unused.
    (func (export "activate") (result i32)
      (i32.store8 (i32.const 1024) (i32.const 0))
      (i32.const 1024))

    ;; result<string, guest-error> -> ok(s). Discriminant 0 at +0, then the
    ;; string pointer at +4 and its byte length at +8.
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
