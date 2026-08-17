;; Fixture guest: provides the world's tool and consumes a brokered capability.
;;
;; `activate` acquires `example.text-echo/v1` once and keeps the own handle in
;; a core global; `invoke` calls the capability with the tool's input and
;; answers with whatever the provider said. Refusals are answered, not
;; trapped: an acquire error is reported as `A<code>` and a call error as
;; `E<code>`, each code the ASCII digit of the WIT enum discriminant, so a
;; host test reads the guest's own observation of the grant instead of
;; inferring it from the host side of the same call.
;;
;; The tool input doubles as a verb, dispatched on its first byte - the
;; corpus controls every input, so one byte is enough:
;;   `g…` grab:    acquire again and keep the new handle (the old one leaks,
;;                 deliberately - this is how the handle ceiling is reached)
;;   `a…` acquire: acquire again and drop the result at once, reporting only
;;                 the outcome ("acquired" or `A<code>`)
;;   `d…` drop:    resource.drop the held handle ("dropped", or "nohand")
;;   anything else: invoke the held handle with the whole input; with no
;;                 handle held, report the activate-time acquire code
;;
;; Structure follows `host-call-flood.wat`, with one addition it did not
;; need: `log` returns nothing, but `acquire` and `invoke` return strings, so
;; the lowered imports each need a realloc - which therefore lives beside the
;; memory, instantiated before any lowering, rather than inside `$impl`.
(component
  (type $capabilities-t (instance
    (export "capability" (type $cap (sub resource)))
    (type $acq-code (enum "invalid-id" "not-granted" "revoked" "unavailable"
                          "mismatched" "handle-limit"))
    (export "acquire-error-code" (type $acq-code-e (eq $acq-code)))
    (type $acq-err (record (field "code" $acq-code-e) (field "message" string)))
    (export "acquire-error" (type $acq-err-e (eq $acq-err)))
    (type $call-code (enum "revoked" "exhausted" "invalid-input" "failed"))
    (export "call-error-code" (type $call-code-e (eq $call-code)))
    (type $call-err (record (field "code" $call-code-e) (field "message" string)))
    (export "call-error" (type $call-err-e (eq $call-err)))
    (type $inv (func (param "self" (borrow $cap)) (param "input" string)
                     (result (result string (error $call-err-e)))))
    (export "[method]capability.invoke" (func (type $inv)))
    (type $acq (func (param "capability-id" string)
                     (result (result (own $cap) (error $acq-err-e)))))
    (export "acquire" (func (type $acq)))
  ))
  (import "yah:plugin/capabilities@0.1.0" (instance $caps (type $capabilities-t)))
  (alias export $caps "acquire" (func $acquire-f))
  (alias export $caps "[method]capability.invoke" (func $invoke-f))
  (alias export $caps "capability" (type $cap-t))

  ;; Memory and realloc together, so the lowered imports can name both.
  ;; The bump arena starts above every fixed region and nothing is freed; the
  ;; two canonical-ABI obligations are alignment and staying in bounds, and
  ;; exhaustion traps rather than handing back an out-of-range pointer.
  (core module $mem
    (memory (export "memory") 1)
    (global $bump (mut i32) (i32.const 4096))
    (func (export "cabi_realloc")
      (param $old_ptr i32) (param $old_len i32) (param $align i32) (param $new_len i32)
      (result i32)
      (local $ptr i32)
      (local.set $ptr
        (i32.and
          (i32.add (global.get $bump) (i32.sub (local.get $align) (i32.const 1)))
          (i32.xor (i32.sub (local.get $align) (i32.const 1)) (i32.const -1))))
      (if (i32.gt_u (i32.add (local.get $ptr) (local.get $new_len)) (i32.const 65536))
        (then unreachable))
      (global.set $bump (i32.add (local.get $ptr) (local.get $new_len)))
      (local.get $ptr))
  )
  (core instance $m (instantiate $mem))

  (core func $acquire (canon lower (func $acquire-f)
    (memory $m "memory") (realloc (func $m "cabi_realloc")) string-encoding=utf8))
  (core func $cap-invoke (canon lower (func $invoke-f)
    (memory $m "memory") (realloc (func $m "cabi_realloc")) string-encoding=utf8))
  (core func $cap-drop (canon resource.drop $cap-t))

  (core module $impl
    (import "mem" "memory" (memory 1))
    ;; Both results flatten past one core value, so each lowered import takes
    ;; a retptr: 16 bytes, discriminant u8 at +0, payload at +4. `acquire`'s
    ;; ok payload is the own handle; `invoke`'s is a string ptr/len pair; both
    ;; err payloads are code u8 at +4, message ptr at +8, len at +12.
    (import "host" "acquire" (func $acquire (param i32 i32 i32)))
    (import "host" "cap-invoke" (func $cap-invoke (param i32 i32 i32 i32)))
    (import "host" "cap-drop" (func $cap-drop (param i32)))

    ;; Memory map, all disjoint: fixed strings at 128..256, the 2-byte code
    ;; report at 512, 16-byte return areas at 1024/1040 and retptr areas at
    ;; 2048/3072, arena from 4096 (owned by `$mem`'s allocator).
    (data (i32.const 128) "example.text-echo/v1")
    (data (i32.const 160) "acquired")
    (data (i32.const 176) "grabbed")
    (data (i32.const 192) "dropped")
    (data (i32.const 208) "nohand")

    (global $handle (mut i32) (i32.const -1))
    (global $acq-code (mut i32) (i32.const -1))

    ;; "A3" / "E0": one prefix char, one ASCII digit, at 512.
    (func $render (param $prefix i32) (param $code i32) (result i32)
      (i32.store8 (i32.const 512) (local.get $prefix))
      (i32.store8 (i32.const 513) (i32.add (i32.const 48) (local.get $code)))
      (i32.const 512))

    ;; result<string, guest-error> -> ok(ptr, len) in the invoke return area.
    (func $answer (param $ptr i32) (param $len i32) (result i32)
      (i32.store8 (i32.const 1040) (i32.const 0))
      (i32.store (i32.const 1044) (local.get $ptr))
      (i32.store (i32.const 1048) (local.get $len))
      (i32.const 1040))

    (func $fresh-acquire
      (call $acquire (i32.const 128) (i32.const 20) (i32.const 2048)))

    ;; grab: keep the new handle; whatever was held before leaks on purpose.
    (func $grab (result i32)
      (call $fresh-acquire)
      (if (i32.eqz (i32.load8_u (i32.const 2048)))
        (then
          (global.set $handle (i32.load (i32.const 2052)))
          (return (call $answer (i32.const 176) (i32.const 7)))))
      (call $answer
        (call $render (i32.const 65) (i32.load8_u (i32.const 2052)))
        (i32.const 2)))

    ;; acquire: observe the outcome without changing what is held.
    (func $probe-acquire (result i32)
      (call $fresh-acquire)
      (if (i32.eqz (i32.load8_u (i32.const 2048)))
        (then
          (call $cap-drop (i32.load (i32.const 2052)))
          (return (call $answer (i32.const 160) (i32.const 8)))))
      (call $answer
        (call $render (i32.const 65) (i32.load8_u (i32.const 2052)))
        (i32.const 2)))

    (func $drop-held (result i32)
      (if (i32.eq (global.get $handle) (i32.const -1))
        (then (return (call $answer (i32.const 208) (i32.const 6)))))
      (call $cap-drop (global.get $handle))
      (global.set $handle (i32.const -1))
      (call $answer (i32.const 192) (i32.const 7)))

    ;; activate: acquire once, remember the handle or the refusal code, and
    ;; return ok either way - the refusal is the tool's answer to report, not
    ;; a reason to fail the activation that will report it.
    (func (export "activate") (result i32)
      (call $fresh-acquire)
      (if (i32.eqz (i32.load8_u (i32.const 2048)))
        (then (global.set $handle (i32.load (i32.const 2052))))
        (else (global.set $acq-code (i32.load8_u (i32.const 2052)))))
      (i32.store8 (i32.const 1024) (i32.const 0))
      (i32.const 1024))

    (func (export "invoke") (param $ptr i32) (param $len i32) (result i32)
      (local $b i32)
      (if (i32.gt_u (local.get $len) (i32.const 0))
        (then (local.set $b (i32.load8_u (local.get $ptr)))))
      (if (i32.eq (local.get $b) (i32.const 103))
        (then (return (call $grab))))
      (if (i32.eq (local.get $b) (i32.const 97))
        (then (return (call $probe-acquire))))
      (if (i32.eq (local.get $b) (i32.const 100))
        (then (return (call $drop-held))))
      (if (i32.eq (global.get $handle) (i32.const -1))
        (then (return (call $answer
          (call $render (i32.const 65) (global.get $acq-code))
          (i32.const 2)))))
      (call $cap-invoke (global.get $handle) (local.get $ptr) (local.get $len)
                        (i32.const 3072))
      (if (i32.eqz (i32.load8_u (i32.const 3072)))
        (then (return (call $answer
          (i32.load (i32.const 3076))
          (i32.load (i32.const 3080))))))
      (call $answer
        (call $render (i32.const 69) (i32.load8_u (i32.const 3076)))
        (i32.const 2)))
  )

  (core instance $i (instantiate $impl
    (with "mem" (instance (export "memory" (memory $m "memory"))))
    (with "host" (instance
      (export "acquire" (func $acquire))
      (export "cap-invoke" (func $cap-invoke))
      (export "cap-drop" (func $cap-drop))))))

  (type $lifecycle-code (enum "cancelled" "invalid-state" "failed"))
  (type $lifecycle-error (record (field "code" $lifecycle-code) (field "message" string)))
  (type $activate-result (result (error $lifecycle-error)))
  (type $activate-fn (func (result $activate-result)))
  (func $activate (type $activate-fn)
    (canon lift (core func $i "activate")
      (memory $m "memory") (realloc (func $m "cabi_realloc"))))
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
      (memory $m "memory") (realloc (func $m "cabi_realloc")) string-encoding=utf8))
  (instance $fixture-tool
    (export "error-code" (type $tool-code))
    (export "guest-error" (type $tool-error))
    (export "invoke" (func $invoke)))
  (export "yah:plugin/fixture-tool@0.1.0" (instance $fixture-tool))
)
