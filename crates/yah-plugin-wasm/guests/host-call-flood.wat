;; Fixture guest: floods the host-call byte budget from a small memory.
;;
;; The first fixture in this corpus that called back into the host; the
;; capability-consumer fixture now does too. The rest import nothing, so this
;; is where the byte-budget half of the guest-to-host path is entered.
;;
;; What it demonstrates is aliasing. The activation builds a 200-element list of
;; `log-field` records whose every key and value points at the SAME bytes - the
;; 22-byte string at offset 128, with the value length declared as 30,000. The
;; guest therefore spends 200 x 16 bytes of list plus one small string, well
;; inside one 64 KiB page, and asks the host to lift roughly six megabytes out
;; of it. That is why `WasmLimits::host_call_bytes` exists and why the memory
;; ceiling cannot stand in for it: a memory bound limits what the guest HOLDS,
;; and this bounds what one call COSTS.
;;
;; The two are proved as a pair - refused under a tight budget, admitted under a
;; generous one - so a failure is attributable to the budget rather than to the
;; fixture.
(component
  (type $logging-t (instance
    (type (enum "trace" "debug" "info" "warn" "error"))
    (export "log-level" (type (eq 0)))
    (type (record (field "key" string) (field "value" string)))
    (export "log-field" (type (eq 2)))
    (type (list 3))
    (export "log" (func (param "level" 1) (param "message" string) (param "fields" 4)))
  ))
  (import "yah:plugin/logging@0.1.0" (instance $logging (type $logging-t)))
  (alias export $logging "log" (func $log-f))

  (core module $mem (memory (export "memory") 1))
  (core instance $m (instantiate $mem))

  (core func $log (canon lower (func $log-f) (memory $m "memory") string-encoding=utf8))

  (core module $impl
    (import "mem" "memory" (memory 1))
    (import "host" "log" (func $log (param i32 i32 i32 i32 i32)))
    (data (i32.const 128) "hello-from-the-fixture")
    (global $bump (mut i32) (i32.const 40000))

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

    (func (export "activate") (result i32)
      (local $i i32)
      (local $p i32)
      (local.set $i (i32.const 0))
      (block $done
        (loop $loop
          (br_if $done (i32.ge_u (local.get $i) (i32.const 200)))
          (local.set $p (i32.add (i32.const 32768) (i32.mul (local.get $i) (i32.const 16))))
          (i32.store (local.get $p) (i32.const 128))
          (i32.store offset=4 (local.get $p) (i32.const 22))
          (i32.store offset=8 (local.get $p) (i32.const 128))
          (i32.store offset=12 (local.get $p) (i32.const 30000))
          (local.set $i (i32.add (local.get $i) (i32.const 1)))
          (br $loop)))
      (call $log (i32.const 2) (i32.const 128) (i32.const 22)
                 (i32.const 32768) (i32.const 200))
      (i32.store8 (i32.const 1024) (i32.const 0))
      (i32.const 1024))

    (func (export "invoke") (param $ptr i32) (param $len i32) (result i32)
      (i32.store8 (i32.const 1040) (i32.const 0))
      (i32.store (i32.const 1044) (i32.const 128))
      (i32.store (i32.const 1048) (i32.const 22))
      (i32.const 1040))
  )

  (core instance $i (instantiate $impl
    (with "mem" (instance (export "memory" (memory $m "memory"))))
    (with "host" (instance (export "log" (func $log))))))

  (type $lifecycle-code (enum "cancelled" "invalid-state" "failed"))
  (type $lifecycle-error (record (field "code" $lifecycle-code) (field "message" string)))
  (type $activate-result (result (error $lifecycle-error)))
  (type $activate-fn (func (result $activate-result)))
  (func $activate (type $activate-fn)
    (canon lift (core func $i "activate")
      (memory $m "memory") (realloc (func $i "cabi_realloc"))))
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
      (memory $m "memory") (realloc (func $i "cabi_realloc")) string-encoding=utf8))
  (instance $fixture-tool
    (export "error-code" (type $tool-code))
    (export "guest-error" (type $tool-error))
    (export "invoke" (func $invoke)))
  (export "yah:plugin/fixture-tool@0.1.0" (instance $fixture-tool))
)
