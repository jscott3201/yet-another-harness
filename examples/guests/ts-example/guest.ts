// Example guest plugin for the `yah:plugin@0.1.0` conformance world.
//
// Its Rust counterpart in ../rust-example answers identically. That is the
// point of the pair: the world is the contract, and neither toolchain is
// privileged by it.
//
// It calls every import the world offers: logging and cancellation on each
// request, and `capabilities` for `cap:`-prefixed ones, where it acquires the
// brokered capability, invokes it, and disposes the handle itself - so the
// resource half of the world is entered by authored code under a real
// toolchain, not only by the hand-written fixture.

// @ts-expect-error - resolved by the component linker, not by a package.
import { log } from 'yah:plugin/logging@0.1.0';
// @ts-expect-error - resolved by the component linker, not by a package.
import { isCancelled } from 'yah:plugin/cancellation@0.1.0';
// @ts-expect-error - resolved by the component linker, not by a package.
import { acquire } from 'yah:plugin/capabilities@0.1.0';

// The capability both example guests consume, granted or not by the test rig.
const CAPABILITY_ID = 'example.text-echo/v1';

// What the generated bindings throw when the host answers a WIT `result` with
// its error case: the record rides on `payload`, and the enum case arrives as
// its kebab-case WIT name. The Rust guest has to spell that mapping by hand;
// here it is the wire format.
type BrokerError = { payload: { code: string; message: string } };

export const lifecycle = {
  activate(): void {
    log('info', 'typescript example activated', []);
  },
};

export const fixtureTool = {
  // Echo the request back inside a fixed envelope, exactly as the Rust guest
  // does — by concatenation, without parsing.
  //
  // `JSON.parse` then `JSON.stringify` would be the idiomatic thing to write
  // here and it is wrong for this example, because it makes the two guests
  // answer differently. Round-tripping through JavaScript normalises
  // whitespace, rewrites `1.0` as `1`, and quietly rounds any integer past 2^53
  // to a double; and on input that is not JSON at all it throws, which crosses
  // the component boundary as a trap rather than as the `invalid-input` this
  // world declares. None of that is visible until someone sends something other
  // than canonical, small-number JSON.
  //
  // The world says `input-json`, and neither this contract nor the host parses
  // it. Treating it as an opaque string is what the contract actually promises,
  // and it is what lets the pair claim the same answer for every input rather
  // than for the convenient one.
  invoke(inputJson: string): string {
    // Cancellation is advisory and read-only, so a guest that means to be
    // interruptible has to ask. Host teardown does not depend on the answer.
    if (isCancelled()) {
      throw { code: 'cancelled', message: 'host asked the guest to stop' };
    }
    if (inputJson.length === 0) {
      throw { code: 'invalid-input', message: 'input-json was empty' };
    }
    // UTF-8 bytes, not `inputJson.length`. A JavaScript string's length is in
    // UTF-16 code units, so `.length` disagrees with the Rust guest's
    // `input_json.len()` on anything outside ASCII - 15 against 18 for
    // `{"s":"café 😀"}` - and the host would be reading two different
    // quantities under one field name. The world moves UTF-8; so does this.
    log('debug', 'typescript example invoked', [
      { key: 'bytes', value: String(new TextEncoder().encode(inputJson).length) },
    ]);
    if (inputJson.startsWith('cap:')) {
      return answerThroughCapability(inputJson.slice('cap:'.length));
    }
    return '{"echo":' + inputJson + ',"from":"typescript"}';
  },
};

// Acquire the capability, invoke it once, and answer - never trap.
//
// A refusal is an answer, not a thrown error: the broker's decision already
// rides in the error record, and rethrowing it would cross the boundary as a
// guest failure rather than as the answer the Rust guest gives.
//
// The `finally` dispose is load-bearing, not tidiness. The host counts live
// handles, and this engine never garbage-collects them - a probe holding two
// hundred undisposed handles under allocation pressure released none - so a
// guest that skips this leaks against the broker's handle limit. Exactly once,
// too: a second dispose of the same handle traps. `using cap = acquire(...)`
// is this same call as scope-exit sugar; it stays spelled out here because the
// release is part of what this example exists to show.
function answerThroughCapability(request: string): string {
  let capability;
  try {
    capability = acquire(CAPABILITY_ID);
  } catch (refusal) {
    const code = (refusal as BrokerError).payload.code;
    return '{"capability-refused":"' + code + '","from":"typescript"}';
  }
  try {
    return '{"capability":"' + capability.invoke(request) + '","from":"typescript"}';
  } catch (failure) {
    const code = (failure as BrokerError).payload.code;
    return '{"capability-failed":"' + code + '","from":"typescript"}';
  } finally {
    capability[Symbol.dispose]();
  }
}
