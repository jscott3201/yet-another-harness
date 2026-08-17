// Example guest plugin for the `yah:plugin@0.1.0` conformance world.
//
// Its Rust counterpart in ../rust-example answers identically. That is the
// point of the pair: the world is the contract, and neither toolchain is
// privileged by it.

// @ts-expect-error - resolved by the component linker, not by a package.
import { log } from 'yah:plugin/logging@0.1.0';
// @ts-expect-error - resolved by the component linker, not by a package.
import { isCancelled } from 'yah:plugin/cancellation@0.1.0';

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
    log('debug', 'typescript example invoked', [
      { key: 'bytes', value: String(inputJson.length) },
    ]);
    return '{"echo":' + inputJson + ',"from":"typescript"}';
  },
};
