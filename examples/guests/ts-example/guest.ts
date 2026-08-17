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
  // does. `JSON.stringify` is free here and a hand-rolled serialiser is not,
  // which is the one place the two examples differ in how rather than what.
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
    return JSON.stringify({ echo: JSON.parse(inputJson), from: 'typescript' });
  },
};
