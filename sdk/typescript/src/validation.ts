import Ajv2020, { type ValidateFunction } from "ajv/dist/2020.js";

import hostSchema from "../../../generated/worker-protocol/host.schema.json" with { type: "json" };
import workerSchema from "../../../generated/worker-protocol/worker.schema.json" with { type: "json" };
import type {
  HostMessage,
  WorkerMessage,
} from "../../../generated/worker-protocol/protocol.ts";
import { MAX_WIRE_UINT32 } from "../../../generated/worker-protocol/protocol.ts";

import { codecError } from "./errors.ts";

function compile<T>(schema: object): ValidateFunction<T> {
  const ajv = new Ajv2020({
    allErrors: false,
    coerceTypes: false,
    ownProperties: true,
    removeAdditional: false,
    strict: true,
    useDefaults: false,
    validateFormats: true,
  });
  ajv.addFormat("uint32", {
    type: "number",
    validate: (value: number) => Number.isInteger(value) && value >= 0 && value <= MAX_WIRE_UINT32,
  });
  ajv.addFormat("uint64", {
    type: "number",
    validate: (value: number) => Number.isSafeInteger(value) && value >= 0,
  });
  return ajv.compile<T>(schema);
}

const hostValidator = compile<HostMessage>(hostSchema);
const workerValidator = compile<WorkerMessage>(workerSchema);

export function assertHostMessage(value: unknown): asserts value is HostMessage {
  if (!hostValidator(value)) {
    throw codecError("INVALID_HOST_MESSAGE");
  }
}

export function assertWorkerMessage(value: unknown): asserts value is WorkerMessage {
  if (!workerValidator(value)) {
    throw codecError("INVALID_WORKER_MESSAGE");
  }
}
