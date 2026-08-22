export {
  DEFAULT_CEILINGS,
  DEFAULT_WIRE_LIMITS,
  FRAME_PREFIX_BYTES,
  MAX_WIRE_ID,
  MAX_WIRE_UINT32,
  PROTOCOL_VERSION,
  WIRE_FIELD_LIMITS,
} from "../../../generated/worker-protocol/protocol.ts";
export type {
  Accept,
  ArtifactOffer,
  Call,
  CallId,
  Cancel,
  CancelReason,
  CancelTarget,
  Ceilings,
  Credit,
  Goodbye,
  HandleId,
  HandleKind,
  Hello,
  HostMessage,
  JsonValue,
  Outcome,
  Refuse,
  Release,
  ReleaseAck,
  Reply,
  StreamClass,
  StreamData,
  StreamOpen,
  WireError,
  WireErrorKind,
  WireLimits,
  WorkerMessage,
} from "../../../generated/worker-protocol/protocol.ts";

export { decodeHostMessage, encodeWorkerMessage } from "./codec.ts";
export { WireCodecError, type WireCodecErrorCode } from "./errors.ts";
export { encodeFrame, FrameDecoder, type FrameEndOfInput } from "./framing.ts";
