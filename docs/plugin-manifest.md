# Plugin Manifest Contract

YAH's first plugin-host slice defines a strict data contract. It validates one
`yah-plugin.toml` document and immutable package-revision vocabulary; it does
not load code, verify packages, grant capabilities, or authorize service
namespaces.

## Manifest v1

The checked acceptance fixture is
[`crates/yah-plugin-host/tests/fixtures/yah-plugin.toml`](../crates/yah-plugin-host/tests/fixtures/yah-plugin.toml):

```toml
manifest_version = 1
id = "acme.issue-context"
version = "0.1.0"
sdk = ">=0.1.0, <0.2.0"

[entrypoint]
driver = "node-process"
path = "src/plugin.ts"

[services]
required = ["yah.context/v1", "yah.graph.read/v1"]
provided = ["acme.issue-context/v1"]

[capabilities]
requested = ["yah.network.connect/v1", "yah.secrets.read/v1"]
```

Every table rejects unknown fields. Duplicate TOML keys, repeated service or
capability declarations, unsupported manifest versions, and documents above
64 KiB fail before a manifest value is returned.

## Identities and versions

Package IDs use canonical lowercase ASCII dotted names:

```text
segment      = [a-z][a-z0-9]*( "-" [a-z0-9]+ )*
qualified-id = segment "." segment *( "." segment )
contract-id  = qualified-id "/v" major
major        = nonzero-digit *digit  # fits u32
```

Segments are at most 63 bytes, package IDs at most 128 bytes, and service or
capability contract IDs at most 160 bytes. Service and capability IDs are
different Rust types even though both carry an explicit contract major.

Plugin package versions are exact canonical SemVer values. SDK compatibility
is a separate, explicit, non-wildcard SemVer requirement. Manifest schema,
package, SDK, service, and capability versions are independent axes.

Syntactic ownership is not authority. A third-party package can write a
well-formed `yah.*` identity, but admission must reject a reserved namespace or
privileged service/capability claim that policy does not authorize.

## Entrypoints

`entrypoint` is one tagged value, so a separate driver and incompatible entry
cannot coexist:

| Driver | Manifest value | Meaning |
|---|---|---|
| Built-in Rust | `driver = "builtin-rust"` | Requests the statically linked lane; no factory can be named by the package |
| Wasm Component | `driver = "wasm-component"` plus `path` | Package-relative component path |
| Node process | `driver = "node-process"` plus `path` | Package-relative ESM/TypeScript entry path |
| Python process | `driver = "python-process"` plus `path` | Package-relative Python entry path |

Guest paths are portable ASCII slash paths. Absolute paths, option-like leading
`-`, backslashes, drive prefixes, empty components, `.` and `..` components,
controls, and paths above 256 bytes are rejected. Parsing does not prove that a
file exists or that archive extraction and symlinks stay inside a package;
package staging owns those checks.

A built-in declaration is not permission to execute linked code. A future host
must require out-of-band trusted provenance and match the exact admitted
revision to a static registration. Package-ID text alone is never sufficient.

## Requests, grants, and revisions

Required/provided services are compatibility and routing claims. Requested
capabilities express desired authority. Neither is an effective grant, and
neither authorizes a sensitive bind, publication, reserved namespace, secret,
network destination, filesystem path, or other host operation. The implemented
[capability broker](plugin-capabilities.md) accepts a separate trusted
activation-scoped snapshot whose exact grants must be a subset of these
requests. Policy, approval, and backend-enforceability evaluation remain outside
the broker.

The manifest cannot declare its own digest. A host-side staging boundary later
supplies a canonical `blake3:<64 lowercase hex>` package digest. YAH then forms
an exact revision identity from package ID, package version, and that supplied
digest. Constructing the value does not verify bytes or admit the package, and
configuration, grants, scope, provider assignments, and activation epochs are
not part of package revision identity.

The runtime-neutral [driver lifecycle](plugin-driver.md) is now a separate
implemented layer over these values. Package loading and verification,
configuration binding, production capability families, and production driver
backends remain later roadmap slices. The host-side driver conformance testkit
is implemented separately from this data contract. A
[provisional WIT world](wasm-plugin-contract.md) and a driver that executes
fixture components against it now exist, but a `wasm-component` entrypoint
path is still never resolved or loaded: component building and loading, WIT
capability resources, process IPC, sandbox enforcement, persistence, and
actual multi-backend equivalence remain later roadmap slices.
