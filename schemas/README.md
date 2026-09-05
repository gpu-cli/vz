# Versioned authoring schemas

`vz-project-definition-v1.schema.json` describes the JSON wire shape of the
current `vz_runtime_contract::ProjectDefinition`. It is a Draft 2020-12 schema.
The example is in `examples/developer-environment/vz.json`.

Validation has three layers:

1. The schema checks field shapes, required profiles, enum values, numeric wire
   bounds, and profile/target Docker restrictions. Unknown fields are rejected
   by both the schema and Rust deserializer, not silently discarded.
2. `ProjectDefinition::validate()` checks semantic relationships, including
   unique resource names, exact endpoint references, and UTF-8 byte-length
   bounds. Schema acceptance alone does not prove these relationships.
3. Runtime admission checks host×target support, exact verified artifacts,
   ownership, and actual resource/capability policy before effects.

The published schema represents implemented contract fields, not every planned
0.4 topology feature. Future secret, fault, host-import, peering, and other
declarations must land in the typed contract and schema together. No omitted or
unknown declaration grants access. This authoring bundle is DEV; packaging it
into the installed release and certifying the full workflow remain required.

Run the schema checks and the independent production-loader tests:

```sh
uv run --with jsonschema==4.23.0 python scripts/helpers/test_project_definition_schema.py
RUSTC_WRAPPER= cargo test --manifest-path crates/Cargo.toml -p vz-cli --test project_definition
RUSTC_WRAPPER= cargo test --manifest-path crates/Cargo.toml -p vz-runtime-contract --lib
```

These checks are prerequisites, not substitutes for installed public-CLI
bootstrap, lifecycle, or release-gate verification.
