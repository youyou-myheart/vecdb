# Native ANN dependency

Vendored headers: [nmslib/hnswlib v0.8.0](https://github.com/nmslib/hnswlib/tree/v0.8.0), Apache-2.0 (see `LICENSE`). No runtime download is required.

Local changes in `hnswalg.h`: preserve the upstream virtual `searchKnn` entry, add `searchKnnQuery` with request-local ef and distance count, and thread the local counter through base-layer search. The wrapper counts actual distance-function evaluations without changing graph construction or persisted layout. Searches no longer reset shared counters or call `setEf`.

Requires CGO and a C++14 compiler. Native searches share a read lock; mutation/save/load take its exclusive lock. The upstream visited-list pool is thread-safe. `tests/native_stress.cc` exercises eight readers and a writer with ThreadSanitizer (`bash scripts/native-check.sh`); Go race tests additionally cover service state. Graph memory is native and is not included in Go B/op or Go heap metrics. Rebuild explicitly releases replaced/discarded graphs; finalizers cover other abandoned handles. Close requires exclusive ownership.

Snapshot format is pinned to 64-bit little-endian hnswlib 0.8.0. The vector module validates structure and bounds before calling `Import`; raw untrusted native bytes must not be passed directly to it. Snapshot portability across library versions/architectures is not guaranteed; rebuild from authoritative records when migrating.

On AArch64, squared L2 uses NEON for four or more coordinates and handles any remaining coordinates with a scalar loop. The HNSW construction and search parameters are shared with the other architectures.

Graph journal support adds mutation hooks for newly inserted labels and captures
reversible images of new or changed nodes, entry point, levels, capacity and RNG state.
`FinishPatch` rolls construction back; `ApplyPatch` installs images directly.
The owning Go index excludes queries throughout capture/rollback. Existing-label
native updates are rejected during capture. Snapshot metadata separately preserves
both RNG engines, and loading an empty graph preserves its configured capacity.
Native patch errors conservatively poison the owning replica until recovery.
