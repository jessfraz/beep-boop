# Builds and package publication

Run `make check` inside `nix develop`. It generates the shared bindings, installs
and builds the runtime, then runs the checks owned by the Makefile.
The exporter owns TypeScript's `.js` import extension without an environment
override.

CI prepares the runtime once before installing its pinned browser fixtures, then
uses `make check-prepared` to run that same verification recipe. This target
requires an already prepared runtime. Packaged Rust checks and the native Codex
and embedding checks still run on Linux and macOS.

Each matrix runner publishes only after all its qualification and package smoke
checks succeed. Publication is limited to pushes and manual runs on `main` in
`jessfraz/beep-boop`. Pull requests only read the public cache. Other refs do not
publish.

The existing read-only Cachix action installs the client without enabling
post-build uploads. The final explicit `cachix push` receives only the immutable
output from `nix build .#beep-boop`, uploading that package's runtime closure to
`jessfraz-global-nix`. It does not scan the store or publish host configurations.
This follows Cachix's [runtime closure publication](https://docs.cachix.org/pushing#pushing-runtime-closure)
and [read-only action](https://github.com/cachix/cachix-action/tree/38b082610b782e7e93e209c35fd730d399dee866#options)
contracts.

Trusted publication requires the repository secret `CACHIX_AUTH_TOKEN`, scoped
to writing this cache. Only the final publication step receives it. A missing
credential or failed upload fails the job instead of reporting a publication
success. Global Nix can update its application pin after both platform jobs and
the other required checks succeed. This workflow does not activate any host.
