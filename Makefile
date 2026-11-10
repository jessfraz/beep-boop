.PHONY: bindings runtime build check check-prepared

bindings:
	cargo run --locked --bin export-protocol

runtime: bindings
	npm --prefix runtime ci --ignore-scripts
	npm --prefix runtime run build

build: runtime
	cargo build

check: runtime

check check-prepared:
	cargo fmt --check
	cargo clippy --all --benches --tests --examples --all-features -- -D warnings
	cargo build --example terminal-fixture
	cargo build --example browser_fixture
	cargo test
	npm --prefix runtime test
