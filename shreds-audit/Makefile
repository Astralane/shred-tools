# shred-audit — build helpers
#
#   make build     debug build of the collector
#   make release   optimized build (target/release/shred-audit)
#   make test      run the Rust test suite
#   make clean     cargo clean

CARGO ?= cargo

.PHONY: all build release test clean

all: release

build:
	$(CARGO) build

release:
	$(CARGO) build --release

test:
	$(CARGO) test

clean:
	$(CARGO) clean
