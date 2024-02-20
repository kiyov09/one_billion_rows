generate:
	cd data && python3 create_measurements.py 50_000_000 && cd ..

build:
	cargo build

run:
	cargo run

release:
	cargo build --release

run_release:
	cargo run --release
