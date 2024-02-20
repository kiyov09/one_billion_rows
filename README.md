# 1 billion row challenge in Rust

This is my attemp to solve the [1 billion row challenge](https://github.com/gunnarmorling/1brc) in Rust.

As the rules state, I'm not:

- Using any external dependencies.
- Relying on any specifics of the input file.
- Pre-processing the input file in any way, including constant evaluation.

# Before running the code

You need to generate the input file. You can do this by running:

```bash
make generate
```

Or, by running the `create_measurements.py` script in the `data` directory.

```bash
python3 create_measurements.py <number_of_rows_you_want>
```

# Running the code

As simple as running:

```bash
make run # or cargo run
```

Or, if you wan the release profile:

```bash
make run_release # or cargo run --release
```
