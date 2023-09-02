# Final report

We aim to provide production quality documentation,
as it would be expected for a releasable crate.

As declared in our initial report,
we therefore provide a *browsable* documentation with `rustdoc`,
as it is customary for in the rust ecosystem.

Usually the documentation would be hosted alongside the crate on https://docs.rs/.
As this is a TUM internal project however,
you'll need to build our documentation yourself:

### Building binary & documentation:

1. Requirements:
    - cargo 1.74.0-nightly (96fe1c9e1 2023-08-29)
    - rustc 1.74.0-nightly (2f5df8a94 2023-08-31)
    - rustdoc 1.74.0-nightly (2f5df8a94 2023-08-31)
    - (At the time of writing, this is simply the latest nightly version)
    - If you wish to install this locally, we suggest that you use `rustup`
      (installable through your favourite package manager or directly from https://rustup.rs/):
        - `rustup toolchain install nightly`
        - `rustup default nightly`

2. Build commands:
    - `cargo build --release` will generate an executable binary in `target/release/`
    - `cargo doc --document-private-items` will generate the documentation for our crate
      in `target/doc/p2p_dht`.
    - As you will be interested in the inner workings of our crate, our documentation includes private items.
      The documentation is generated directly out of the project source code,
      ensuring it is kept up-to-date with the codebase.

3. Running the executable:
    - `cargo run --release -- -c ./config.ini` will build & run the executable binary
    - `-c ./config.ini` passes the configuration file, as required by the project specification

To access the rest of our documentation,
please open file://target/doc/p2p_dht/index.html in your favourite browser.
