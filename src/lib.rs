#![feature(async_closure)]

//! Documentation and final report for the P2P DHT project.
//!
//! # Architecture:
//! All you need to build our package is the latest (nightly) version of the Rust compiler and Cargo.
//!
//! Installation instructions for Rust are provided [here](https://www.rust-lang.org/tools/install).
//!
//! For reference, we are currently working with the following versions:
//! - `cargo 1.74.0-nightly (96fe1c9e1 2023-08-29)`
//! - `rustc 1.74.0-nightly (35e416303 2023-09-01)`
//!
//! [This documentation](https://gitlab.lrz.de/netintum/teaching/p2psec_projects_2023/DHT-2/-/jobs/artifacts/master/file/target/doc/p2p_dht/index.html?job=documentation) was built with:
//! - `rustdoc 1.74.0-nightly (35e416303 2023-09-01)`
//!
//! # Main executable:
//! The main executable of this program `p2p_dht` takes one command line argument, `-c <config>`,
//! where `<config>` is the path to a configuration file.
//!
//! The configuration file is expected to be in the INI format. The following configuration options are available:
//! ```ini
//! [dht]
//! default_store_duration = 60     ; Default storage duration in seconds
//! max_store_duration = 600        ; Maximum storage duration in seconds
//! p2p_address = 127.0.0.1:8001    ; Address to listen on for P2P connections
//! api_address = 127.0.0.1:7401    ; Address to listen on for API connections
//! bootstrap_node = 127.0.0.1:8000 ; (Optional) Address of a bootstrap node
//! ```
//! To run our program, you may use:
//! ```bash
//! cargo run --release -- -c example_config_node_0.ini
//! ```
//! We support different levels of logging, with the default being `info`.
//! You can change the log level by setting the `RUST_LOG` environment variable to one of the following values:
//! - `error`
//! - `warn`
//! - `info`
//! - `debug`
//! - `trace`
//!
//! The latest `x86_64-unknown-linux-gnu` binary of our program is provided as an artifact on our [GitLab repository](https://gitlab.lrz.de/netintum/teaching/p2psec_projects_2023/DHT-2/-/jobs/artifacts/master/file/target/release/p2p_dht?job=build).
//!
//! Alternatively, you may choose to build our program yourself:
//! ## Compilation:
//! After cloning this repository, you may run the following command to build our program:
//! ```bash
//! cargo build --release
//! ```
//! This will create a binary in `target/release/p2p_dht`.
//!
//! You can also build this documentation locally:
//! ```bash
//! cargo doc --no-deps --document-private-items
//! ```
//! The documentation will be created in `target/doc/p2p_dht/index.html`.
//! ## Testing:
//! We provide a CI/CD pipeline that runs our tests on every commit.
//! You may find the latest results [here](https://gitlab.lrz.de/netintum/teaching/p2psec_projects_2023/DHT-2/-/pipelines/latest).
//!
//! Alternatively, you can run our test suit yourself:
//! ```bash
//! cargo test --release
//! ```
//!
//! # How to read this documentation:
//! Our documentation follows a "docs as code" approach,
//! and is generated directly out of our source code.
//! Whenever you see a module, struct or function or any other clickable link in this documentation,
//! you can use it to directly jump to description and explanation of the corresponding code.
//!
//! Below, you will find links to the modules of this crate,
//! which contain further details of our report:
#![doc(html_favicon_url = "https://www.net.in.tum.de/favicon.ico")]
#![doc(html_logo_url = "https://net.in.tum.de/Images/tum-logo-5d7bd62d.svg")]
#![allow(dead_code)]
pub mod api_communication;
pub mod chord;
