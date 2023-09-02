#![feature(async_closure)]

//! Documentation and final report for the P2P DHT project.
//!
//! Our implementation is split into two core modules:
//! - [`api_communication`]
//! - [`chord`]
//!
//! The [`api_communication`] module
//!
//! The main executable of this program `p2p_dht` takes one command line argument, `-c <config>`,
//! where `<config>` is the path to a configuration file.
//! The configuration file is expected to be in the INI format. The following configuration options are available:
mod api_communication;
pub mod chord;
