# mmv-rs

This is a proof-of-concept native Rust implemention for writing MMV v1 files.

The included example shows how to create metrics, write them to an MMV file, and
update their values.

This crate was writen during the community bonding period of my [GSoC project](https://medium.com/@saurvs/gsoc-2017-introduction-834825fb2aee) in order to decide between a Rust-C FFI approach vs. a pure Rust approach with regards to writing an MMV file.