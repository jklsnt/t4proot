# taproot
> a fast and customizable build system for notes

## usage
taproot will create a `notes.db` file on first run, then watch for file changes to progressively generate more html files as updates come along.

simply run it with `RUST_LOG=taproot=trace cargo +nightly run` and it'll start! Then serve whatever ends up in `out/`.
