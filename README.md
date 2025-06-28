# rmqtt-storage

<a href="https://github.com/rmqtt/rmqtt-storage/releases"><img alt="GitHub Release" src="https://img.shields.io/github/release/rmqtt/rmqtt-storage?color=brightgreen" /></a>
<a href="https://crates.io/crates/rmqtt-storage"><img alt="crates.io" src="https://img.shields.io/crates/v/rmqtt-storage" /></a>
<a href="https://docs.rs/rmqtt-storage"><img alt="Documentation" src="https://docs.rs/rmqtt-storage/badge.svg" /></a>


Is a simple wrapper around some key-value storages.


## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
rmqtt-storage = "0.5"
```

## Features

- Supports basic operations of key-value libraries.
- Supports Map data type and related operations.
- Supports List data type and related operations.
- Supports key expiration.
- Provides an implementation for 'sled'.
- Provides an implementation for 'redis'.
