from rust:buster

run apt-get update && apt-get install vim -y

run rustup default nightly-2021-01-31

run rustup component add --toolchain nightly-2021-01-31 rustfmt
