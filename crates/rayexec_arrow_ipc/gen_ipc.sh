#!/usr/bin/env bash

# Generate rust code from flatbuffers definitions in the arrow submodule.
#
# Adapted from https://github.com/apache/arrow-rs/blob/master/arrow-ipc/regen.sh

set -eux -o pipefail

pushd "$(git rev-parse --show-toplevel)"

flatc --filename-suffix "" --rust -o crates/rayexec_arrow_ipc/src/ submodules/arrow/format/*.fbs

pushd crates/rayexec_arrow_ipc/src/

# Common prefix content for all generated files.
PREFIX=$(cat <<'HEREDOC'
use std::{cmp::Ordering, mem};
use flatbuffers::EndianScalar;

HEREDOC
)

SCHEMA_IMPORT="use crate::Schema::*;"
SPARSE_TENSOR_IMPORT="use crate::SparseTensor::*;"
TENSOR_IMPORT="use crate::Tensor::*;"

# For flatbuffer(1.12.0+), remove: use crate::${name}::\*;
names=("File" "Message" "Schema" "SparseTensor" "Tensor")

for file in *.rs; do
    if [ "$file" == "lib.rs" ]; then
        continue
    fi

    echo "Modifying file: $file"

    # Remove unnecessary module nesting, and duplicated imports.
    sed -i \
        -e '/extern crate flatbuffers;/d' \
        -e '/use self::flatbuffers::EndianScalar;/d' \
        -e '/\#\[allow(unused_imports, dead_code)\]/d' \
        -e '/pub mod org {/d' \
        -e '/pub mod apache {/d' \
        -e '/pub mod arrow {/d' \
        -e '/pub mod flatbuf {/d' \
        -e '/}  \/\/ pub mod flatbuf/d' \
        -e '/}  \/\/ pub mod arrow/d' \
        -e '/}  \/\/ pub mod apache/d' \
        -e '/}  \/\/ pub mod org/d' \
        -e '/use core::mem;/d' \
        -e '/use core::cmp::Ordering;/d' \
        -e '/use self::flatbuffers::{EndianScalar, Follow};/d' \
        "$file"

    for name in "${names[@]}"; do
        sed -i \
            -e "/use crate::${name}::\*;/d" \
            -e"s/use self::flatbuffers::Verifiable;/use flatbuffers::Verifiable;/g" \
            "$file"
    done

    if [ "$file" == "File.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" | cat - "$file" > temp && mv temp "$file"
    elif [ "$file" == "Message.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" "$SPARSE_TENSOR_IMPORT" "$TENSOR_IMPORT" | cat - "$file" > temp && mv temp "$file"
    elif [ "$file" == "SparseTensor.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" "$TENSOR_IMPORT" | cat - "$file" > temp && mv temp "$file"
    elif [ "$file" == "Tensor.rs" ]; then
        echo "$PREFIX" "$SCHEMA_IMPORT" | cat - "$file" > temp && mv temp "$file"
    else
        echo "$PREFIX" | cat - "$file" > temp && mv temp "$file"
    fi
done

popd
cargo fmt -- crates/rayexec_arrow_ipc/src/*
