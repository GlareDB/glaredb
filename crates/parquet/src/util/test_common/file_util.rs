// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{env, fs, path::PathBuf};

fn get_parquet_data_dir() -> PathBuf {
    // Try env var first.
    if let Ok(dir) = env::var("PARQUET_TEST_DATA") {
        let path = PathBuf::from(dir);
        return path;
    }

    // Otherwise fall back to the default.
    const MANIFEST_DIR: &'static str = std::env!("CARGO_MANIFEST_DIR"); // Points to root of parquet crate.
    PathBuf::from(MANIFEST_DIR).join("../../submodules/parquet-testing/data")
}

/// Returns path to the test parquet file in 'data' directory
pub fn get_test_path(file_name: &str) -> PathBuf {
    get_parquet_data_dir().join(file_name)
}

/// Returns file handle for a test parquet file from 'data' directory
pub fn get_test_file(file_name: &str) -> fs::File {
    let path = get_test_path(file_name);
    fs::File::open(path.as_path()).unwrap_or_else(|err| {
        panic!(
            "Test file {} could not be opened, did you do `git submodule update`?: {}",
            path.display(),
            err
        )
    })
}
