use anyhow::{anyhow, Result};
use glob::Pattern;
use std::{collections::BTreeMap, path::Path};
use walkdir::WalkDir;

use crate::test::{Test, TestHooks};

pub use crate::test::{FnTest, Hook, TestClient, TestHook};

#[derive(Default)]
pub struct SltRunner {
    tests: BTreeMap<String, Test>,
    hooks: TestHooks,
}

impl SltRunner {
    pub fn new() -> Self {
        Default::default()
    }

    fn add_test(&mut self, name: String, test: Test) -> Result<()> {
        if self.tests.contains_key(&name) {
            return Err(anyhow!(
                "Test `{}` already exists: Unable to add {:?}",
                name,
                test
            ));
        }

        self.tests.insert(name, test);
        Ok(())
    }

    fn add_file_test(&mut self, prefix: &Path, file_path: &Path) -> Result<()> {
        validate_test_file(file_path)?;

        let test_name = file_path.strip_prefix(prefix)?.to_string_lossy();
        // Remove the ".slt" extension to get the test_name.
        let test_name = test_name.trim_end_matches(".slt");

        self.add_test(test_name.to_string(), Test::File(file_path.to_path_buf()))
    }

    pub fn test_files_dir(mut self, dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();

        if !dir.exists() {
            return Err(anyhow!("Path `{}` does not exist", dir.to_string_lossy()));
        }

        if !dir.is_dir() {
            return Err(anyhow!(
                "Path `{}` is not a valid directory",
                dir.to_string_lossy()
            ));
        }

        // Get the absolute path
        let dir = dir.canonicalize()?;

        let walker = WalkDir::new(dir.as_path())
            .into_iter()
            .filter_map(|entry| match entry {
                Ok(entry) if validate_test_file(entry.path()).is_ok() => {
                    Some(entry.path().to_path_buf())
                }
                _ => None,
            });

        for file_path in walker {
            self.add_file_test(dir.as_path(), &file_path)?;
        }

        Ok(self)
    }

    pub fn test<S>(mut self, test_name: S, test_fn: Box<dyn FnTest>) -> Result<Self>
    where
        S: ToString,
    {
        self.add_test(test_name.to_string(), Test::FnTest(test_fn))?;
        Ok(self)
    }

    pub fn hook<S>(mut self, regx: S, hook: TestHook) -> Result<Self>
    where
        S: AsRef<str>,
    {
        let pattern = Pattern::new(regx.as_ref())?;
        self.hooks.push((pattern, hook));
        Ok(self)
    }
}

fn validate_test_file(file_path: &Path) -> Result<()> {
    if !file_path.exists() {
        return Err(anyhow!(
            "Path `{}` does not exist",
            file_path.to_string_lossy()
        ));
    }

    if !file_path.is_file() {
        return Err(anyhow!(
            "Path `{}` is not a valid file",
            file_path.to_string_lossy()
        ));
    }

    match file_path.extension() {
        Some(ext) if ext.to_string_lossy().as_ref() == "slt" => {}
        _ => {
            return Err(anyhow!(
                "File `{}` doesn't have `.slt` extension",
                file_path.to_string_lossy()
            ))
        }
    }

    Ok(())
}
