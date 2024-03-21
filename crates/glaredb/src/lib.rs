use std::collections::HashMap;

use derive_builder::Builder;

#[derive(Default, Builder)]
pub struct ConnectOptions {
    #[builder(setter(into, strip_option))]
    pub location: Option<String>,
    #[builder(setter(into, strip_option))]
    pub spill_path: Option<String>,
    pub disable_tls: Option<bool>,
    #[builder(setter(strip_option))]
    pub storage_options: HashMap<String, String>,
    #[builder(default = "Some(\"https://console.glaredb.com\".to_string())")]
    #[builder(setter(into, strip_option))]
    pub cloud_addr: Option<String>,
}

impl ConnectOptionsBuilder {
    pub fn set_storage_option(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> &mut Self {
        let mut opts = match self.storage_options.to_owned() {
            Some(opts) => opts,
            None => HashMap::new(),
        };
        opts.insert(key.into(), value.into());
        self.storage_options(opts)
    }
}
