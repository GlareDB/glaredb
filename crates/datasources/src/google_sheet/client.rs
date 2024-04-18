// TODO: wrap the google cloud console API or find a good third party client in rs
// there is: https://crates.io/crates/sheets but that means using OAuth
// OR curl https://sheets.googleapis.com/v4/spreadsheets/1f5epAPxP_Yd3g1TunEMdtianpVAhKS0RG6BKRDSLtrk\?key=<API_KEY>
// a small wrapper with reqwest might not be too involved

// see: https://developers.google.com/sheets/api/samples/reading
// see: https://developers.google.com/apis-explorer/#p/sheets/v4/

// stream over HTTP/JSON or re-use gRPC interface?
