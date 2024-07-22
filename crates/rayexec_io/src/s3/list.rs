use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct S3ListResponse {
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub contents: Vec<S3ListContents>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct S3ListContents {
    pub key: String,
    pub last_modified: DateTime<Utc>,
    pub size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_no_continuation() {
        let input = r#"
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Name>bucket</Name>
              <Prefix/>
              <KeyCount>205</KeyCount>
              <MaxKeys>1000</MaxKeys>
              <IsTruncated>false</IsTruncated>
              <Contents>
                  <Key>my-image.jpg</Key>
                  <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                  <ETag>"fba9dede5f27731c9771645a39863328"</ETag>
                  <Size>434234</Size>
                  <StorageClass>STANDARD</StorageClass>
              </Contents>
          </ListBucketResult>
        "#;

        let resp: S3ListResponse = quick_xml::de::from_str(input).unwrap();

        let expected = S3ListResponse {
            is_truncated: false,
            next_continuation_token: None,
            contents: vec![S3ListContents {
                key: "my-image.jpg".to_string(),
                last_modified: DateTime::parse_from_rfc3339("2009-10-12T17:50:30.000Z")
                    .unwrap()
                    .to_utc(),
                size: 434234,
            }],
        };

        assert_eq!(expected, resp);
    }

    #[test]
    fn basic_with_continuation() {
        let input = r#"
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Name>bucket</Name>
              <Prefix/>
              <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
              <KeyCount>205</KeyCount>
              <MaxKeys>1000</MaxKeys>
              <IsTruncated>true</IsTruncated>
              <Contents>
                  <Key>my-image.jpg</Key>
                  <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                  <ETag>"fba9dede5f27731c9771645a39863328"</ETag>
                  <Size>434234</Size>
                  <StorageClass>STANDARD</StorageClass>
              </Contents>
          </ListBucketResult>
        "#;

        let resp: S3ListResponse = quick_xml::de::from_str(input).unwrap();

        let expected = S3ListResponse {
            is_truncated: true,
            next_continuation_token: Some(
                "1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=".to_string(),
            ),
            contents: vec![S3ListContents {
                key: "my-image.jpg".to_string(),
                last_modified: DateTime::parse_from_rfc3339("2009-10-12T17:50:30.000Z")
                    .unwrap()
                    .to_utc(),
                size: 434234,
            }],
        };

        assert_eq!(expected, resp);
    }
}
