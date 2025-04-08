# S3 Tests

Tests that use real S3 buckets.

Tests that use public buckets and don't require auth go in `public`. Tests that
use private buckets and require auth go in `private`.

## Bucket Configuration

- `glaredb-public`: `us-east-1`, public
- `glaredb-private`: `us-east-1`, public
- `glaredb-public-eu`: `eu-west-1`, private
- `glaredb-private-eu`: `eu-west-1`, private

TODO: CORS configuration. Definitely top-10 piece of technology
