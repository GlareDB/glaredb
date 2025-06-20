//! Protobuf stub crate.

// Previously we defined an interface in this crate for encoding/decoding
// to/from protobuf, however the resulted in a hard requirement on protoc, which
// is annoying.
//
// Next iteration should define an encoder/decoder trait in glaredb_core that
// does not depend on any serialization protocol so that protobuf usage and the
// build requirements for it are optional. However this trait can assume that
// the only real implementation will be for protobuf as it's unlikely to be
// worth trying to genericize over multiple protocols.
//
// We also should not try to write serde serializer/deserializers. Those are
// hell. Do not do it.
