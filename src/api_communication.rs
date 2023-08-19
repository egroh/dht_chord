use bincode::config::{BigEndian, FixintEncoding, WithOtherEndian, WithOtherIntEncoding};
use bincode::{DefaultOptions, Options};

// Code not dead, but linter thinks so
#[allow(dead_code)]
pub fn with_big_endian(
) -> WithOtherEndian<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, BigEndian> {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
}
