use bincode::config::{BigEndian, FixintEncoding, WithOtherEndian, WithOtherIntEncoding};
use bincode::{DefaultOptions, Options};

pub fn with_big_endian(
) -> WithOtherEndian<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, BigEndian> {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
}
