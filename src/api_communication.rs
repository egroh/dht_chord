use bincode::config::{BigEndian, FixintEncoding, WithOtherEndian, WithOtherIntEncoding};
use bincode::{DefaultOptions, Options};

pub fn get_bincode_options(
) -> WithOtherEndian<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, BigEndian> {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
}
