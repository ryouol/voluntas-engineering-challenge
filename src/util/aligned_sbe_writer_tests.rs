use super::*;

#[test]
fn invalid_chunk_does_not_truncate_an_existing_file() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("existing.sbe");
    std::fs::write(&path, b"preserve these bytes").unwrap();
    for chunk in [0, HEADER_LENGTH] {
        assert!(AlignedSbeWriter::create(&path, chunk).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"preserve these bytes");
    }
}

struct Encoded {
    length: usize,
    byte: u8,
    error: bool,
    mismatch: bool,
}
impl SbeEncode for Encoded {
    fn encoded_length_with_header(&self) -> anyhow::Result<usize> {
        Ok(self.length)
    }
    fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
        buf.fill(self.byte);
        anyhow::ensure!(!self.error, "deliberate partial encoding failure");
        Ok(self.length - usize::from(self.mismatch))
    }
}

#[test]
fn rotation_padding_and_failed_encoders_preserve_valid_records() {
    let mut bytes = Vec::new();
    let mut writer = AlignedSbeWriter::from_writer(&mut bytes, 24).unwrap();
    let message = |length, byte| Encoded {
        length,
        byte,
        error: false,
        mismatch: false,
    };
    writer.write_msg(&message(16, 1)).unwrap();
    writer.write_msg(&message(4, 2)).unwrap();
    for (error, mismatch) in [(true, false), (false, true)] {
        assert!(
            writer
                .write_msg(&Encoded {
                    length: 4,
                    byte: 9,
                    error,
                    mismatch
                })
                .is_err()
        );
    }
    assert!(writer.write_msg(&message(17, 9)).is_err());
    writer.write_msg(&message(4, 3)).unwrap();
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(bytes.len(), 48);
    assert_eq!(&bytes[..16], &[1; 16]);
    assert_eq!(&bytes[16..24], &[0; 8]);
    assert_eq!(&bytes[24..28], &[2; 4]);
    assert_eq!(&bytes[28..32], &[3; 4]);
    assert_eq!(&bytes[32..], &[0; 16]);
}
