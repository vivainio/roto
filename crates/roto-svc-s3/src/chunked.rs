//! Decoder for `aws-chunked` upload bodies (`STREAMING-*` payloads, optionally with trailers):
//! `<hex-size>[;chunk-signature=…]\r\n<data>\r\n … 0\r\n[trailer: value\r\n]\r\n`.
//! Signatures and trailing checksums are not verified.

pub fn is_aws_chunked(content_sha256: Option<&str>, content_encoding: Option<&str>) -> bool {
    content_sha256.is_some_and(|v| v.starts_with("STREAMING-"))
        || content_encoding.is_some_and(|v| v.split(',').any(|e| e.trim() == "aws-chunked"))
}

/// Returns the decoded payload, or `None` if the framing is malformed.
pub fn decode(body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(body.len());
    let mut rest = body;
    loop {
        let eol = find(rest, b"\r\n")?;
        let header = std::str::from_utf8(&rest[..eol]).ok()?;
        let size = usize::from_str_radix(header.split(';').next()?.trim(), 16).ok()?;
        rest = &rest[eol + 2..];
        if size == 0 {
            return Some(out); // anything after is trailers
        }
        if rest.len() < size + 2 || &rest[size..size + 2] != b"\r\n" {
            return None;
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_signed_chunks() {
        let body = b"5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=fff\r\n\r\n";
        assert_eq!(decode(body).unwrap(), b"hello world");
    }

    #[test]
    fn decodes_unsigned_with_trailer() {
        let body = b"3\r\nabc\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n";
        assert_eq!(decode(body).unwrap(), b"abc");
    }

    #[test]
    fn rejects_truncated_and_detects_encoding() {
        assert!(decode(b"5\r\nhel").is_none());
        assert!(is_aws_chunked(
            Some("STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            None
        ));
        assert!(is_aws_chunked(None, Some("aws-chunked")));
        assert!(!is_aws_chunked(Some("UNSIGNED-PAYLOAD"), None));
    }
}
