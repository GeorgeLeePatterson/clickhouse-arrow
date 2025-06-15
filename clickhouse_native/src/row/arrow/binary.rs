macro_rules! binary {
    // Infallible aka panic
    (String => $reader:expr) => {
        String::from_utf8_lossy(&$reader.try_get_string()?).as_ref()
    };
    (Binary => $reader:expr) => {{ $reader.try_get_string()? }};
    // TODO: Perhaps serde_json deserialization should be behind feature flag due to overhead
    (Object => $reader:expr) => {{
        if cfg!(feature = "serde") {
            let byts = $reader.try_get_string()?;
            let value = String::from_utf8_lossy(&byts);
            match serde_json::from_str::<serde_json::Value>(value.as_ref()) {
                Ok(val) => val.to_string(),
                Err(_) => value.to_string(),
            }
        } else {
            String::from_utf8_lossy(&$reader.try_get_string()?).to_string()
        }
    }};
    (FixedBinary($n:expr) => $reader:expr) => {{
        {
            let mut buf = vec![0u8; $n];
            $reader.try_copy_to_slice(&mut buf)?;
            buf
        }
    }};
    (Fixed($n:expr) => $reader:expr) => {{
        {
            let mut buf = [0u8; $n];
            $reader.try_copy_to_slice(&mut buf)?;
            buf
        }
    }};
    (FixedRev($n:expr) => $reader:expr) => {{
        {
            let mut buf = [0u8; $n];
            $reader.try_copy_to_slice(&mut buf)?;
            buf.reverse();
            buf
        }
    }};
    (Ipv4 => $reader:expr) => {{
        {
            let ipv4_int = $reader.try_get_u32_le()?;
            let ip_addr = ::std::net::Ipv4Addr::from(ipv4_int);
            ip_addr.octets()
        }
    }};
    (Ipv6 => $reader:expr) => {{
        {
            let mut octets = [0u8; 16];
            $reader.try_copy_to_slice(&mut octets[..])?;
            std::net::Ipv6Addr::from(octets).octets()
        }
    }};
}
pub(super) use binary;
