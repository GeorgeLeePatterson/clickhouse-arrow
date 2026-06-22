//! Internal SIMD-oriented helpers for Arrow hot paths.

/// Expand Arrow's packed validity bitmap into `ClickHouse` null bytes.
///
/// Arrow validity bitmap: `1 = valid`, `0 = null`.
/// `ClickHouse` null map: `0 = valid`, `1 = null`.
///
/// `len` is the number of values to expand (not the number of bitmap bytes).
#[inline]
pub(crate) fn expand_null_bitmap(bitmap: &[u8], output: &mut [u8], len: usize) {
    debug_assert!(output.len() >= len);
    debug_assert!(bitmap.len() >= len.div_ceil(8));

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: runtime feature detection guarantees AVX2 availability.
            unsafe { expand_null_bitmap_avx2(bitmap, output, len) };
            return;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: runtime feature detection guarantees NEON availability.
            unsafe { expand_null_bitmap_neon(bitmap, output, len) };
            return;
        }
    }

    expand_null_bitmap_scalar(bitmap, output, len);
}

#[inline]
fn expand_null_bitmap_scalar(bitmap: &[u8], output: &mut [u8], len: usize) {
    let full_bytes = len / 8;
    let remainder = len % 8;

    for (byte_idx, &byte) in bitmap.iter().take(full_bytes).enumerate() {
        let base = byte_idx * 8;
        output[base] = u8::from((byte & 0x01) == 0);
        output[base + 1] = u8::from((byte & 0x02) == 0);
        output[base + 2] = u8::from((byte & 0x04) == 0);
        output[base + 3] = u8::from((byte & 0x08) == 0);
        output[base + 4] = u8::from((byte & 0x10) == 0);
        output[base + 5] = u8::from((byte & 0x20) == 0);
        output[base + 6] = u8::from((byte & 0x40) == 0);
        output[base + 7] = u8::from((byte & 0x80) == 0);
    }

    if remainder > 0 {
        let byte = bitmap[full_bytes];
        let base = full_bytes * 8;
        for bit in 0..remainder {
            output[base + bit] = u8::from((byte & (1 << bit)) == 0);
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn expand_null_bitmap_avx2(bitmap: &[u8], output: &mut [u8], len: usize) {
    // The unrolled loop shape improves auto-vectorization and keeps branchless bit extraction.
    let full_chunks = len / 32;
    let mut out_idx = 0;

    for chunk in 0..full_chunks {
        let bitmap_offset = chunk * 4;
        let b0 = bitmap[bitmap_offset];
        let b1 = bitmap[bitmap_offset + 1];
        let b2 = bitmap[bitmap_offset + 2];
        let b3 = bitmap[bitmap_offset + 3];

        write_expanded_byte(b0, &mut output[out_idx..out_idx + 8]);
        out_idx += 8;
        write_expanded_byte(b1, &mut output[out_idx..out_idx + 8]);
        out_idx += 8;
        write_expanded_byte(b2, &mut output[out_idx..out_idx + 8]);
        out_idx += 8;
        write_expanded_byte(b3, &mut output[out_idx..out_idx + 8]);
        out_idx += 8;
    }

    let remaining = len - (full_chunks * 32);
    if remaining > 0 {
        expand_null_bitmap_scalar(&bitmap[full_chunks * 4..], &mut output[out_idx..], remaining);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn write_expanded_byte(byte: u8, out: &mut [u8]) {
    out[0] = u8::from((byte & 0x01) == 0);
    out[1] = u8::from((byte & 0x02) == 0);
    out[2] = u8::from((byte & 0x04) == 0);
    out[3] = u8::from((byte & 0x08) == 0);
    out[4] = u8::from((byte & 0x10) == 0);
    out[5] = u8::from((byte & 0x20) == 0);
    out[6] = u8::from((byte & 0x40) == 0);
    out[7] = u8::from((byte & 0x80) == 0);
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn expand_null_bitmap_neon(bitmap: &[u8], output: &mut [u8], len: usize) {
    use std::arch::aarch64::{vand_u8, vceqz_u8, vdup_n_u8, vld1_u8, vst1_u8};

    let full_chunks = len / 32;
    let mut out_idx = 0;
    let bit_mask = unsafe { vld1_u8([0x01u8, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80].as_ptr()) };
    let ones = vdup_n_u8(0x01);

    for chunk in 0..full_chunks {
        let bitmap_offset = chunk * 4;
        for i in 0..4 {
            let byte = bitmap[bitmap_offset + i];
            let byte_vec = vdup_n_u8(byte);
            let bits = vand_u8(byte_vec, bit_mask);
            let is_zero = vceqz_u8(bits);
            let result = vand_u8(is_zero, ones);
            unsafe { vst1_u8(output.as_mut_ptr().add(out_idx), result) };
            out_idx += 8;
        }
    }

    let remaining = len - (full_chunks * 32);
    if remaining > 0 {
        expand_null_bitmap_scalar(&bitmap[full_chunks * 4..], &mut output[out_idx..], remaining);
    }
}

#[cfg(test)]
mod tests {
    use super::expand_null_bitmap;

    #[test]
    fn test_expand_null_bitmap_all_valid() {
        let bitmap = [0xFF, 0xFF];
        let mut out = vec![9; 16];
        expand_null_bitmap(&bitmap, &mut out, 16);
        assert_eq!(out, vec![0; 16]);
    }

    #[test]
    fn test_expand_null_bitmap_all_null() {
        let bitmap = [0x00, 0x00];
        let mut out = vec![9; 16];
        expand_null_bitmap(&bitmap, &mut out, 16);
        assert_eq!(out, vec![1; 16]);
    }

    #[test]
    fn test_expand_null_bitmap_partial_len() {
        let bitmap = [0b0001_0111];
        let mut out = vec![9; 5];
        expand_null_bitmap(&bitmap, &mut out, 5);
        assert_eq!(out, vec![0, 0, 0, 1, 0]);
    }
}
