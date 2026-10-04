//! The subset of crc-fast's API that object_store uses: a CRC-64/NVME digest
//! (reflected polynomial 0x9A6C9329AC4BC9B5, initial value and final XOR all
//! ones), table-driven.

/// CRC algorithms (only the one object_store uses).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrcAlgorithm {
    Crc64Nvme,
}

const POLY: u64 = 0x9A6C_9329_AC4B_C9B5;

const TABLE: [u64; 256] = {
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// An incremental CRC computation.
#[derive(Debug, Clone)]
pub struct Digest {
    crc: u64,
}

impl Digest {
    pub fn new(algorithm: CrcAlgorithm) -> Self {
        match algorithm {
            CrcAlgorithm::Crc64Nvme => Self { crc: u64::MAX },
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.crc;
        for &byte in data {
            crc = TABLE[((crc ^ byte as u64) & 0xFF) as usize] ^ (crc >> 8);
        }
        self.crc = crc;
    }

    pub fn finalize(&self) -> u64 {
        self.crc ^ u64::MAX
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_nvme_check_value() {
        let mut digest = Digest::new(CrcAlgorithm::Crc64Nvme);
        digest.update(b"1234");
        digest.update(b"56789");
        assert_eq!(digest.finalize(), 0xAE8B_1486_0A79_9888);
    }

    #[test]
    fn empty_input() {
        assert_eq!(Digest::new(CrcAlgorithm::Crc64Nvme).finalize(), 0);
    }
}
