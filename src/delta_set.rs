use crate::delta::{Delta, RawDelta};

use sled::IVec;
use std::ops::Deref;

/// A wrapper around a byte slice used for decoding a set of `Delta`s.
///
/// The on-disk encoding is a sequence of [RawDelta](crate::raw_delta::RawDelta).
#[derive(Clone)]
pub struct RawDeltaSet<B> {
    pub bytes: B,
}

impl<B> RawDeltaSet<B>
where
    B: Deref<Target = [u8]>,
{
    pub fn new(bytes: B) -> Self {
        Self { bytes }
    }

    pub fn iter_deltas(&self) -> RawDeltaIter<'_> {
        RawDeltaIter {
            bytes: &self.bytes,
            offset: 0,
        }
    }

    pub fn iter_deltas_into_ivecs(&self) -> impl '_ + Iterator<Item = Delta<IVec>> {
        self.iter_deltas()
            .map(|raw| Delta::from(&raw).map(|bytes| IVec::from(*bytes)))
    }
}

pub struct RawDeltaIter<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for RawDeltaIter<'a> {
    type Item = RawDelta<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.bytes.len() {
            return None;
        }
        debug_assert!(self.offset < self.bytes.len());
        let delta = RawDelta::new(&self.bytes[self.offset..]);
        self.offset += delta.range().len();
        Some(delta)
    }
}

// ████████╗███████╗███████╗████████╗
// ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝
//    ██║   █████╗  ███████╗   ██║
//    ██║   ██╔══╝  ╚════██║   ██║
//    ██║   ███████╗███████║   ██║
//    ╚═╝   ╚══════╝╚══════╝   ╚═╝
#[cfg(test)]
mod test {
    use super::*;

    use sled::IVec;

    #[test]
    fn deltas_encode_decode_round_trip() {
        let deltas = [
            Delta::Insert(IVec::from(b"key1"), IVec::from(b"value1")),
            Delta::Insert(IVec::from(b"key2"), IVec::from(b"value2")),
            Delta::Remove(IVec::from(b"key3")),
        ];

        let mut bytes = Vec::new();
        for delta in deltas.iter() {
            delta.encode(&mut bytes).unwrap();
        }

        let raw_deltas = RawDeltaSet::new(bytes.as_ref());
        let decoded_deltas: Vec<_> = raw_deltas
            .iter_deltas()
            .map(|d| Delta::from(&d).map(|b| IVec::from(*b)))
            .collect();

        assert_eq!(decoded_deltas, deltas);
    }
}
