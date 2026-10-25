use std::time::{Duration, Instant};

use crate::{db::{Entry, Store, Value}, util::now_ms};

enum Size {
    Len(usize),
    Encoded(u8),
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn u8(&mut self) -> Option<u8> {
        let b = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    fn len(&mut self) -> Option<usize> {
        let flag = self.u8()?;
        match flag >> 6 {
            // 00xxxxxx
            0b00 => Some((flag & 0x3F) as usize),
            // 01xxxxxx xxxxxx big endian
            0b01 => {
                let second = self.u8()?;
                let size = (((flag & 0x3F) as usize) << 8) | (second as usize);
                Some(size)
            },
            // 10xxxxxx + 32 bits
            0b10 => {
                let b = self.take(4)?;
                Some(u32::from_be_bytes([b[0],b[1],b[2],b[3]]) as usize)
            },
            _ => None,
        }
    }

    fn size(&mut self) -> Option<Size> {
        let first = self.u8()?;
        match first >> 6 {
            0b00 => Some(Size::Len((first & 0x3F) as usize)),
            0b01 => {
                let second = self.u8()?;
                let size = (((first & 0x3F) as usize) << 8) | (second as usize);
                Some(Size::Len(size))
            },
            0b10 => {
                let b = self.take(4)?;
                Some(Size::Len(u32::from_be_bytes([b[0], b[1],b[2],b[3]]) as usize))
            },
            _ => Some(Size::Encoded(first & 0x3F)),
        }
    }

    fn string(&mut self) -> Option<Vec<u8>> {
        match self.size()? {
            Size::Len(n) => Some(self.take(n)?.to_vec()),
            Size::Encoded(t) => match t {
                0 => Some((self.u8()? as i8).to_string().into_bytes()), // C0: int8
                1 => {
                    let b = self.take(2)?;
                    Some(i16::from_le_bytes([b[0],b[1]]).to_string().into_bytes())  //C1: int16 LE
                },
                2 => {
                    let b = self.take(4)?;
                    Some(i32::from_le_bytes([b[0], b[1], b[2], b[3]]).to_string().into_bytes())
                },
                _ => None,
            }
        }
    }

    fn u32_le(&mut self) -> Option<u32> {
        let b = self.take(4)?;
        Some(u32::from_le_bytes([b[0],b[1],b[2],b[3]]))
    }

    fn u64_le(&mut self) -> Option<u64> {
        let b = self.take(8)?;
        Some(u64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]]))
    }

    fn read_entry(&mut self, store: &mut Store, expire_at_ms:Option<u64>) -> Option<()>{
        let ty = self.u8()?;
        self.read_entry_with(store, ty, expire_at_ms)
    }

    fn read_entry_with(&mut self, store: &mut Store, ty:u8, expire_at_ms:Option<u64>) -> Option<()> {
        match ty {
            0x00 => {
                let key = self.string()?;
                let value = self.string()?;
                let expires_at = match expire_at_ms {
                    Some(ms) => {
                        let now = now_ms();
                        if ms <= now {
                            return Some(())
                        }
                        Instant::now().checked_add(Duration::from_millis(ms-now))
                    },
                    None => None
                };
                store.data.insert(key, Entry {
                    value: Value::String(value),
                    expires_at
                });
                Some(())
            },
            _ => {
                None
            }
        }
    }
}


pub fn load(bytes: &[u8], store: &mut Store) {
    let mut reader = Reader{ data: bytes, pos:0};
    reader.take(9); //skip 9 bytes
    loop { 
        let Some(op) = reader.u8() else {
            break;
        };
        match op {
            0xFF => break,
            0xFA => {
                // read metadata key and value
                let _name = reader.string();
                let _value = reader.string();
            },
            0xFE => {  // select db 0
                reader.len();
                // select db index
                
            },
            0xFB => {
                // len, len, hash size, key size
                reader.len();
                reader.len();
            },
            0xFC => {
                // read_entry, expire timeout in millis
                let ms = reader.u64_le();
                reader.read_entry(store, ms);
            },
            0xFD => {
                // read_entry, expire timeout in sec
                let ms = reader.u32_le().map(|sec|sec as u64*1000);
                reader.read_entry(store, ms);
            },
            ty => {
                reader.read_entry_with(store, ty, None);
            }
        }
    } 
}