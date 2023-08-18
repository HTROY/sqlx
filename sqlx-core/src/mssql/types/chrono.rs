use byteorder::{BigEndian, ByteOrder, LittleEndian};
use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::mssql::protocol::type_info::{DataType, TypeInfo};
use crate::mssql::{Mssql, MssqlTypeInfo, MssqlValueRef};
use crate::types::Type;

impl Type<Mssql> for NaiveTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::TimeN, 8))
    }
}

impl Type<Mssql> for NaiveDate {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::DateTime, 8))
    }
}

impl Type<Mssql> for NaiveDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::DateTimeN, 8))
    }

    fn compatible(ty: &<Mssql as crate::database::Database>::TypeInfo) -> bool {
        matches!(
            ty.0.ty,
            DataType::DateTime | DataType::DateTimeN | DataType::DateTime2N
        )
    }
}
/*
impl Type<Mssql> for NaiveDate {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::DateTimeN, 8))
    }
}
impl Type<Mssql> for NaiveDateTime {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::DateTimeN, 8))
    }
}
*/
impl<Tz: TimeZone> Type<Mssql> for DateTime<Tz> {
    fn type_info() -> MssqlTypeInfo {
        MssqlTypeInfo(TypeInfo::new(DataType::DateTimeOffsetN, 8))
    }
}

impl Encode<'_, Mssql> for NaiveTime {
    fn encode_by_ref(&self, _buf: &mut Vec<u8>) -> IsNull {
        todo!()
    }
}

impl<'r> Decode<'r, Mssql> for NaiveTime {
    fn decode(_value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        todo!()
    }
}

impl Encode<'_, Mssql> for NaiveDate {
    fn encode_by_ref(&self, _buf: &mut Vec<u8>) -> IsNull {
        todo!()
    }
}

impl<'r> Decode<'r, Mssql> for NaiveDate {
    fn decode(_value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        todo!()
    }
}

impl Encode<'_, Mssql> for NaiveDateTime {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> IsNull {
        let days_duration = self.date() - NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
        let ms_duration = self.time() - NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let days = days_duration.num_days() as i32;
        let ms = ms_duration.num_milliseconds() as u32 * 3 / 10;

        buf.extend(&days.to_le_bytes());
        buf.extend_from_slice(&ms.to_le_bytes());
        IsNull::No
    }
}

impl<'r> Decode<'r, Mssql> for NaiveDateTime {
    fn decode(value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.type_info.0.ty {
            DataType::DateTime | DataType::DateTimeN => {
                let days = LittleEndian::read_i32(&value.as_bytes()?[0..4]);
                let third_seconds = LittleEndian::read_u32(&value.as_bytes()?[4..8]);
                let ms = third_seconds / 3 * 10;

                let time =
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::milliseconds(ms.into());
                let date =
                    NaiveDate::from_ymd_opt(1900, 1, 1).unwrap() + Duration::days(days.into());

                Ok(date.and_time(time))
            }
            DataType::DateTime2N => {
                let scale = value.type_info.0.scale;
                let (seconds, days) = match scale {
                    0 | 1 | 2 => (
                        LittleEndian::read_u24(&value.as_bytes()?[0..3]) as u64,
                        LittleEndian::read_u24(&value.as_bytes()?[3..6]),
                    ),
                    3 | 4 => (
                        LittleEndian::read_u32(&value.as_bytes()?[0..4]) as u64,
                        LittleEndian::read_u24(&value.as_bytes()?[4..7]),
                    ),
                    5 | 6 | 7 => {
                        let mut slice = [0u8; 6];
                        slice.copy_from_slice(&value.as_bytes()?[0..6]);
                        slice[5] = 0;
                        (
                            LittleEndian::read_u48(&slice),
                            LittleEndian::read_u24(&value.as_bytes()?[5..8]),
                        )
                    }
                    _ => unreachable!(),
                };

                let nano_seconds = seconds * 10u64.pow(9 - scale as u32);
                let (seconds, nano_seconds) =
                    (nano_seconds / 1_000_000_000, nano_seconds % 1_000_000_000);
                let time = NaiveTime::from_hms_nano_opt(0, 0, 0, nano_seconds as u32).unwrap()
                    + Duration::seconds(seconds as i64);
                let date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap() + Duration::days(days.into());
                Ok(date.and_time(time))
            }
            DataType::DateTimeOffsetN => {
                let days = LittleEndian::read_i32(&value.as_bytes()?[0..4]);
                let third_seconds = LittleEndian::read_u32(&value.as_bytes()?[4..8]);
                let ms = third_seconds / 3 * 10;

                let time =
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::milliseconds(ms.into());
                let date =
                    NaiveDate::from_ymd_opt(1900, 1, 1).unwrap() + Duration::days(days.into());

                Ok(date.and_time(time))
            }
            _ => unreachable!(),
        }
    }
}

impl<Tz: TimeZone> Encode<'_, Mssql> for DateTime<Tz> {
    fn encode_by_ref(&self, _buf: &mut Vec<u8>) -> IsNull {
        todo!()
    }
}

impl<'r> Decode<'r, Mssql> for DateTime<Local> {
    fn decode(_value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        todo!()
    }
}

impl<'r> Decode<'r, Mssql> for DateTime<Utc> {
    fn decode(_value: MssqlValueRef<'r>) -> Result<Self, BoxDynError> {
        todo!()
    }
}
