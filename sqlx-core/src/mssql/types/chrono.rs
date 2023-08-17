use byteorder::{ByteOrder, LittleEndian};
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
        MssqlTypeInfo(TypeInfo::new(DataType::DateTime, 8))
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
        let days_duration = self.date() - NaiveDate::from_ymd(1900, 1, 1);
        let ms_duration = self.time() - NaiveTime::from_hms(0, 0, 0);
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

                let time = NaiveTime::from_hms(0, 0, 0) + Duration::milliseconds(ms.into());
                let date = NaiveDate::from_ymd(1900, 1, 1) + Duration::days(days.into());

                Ok(date.and_time(time))
            }
            DataType::DateTime2N => {
                let days = LittleEndian::read_u32(&value.as_bytes()?[0..3]);
                let scale = value.type_info.0.scale;
                let seconds = match scale {
                    0 | 1 | 2 => {
                        LittleEndian::read_u32(&value.as_bytes()?[3..6]) / 10u32.pow(scale.into())
                    }
                    3 | 4 => {
                        LittleEndian::read_u32(&value.as_bytes()?[3..7]) / 10u32.pow(scale.into())
                    }
                    5 | 6 | 7 => {
                        LittleEndian::read_u32(&value.as_bytes()?[3..8]) / 10u32.pow(scale.into())
                    }
                    _ => unreachable!(),
                };

                let time = NaiveTime::from_hms(0, 0, 0) + Duration::seconds(seconds.into());
                let date = NaiveDate::from_ymd(1, 1, 1) + Duration::days(days.into());
                Ok(date.and_time(time))
            }
            DataType::DateTimeOffsetN => {
                let days = LittleEndian::read_i32(&value.as_bytes()?[0..4]);
                let third_seconds = LittleEndian::read_u32(&value.as_bytes()?[4..8]);
                let ms = third_seconds / 3 * 10;

                let time = NaiveTime::from_hms(0, 0, 0) + Duration::milliseconds(ms.into());
                let date = NaiveDate::from_ymd(1900, 1, 1) + Duration::days(days.into());

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
