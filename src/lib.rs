use arrow::array::{Array, as_boolean_array, as_primitive_array, as_string_array};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use rust_xlsxwriter::{Workbook, XlsxError};

#[derive(Debug)]
pub enum Error {
    Xlsx(XlsxError),
    Arrow(arrow::error::ArrowError),
}

impl From<XlsxError> for Error {
    fn from(e: XlsxError) -> Self {
        Error::Xlsx(e)
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(e: arrow::error::ArrowError) -> Self {
        Error::Arrow(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Xlsx(e) => write!(f, "Xlsx error: {}", e),
            Error::Arrow(e) => write!(f, "Arrow error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

pub fn batch_iter2x<I>(mut bi: I, book: &mut Workbook, sheet_name: &str) -> Result<(), Error>
where
    I: Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>,
{
    let worksheet = book.add_worksheet().set_name(sheet_name)?;

    let mut row_offset = 0;

    if let Some(batch_result) = bi.next() {
        let batch = batch_result?;
        let schema = batch.schema();
        for (col, field) in schema.fields().iter().enumerate() {
            worksheet.write_string(row_offset, col as u16, field.name())?;
        }
        row_offset += 1;

        write_batch(worksheet, &batch, &mut row_offset)?;

        for batch_result in bi {
            let batch = batch_result?;
            write_batch(worksheet, &batch, &mut row_offset)?;
        }
    }

    Ok(())
}

macro_rules! write_primitive_number {
    ($worksheet:expr, $column:expr, $row:expr, $row_offset:expr, $col:expr, $type:ty) => {{
        let array = as_primitive_array::<$type>($column);
        if !array.is_null($row) {
            let value = array.value($row);
            $worksheet.write_number($row_offset, $col as u16, value as f64)?;
        }
    }};
}

fn write_batch(
    worksheet: &mut rust_xlsxwriter::Worksheet,
    batch: &RecordBatch,
    row_offset: &mut u32,
) -> Result<(), XlsxError> {
    for row in 0..batch.num_rows() {
        for col in 0..batch.num_columns() {
            let column = batch.column(col);
            let data_type = column.data_type();

            match data_type {
                DataType::Utf8 => {
                    let array = as_string_array(column);
                    if !array.is_null(row) {
                        let value = array.value(row);
                        worksheet.write_string(*row_offset, col as u16, value)?;
                    }
                }
                DataType::Int8 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, Int8Type)
                }
                DataType::Int16 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, Int16Type)
                }
                DataType::Int32 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, Int32Type)
                }
                DataType::Int64 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, Int64Type)
                }
                DataType::UInt8 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, UInt8Type)
                }
                DataType::UInt16 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, UInt16Type)
                }
                DataType::UInt32 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, UInt32Type)
                }
                DataType::UInt64 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, UInt64Type)
                }
                DataType::Float16 => {
                    let array = as_primitive_array::<Float16Type>(column);
                    if !array.is_null(row) {
                        let value = array.value(row);
                        worksheet.write_number(*row_offset, col as u16, value.to_f64())?;
                    }
                }
                DataType::Float32 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, Float32Type)
                }
                DataType::Float64 => {
                    write_primitive_number!(worksheet, column, row, *row_offset, col, Float64Type)
                }
                DataType::Boolean => {
                    let array = as_boolean_array(column);
                    if !array.is_null(row) {
                        let value = array.value(row);
                        worksheet.write_boolean(*row_offset, col as u16, value)?;
                    }
                }
                DataType::Date32 => {
                    let array = as_primitive_array::<Date32Type>(column);
                    if !array.is_null(row) {
                        let value = array.value(row);
                        if let Some(date) = NaiveDate::from_epoch_days(value) {
                            worksheet.write_datetime(*row_offset, col as u16, date)?;
                        }
                    }
                }
                DataType::Date64 => {
                    let array = as_primitive_array::<Date64Type>(column);
                    if !array.is_null(row) {
                        let value = array.value(row);
                        if let Some(datetime_utc) = DateTime::<Utc>::from_timestamp_millis(value) {
                            worksheet.write_datetime(
                                *row_offset,
                                col as u16,
                                datetime_utc.naive_utc(),
                            )?;
                        }
                    }
                }
                DataType::Time32(unit) => match unit {
                    TimeUnit::Second => {
                        let array = as_primitive_array::<Time32SecondType>(column);
                        if !array.is_null(row) {
                            let value = array.value(row);
                            worksheet.write_number(
                                *row_offset,
                                col as u16,
                                value as f64 / 86400.0,
                            )?;
                        }
                    }
                    TimeUnit::Millisecond => {
                        let array = as_primitive_array::<Time32MillisecondType>(column);
                        if !array.is_null(row) {
                            let value = array.value(row);
                            worksheet.write_number(
                                *row_offset,
                                col as u16,
                                value as f64 / 86_400_000.0,
                            )?;
                        }
                    }
                    _ => {}
                },
                DataType::Time64(unit) => match unit {
                    TimeUnit::Microsecond => {
                        let array = as_primitive_array::<Time64MicrosecondType>(column);
                        if !array.is_null(row) {
                            let value = array.value(row);
                            worksheet.write_number(
                                *row_offset,
                                col as u16,
                                value as f64 / 86_400_000_000.0,
                            )?;
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let array = as_primitive_array::<Time64NanosecondType>(column);
                        if !array.is_null(row) {
                            let value = array.value(row);
                            worksheet.write_number(
                                *row_offset,
                                col as u16,
                                value as f64 / 86_400_000_000_000.0,
                            )?;
                        }
                    }
                    _ => {}
                },
                DataType::Timestamp(unit, _) => {
                    let s = match unit {
                        TimeUnit::Second => {
                            let array =
                                as_primitive_array::<arrow::datatypes::TimestampSecondType>(column);
                            if !array.is_null(row) {
                                format!("{}", array.value(row))
                            } else {
                                String::new()
                            }
                        }
                        TimeUnit::Millisecond => {
                            let array = as_primitive_array::<
                                arrow::datatypes::TimestampMillisecondType,
                            >(column);
                            if !array.is_null(row) {
                                format!("{}", array.value(row))
                            } else {
                                String::new()
                            }
                        }
                        TimeUnit::Microsecond => {
                            let array = as_primitive_array::<
                                arrow::datatypes::TimestampMicrosecondType,
                            >(column);
                            if !array.is_null(row) {
                                format!("{}", array.value(row))
                            } else {
                                String::new()
                            }
                        }
                        TimeUnit::Nanosecond => {
                            let array = as_primitive_array::<
                                arrow::datatypes::TimestampNanosecondType,
                            >(column);
                            if !array.is_null(row) {
                                format!("{}", array.value(row))
                            } else {
                                String::new()
                            }
                        }
                    };
                    worksheet.write_string(*row_offset, col as u16, s)?;
                }
                _ => {
                    worksheet.write_string(
                        *row_offset,
                        col as u16,
                        format!("unsupported data type: {:?}", data_type),
                    )?;
                }
            }
        }
        *row_offset += 1;
    }
    Ok(())
}
