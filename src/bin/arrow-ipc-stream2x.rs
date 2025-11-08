use std::fs::File;
use std::io::{self, BufReader, Read};

use arrow::ipc::reader::StreamReader;
use clap::Parser;
use rust_xlsxwriter::Workbook;

use rs_arrow_ipc_stream2x::batch_iter2x;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Input Arrow IPC stream file. If not specified, reads from stdin.
    #[arg(short, long)]
    input: Option<String>,

    /// Output Excel file
    #[arg(short, long)]
    output: String,

    /// Sheet name
    #[arg(short, long)]
    sheet: String,
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let reader: Box<dyn Read> = if let Some(input_path) = args.input {
        Box::new(File::open(input_path)?)
    } else {
        Box::new(io::stdin())
    };

    let buf_reader = BufReader::new(reader);

    let ipc_reader = StreamReader::try_new(buf_reader, None)?;

    let mut workbook = Workbook::new();

    batch_iter2x(ipc_reader, &mut workbook, &args.sheet)?;

    workbook.save(args.output)?;

    Ok(())
}
