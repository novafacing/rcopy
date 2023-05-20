use anyhow::{anyhow, bail, Result};
use async_stream::{stream, try_stream};
use bytes::{Bytes, BytesMut};
use clap::{Parser, Subcommand};
use futures::{future, stream::iter, StreamExt, TryStreamExt};
use rkyv::{AlignedVec, Archive, Deserialize, Infallible, Serialize};
use rkyv_codec::{archive_stream, RkyvWriter, VarintLength};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error as ThisError;
use tokio::{
    fs::{read_dir, File},
    io::{AsyncRead, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_stream::{
    wrappers::{ReadDirStream, TcpListenerStream},
    Stream,
};

const FILE_READBUF_SIZE: usize = 1024 * 8;

/// Subcommand for sending
#[derive(Parser, Debug)]
#[clap(name = "send")]
struct Send {
    /// File or directory to send
    local_path: PathBuf,
    #[clap(short, long, default_value_t = 3120)]
    /// Port to listen on
    port: u16,
}

/// Subcommand for receiving
#[derive(Parser, Debug)]
#[clap(name = "receive")]
struct Receive {
    /// File or directory to receive
    remote_path: PathBuf,
    /// Machine address to receive from
    #[clap(short, long)]
    address: SocketAddr,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Send(Send),
    Receive(Receive),
}

/// Main command
#[derive(Parser, Debug)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

struct Parameters {
    /// Network chunk size
    chunk_size: usize,
}

impl Default for Parameters {
    fn default() -> Self {
        Self { chunk_size: 1024 }
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[archive(check_bytes)] // check_bytes is required
#[archive_attr(derive(Debug))]
struct FileIdentifier {
    identifier: u64,
}

impl FileIdentifier {
    fn new(identifier: u64) -> Self {
        Self { identifier }
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[archive(check_bytes)] // check_bytes is required
#[archive_attr(derive(Debug))]
struct FileData {
    path: String,
    identifier: FileIdentifier,
    size: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[archive(check_bytes)] // check_bytes is required
#[archive_attr(derive(Debug))]
struct FileChunk {
    identifier: FileIdentifier,
    offset: u64,
    data: Vec<u8>,
}

impl FileData {
    fn try_new(path: PathBuf, identifier: FileIdentifier) -> Result<Self> {
        path.metadata()
            .map_err(|e| anyhow!("Failed to get metadata for {}: {}", path.display(), e))
            .map(|metadata| Self {
                path: path.to_string_lossy().into_owned(),
                identifier,
                size: metadata.len(),
            })
    }
}

async fn file_listing(path: PathBuf) -> impl Stream<Item = Result<FileData>> {
    try_stream! {
        let mut identifier = 0;
        if path.is_dir() {
            for await entry in ReadDirStream::new(read_dir(path).await?) {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() {
                        yield FileData::try_new(path, FileIdentifier::new(identifier))?;
                        identifier += 1;
                    }
                }
            }
        } else {
            yield FileData::try_new(path, FileIdentifier::new(identifier))?;
        }
    }
}

/// Listen for one incoming connection on the given port and send the given file or directory
/// over the connection.
async fn send(args: Send) -> Result<()> {
    // TODO: This isn't a good idea if the file listing is so large we can't hold it in memory
    let files = file_listing(args.local_path)
        .await
        .try_collect::<Vec<_>>()
        .await?;

    let mut listener = TcpListenerStream::new(
        TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            args.port,
        ))
        .await?,
    );

    let stream = listener
        .try_next()
        .await?
        .ok_or_else(|| anyhow!("No incoming connections received on port {}", args.port))?;

    // Send the file listing

    Ok(())
}

async fn receive(args: Receive) -> Result<()> {
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Commands::Send(send_args) = args.command {
        send(send_args).await?;
    } else if let Commands::Receive(receive_args) = args.command {
        receive(receive_args).await?;
    }

    Ok(())
}
