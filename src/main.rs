use anyhow::{anyhow, bail, Error, Result};
use async_stream::try_stream;
use bytes::{Buf, BytesMut};
use clap::{Parser, Subcommand};
use futures::{future::join_all, SinkExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use rkyv::{from_bytes, to_bytes, Archive, Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Instant,
};
use tokio::{
    fs::{create_dir_all, read_dir},
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tokio_stream::{
    wrappers::{ReadDirStream, TcpListenerStream},
    Stream,
};
use tokio_uring::{fs::File, spawn as spawn_tokio_uring, start as start_tokio_uring};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, dispatcher::set_global_default, error, trace, Level};
use tracing_subscriber::FmtSubscriber;

const FILE_READBUF_SIZE: usize = 1024 * 8;
const CONNECT_RETRY_MILLIS: u64 = 100;
const CONNECT_RETRY_TIMES: usize = 10;

/// Length encoding trait, allows for different kinds of length encoding
pub trait LengthCodec: 'static {
    type Error;
    type Buffer: Default + Sized;

    fn as_slice(buffer: &mut Self::Buffer) -> &mut [u8];
    /// Encode into buffer and return part of buffer that was written to
    fn encode(length: usize, buffer: &mut Self::Buffer) -> &[u8];
    /// Decode from buffer, fails if length is formatted incorrectly, returns length and remaining buffer
    fn decode(buffer: &[u8]) -> Result<(usize, &[u8]), Self::Error>;
}

/// Big-endian 64-bit length encoding, can handle length up to 2^64
pub struct U64Length;
impl LengthCodec for U64Length {
    type Error = NotEnoughBytesError;
    type Buffer = [u8; 8];

    #[inline]
    fn as_slice(buffer: &mut Self::Buffer) -> &mut [u8] {
        &mut buffer[..]
    }

    #[inline]
    fn encode(length: usize, buf: &mut Self::Buffer) -> &[u8] {
        *buf = u64::to_be_bytes(length as u64);
        &buf[..]
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<(usize, &[u8]), Self::Error> {
        if buf.len() < 8 {
            Err(NotEnoughBytesError)
        } else {
            // trace!("Decoding length from {:?}", buf);
            let bytes: [u8; 8] = buf[..8].try_into().map_err(|_| NotEnoughBytesError)?;
            // trace!("Decoded length bytes {:?}", bytes);
            let length = u64::from_be_bytes(bytes) as usize;
            let rest = &buf[8..];
            // debug!("Decoded length {} and rest {:?}", length, rest);
            Ok((length, rest))
        }
    }
}

/// Error emitted by const-length encodings when there aren't bytes in the passed buffer when decoding
pub struct NotEnoughBytesError;

/// Subcommand for sending
#[derive(Parser, Debug)]
#[clap(name = "send")]
struct Send {
    /// File or directory to send
    local_path: PathBuf,
    #[clap(short, long, default_value_t = 3120)]
    /// Port to listen on
    port: u16,
    #[clap(short, long, default_value_t = false)]
    /// Whether to listen on localhost only
    local: bool,
}

/// Subcommand for receiving
#[derive(Parser, Debug)]
#[clap(name = "receive")]
struct Receive {
    /// File or directory to receive into
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
    #[clap(short, long, default_value_t = Level::INFO)]
    /// Logging level
    log_level: Level,
    #[clap(short, long, default_value_t = false)]
    /// Whether to not print anything
    quiet: bool,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
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

impl Default for FileIdentifier {
    fn default() -> Self {
        Self { identifier: 0 }
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

impl FileData {
    fn try_new<P: AsRef<Path>>(path: P, identifier: FileIdentifier) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        path.metadata()
            .map_err(|e| anyhow!("Failed to get metadata for {}: {}", path.display(), e))
            .map(|metadata| Self {
                path: path.to_string_lossy().into_owned(),
                identifier,
                size: metadata.len(),
            })
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[archive(check_bytes)] // check_bytes is required
#[archive_attr(derive(Debug))]
struct FileChunk {
    identifier: FileIdentifier,
    offset: u64,
    data: Vec<u8>,
}

impl FileChunk {
    fn new(identifier: FileIdentifier, offset: u64, data: Vec<u8>) -> Self {
        Self {
            identifier,
            offset,
            data,
        }
    }
}

async fn file_listing<P: AsRef<Path>>(path: P) -> impl Stream<Item = Result<FileData>> {
    try_stream! {
        trace!("Listing files in {:?}", path.as_ref());
        let root_path = path.as_ref().to_path_buf();
        if root_path.is_dir() {
            let mut stack = vec![root_path.clone()];
            let mut identifier = 0;

            while !stack.is_empty() {
                let dir = stack.pop().unwrap();

                for await entry in ReadDirStream::new(read_dir(&dir).await?) {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.is_file() {
                            yield FileData::try_new(path, FileIdentifier::new(identifier))?;
                            identifier += 1;
                        } else if path.is_dir() {
                            stack.push(path);
                        }
                    }
                }
            }
        } else {
            // let file_name = PathBuf::from(root_path.file_name().unwrap().to_string_lossy().into_owned());
            yield FileData::try_new(root_path, FileIdentifier::default())?;

        }
    }
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[archive(check_bytes)] // check_bytes is required
#[archive_attr(derive(Debug))]
enum Message {
    FileListing((Option<String>, Vec<FileData>)),
    FileChunk(FileChunk),
    Done,
}

struct MessageCodec {}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // debug!("Trying to decode message {:?}", src);

        let mut length_buf = <U64Length as LengthCodec>::Buffer::default();
        let length_buf = <U64Length as LengthCodec>::as_slice(&mut length_buf);
        if let Ok((length, rest)) = <U64Length as LengthCodec>::decode(src)
            .map_err(|_| anyhow!("Failed to decode message length"))
        {
            if rest.len() < length {
                // If we don't have the archive, we'll re-read the length next time
                trace!(
                    "Not enough bytes to decode message, expected {} but only have {}. Waiting for more bytes",
                    length,
                    rest.len()
                );
                Ok(None)
            } else {
                // We read the length and we have enough data, so if we fail now it's fatal
                let message = from_bytes::<Message>(&rest[..length])
                    .map_err(|_| anyhow!("Failed to decode message"))?;

                src.advance(length_buf.len());
                src.advance(length);
                // debug!("Decoded message: {:?}", message);

                Ok(Some(message))
            }
        } else {
            trace!("Not enough bytes to decode message length, waiting for more bytes");
            Ok(None)
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let message_bytes = to_bytes::<_, 8192>(&item)?;
        let length_buf = &mut <U64Length as LengthCodec>::Buffer::default();
        let length_buf = <U64Length as LengthCodec>::encode(message_bytes.len(), length_buf);
        dst.extend_from_slice(length_buf);
        dst.extend_from_slice(&message_bytes);

        // debug!("Encoded message: {:?}", item);

        Ok(())
    }
}

async fn listen_one(port: u16, local: bool) -> Result<Framed<TcpStream, MessageCodec>> {
    let mut listener = TcpListenerStream::new(
        TcpListener::bind(SocketAddr::new(
            IpAddr::V4(if local {
                Ipv4Addr::new(127, 0, 0, 1)
            } else {
                Ipv4Addr::new(0, 0, 0, 0)
            }),
            port,
        ))
        .await?,
    );

    Ok(Framed::new(
        listener
            .try_next()
            .await?
            .ok_or_else(|| anyhow!("No incoming connections received on port {}", port))?,
        MessageCodec {},
    ))
}

/// Listen for one incoming connection on the given port and send the given file or directory
/// over the connection.
async fn send(args: Send) -> Result<()> {
    // TODO: This isn't a good idea if the file listing is so large we can't hold it in memory
    let files = file_listing(&args.local_path)
        .await
        .try_collect::<Vec<_>>()
        .await?;

    trace!("Sending files: {:?}", files);

    let mut stream = listen_one(args.port, args.local).await?;

    stream
        .send(Message::FileListing((
            if args.local_path.is_dir() {
                let path_str = args.local_path.to_string_lossy().to_string();
                if path_str.ends_with('/') {
                    Some(path_str)
                } else {
                    Some(format!("{}/", path_str))
                }
            } else {
                None
            },
            files.clone(),
        )))
        .await?;
    let (tx, mut rx) = unbounded_channel::<FileChunk>();

    let tasks = files
        .iter()
        .cloned()
        .map(|fd| {
            let tx = tx.clone();
            let task = spawn_tokio_uring(async move {
                let file = File::open(fd.path.clone()).await?;
                let mut offset = 0;

                loop {
                    let buf = vec![0; FILE_READBUF_SIZE];
                    match file.read_at(buf, offset).await {
                        (Ok(0), _) => {
                            break;
                        }
                        (Ok(read), buffer) => {
                            tx.send(FileChunk::new(
                                fd.identifier.clone(),
                                offset,
                                buffer[..read].to_vec(),
                            ))
                            .or_else(|e| Err(anyhow!("Failed to send file chunk: {}", e)))?;
                            trace!("Sent file chunk of length {} for {}", read, fd.path);
                            offset += read as u64;
                        }
                        (Err(e), _) => {
                            return Err(anyhow!(e));
                        }
                    }
                }

                tx.send(FileChunk::new(fd.identifier.clone(), offset, vec![]))
                    .or_else(|e| Err(anyhow!("Failed to send file chunk: {}", e)))?;

                Ok(())
            });
            task
        })
        .collect::<Vec<_>>();

    drop(tx);

    while let Some(file_chunk) = rx.recv().await {
        stream.send(Message::FileChunk(file_chunk)).await?;
    }

    stream.send(Message::Done).await?;

    join_all(tasks).await.iter().for_each(|r| {
        if let Err(e) = r {
            error!("Failed to join: {}", e);
        }
        if let Ok(Err(e)) = r {
            error!("Failed to send file: {}", e);
        }
    });

    Ok(())
}

async fn connect_one(address: SocketAddr) -> Result<Framed<TcpStream, MessageCodec>> {
    debug!("Connecting to {}:{}", address.ip(), address.port());

    Ok(Framed::new(
        TcpStream::connect(address).await.map_err(|e| {
            anyhow!(
                "Failed to connect to {}:{}: {}",
                address.ip(),
                address.port(),
                e
            )
        })?,
        MessageCodec {},
    ))
}

async fn connect(address: SocketAddr) -> Result<Framed<TcpStream, MessageCodec>> {
    let strategy = ExponentialBackoff::from_millis(CONNECT_RETRY_MILLIS)
        .map(jitter)
        .take(CONNECT_RETRY_TIMES);

    Retry::spawn(strategy, || connect_one(address)).await
}

async fn receive(args: Receive) -> Result<()> {
    let mut stream = connect(args.address).await?;

    let mut files = if let Message::FileListing(file_listing) = stream
        .try_next()
        .await?
        .ok_or_else(|| anyhow!("No file listing received"))?
    {
        file_listing
    } else {
        bail!("Expected file listing");
    };

    let mut channels = files
        .1
        .iter()
        .map(|fd| {
            let identifier = fd.identifier.clone();
            let (tx, rx) = unbounded_channel::<FileChunk>();
            (identifier, (tx, Some(rx)))
        })
        .collect::<HashMap<
            FileIdentifier,
            (
                UnboundedSender<FileChunk>,
                Option<UnboundedReceiver<FileChunk>>,
            ),
        >>();

    let prefix = files.0.take();
    let tasks = files
        .1
        .iter()
        .cloned()
        .map(|fd| {
            let mut rx = channels.get_mut(&fd.identifier).unwrap().1.take().unwrap();
            let prefix = prefix.clone();
            let remote_path = args.remote_path.clone();
            trace!(
                "Receiving file: {:?} to remote path {} from prefix {:?}",
                fd,
                remote_path.display(),
                prefix
            );
            let task = spawn_tokio_uring(async move {
                let path = if let Some(prefix) = &prefix {
                    remote_path.join(PathBuf::from(fd.path.strip_prefix(prefix).unwrap()))
                } else {
                    remote_path.join(PathBuf::from(fd.path.clone()).file_name().unwrap())
                };

                debug!("Receiving file {} to {:?}", fd.path, path);

                let dir = path.parent().expect("File path has no parent");

                create_dir_all(dir).await.or_else(|e| {
                    error!("Failed to create directory {:?}: {}", dir, e);
                    Err(anyhow!("Failed to create directory {:?}: {}", dir, e))
                })?;

                let file = File::create(&path).await.or_else(|e| {
                    error!("Failed to create file {:?}: {}", path, e);
                    Err(anyhow!("Failed to create file {:?}: {}", path, e))
                })?;

                debug!("Created file {:?}", path);

                let mut queued_writes: Vec<(Vec<u8>, u64)> = Vec::new();

                loop {
                    // Try to write any queued writes
                    while let Some((data, offset)) = queued_writes.pop() {
                        let expected_len = data.len();
                        debug!("Writing queued write of length {}", expected_len);
                        match file.write_at(data, offset).await {
                            (Ok(written), buffer) => {
                                if written != expected_len {
                                    // This isn't an error, we need to retry later
                                    debug!(
                                        "Queuing incomplete write of length {}",
                                        buffer.len() - written
                                    );
                                    queued_writes.push((
                                        buffer[written..].to_vec(),
                                        offset + written as u64,
                                    ));
                                }
                            }
                            (Err(e), _) => {
                                error!("Failed to write file chunk: {}", e);
                                return Err(anyhow!(e));
                            }
                        }
                    }

                    match rx.recv().await {
                        Some(file_chunk) => {
                            trace!(
                                "Received file chunk of length {} for {}",
                                file_chunk.data.len(),
                                fd.path
                            );
                            if file_chunk.data.is_empty() {
                                debug!("Received all file chunks for {}", fd.path);
                                break;
                            }
                            let expected_len = file_chunk.data.len();
                            match file.write_at(file_chunk.data, file_chunk.offset).await {
                                (Ok(written), buffer) => {
                                    if written != expected_len {
                                        // This isn't an error, we need to retry later
                                        debug!(
                                            "Queuing incomplete write of length {}",
                                            buffer.len() - written
                                        );
                                        queued_writes.push((
                                            buffer[written..].to_vec(),
                                            file_chunk.offset + written as u64,
                                        ));
                                    }
                                }
                                (Err(e), _) => {
                                    error!("Failed to write file chunk: {}", e);
                                    return Err(anyhow!(e));
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }

                Ok(())
            });

            task
        })
        .collect::<Vec<_>>();

    let mut bytes_received = 0;

    let start_time = Instant::now();
    let m = MultiProgress::new();
    let mstyle = ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] ({msg:<12.cyan/blue}) [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-");

    let pbs = files
        .1
        .iter()
        .map(|fd| {
            let pb = m.add(ProgressBar::new(fd.size as u64));
            pb.set_message(
                PathBuf::from(&fd.path)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            );
            pb.set_style(mstyle.clone());
            (fd.identifier.clone(), pb)
        })
        .collect::<HashMap<FileIdentifier, ProgressBar>>();

    loop {
        let message = stream.try_next().await?;

        match message {
            Some(Message::FileListing(_)) => {
                bail!("Received unexpected file listing");
            }
            Some(Message::FileChunk(file_chunk)) => {
                if let Some(pb) = pbs.get(&file_chunk.identifier) {
                    pb.inc(file_chunk.data.len() as u64);

                    if file_chunk.data.is_empty() {
                        pb.finish_and_clear();
                        m.remove(pb);
                    }
                }

                bytes_received += file_chunk.data.len();

                if let Some((tx, _)) = channels.get_mut(&file_chunk.identifier) {
                    let identifier = file_chunk.identifier.clone();
                    tx.send(file_chunk).or_else(|e| {
                        Err(anyhow!(
                            "Failed to send file chunk for identifier {:?}: {}",
                            identifier,
                            e
                        ))
                    })?;
                } else {
                    bail!("Received file chunk for unknown file identifier");
                }
            }
            Some(Message::Done) => {
                break;
            }
            None => {}
        }
    }

    join_all(tasks).await.iter().for_each(|r| {
        if let Err(e) = r {
            error!("Failed to receive file: {}", e);
        }
        if let Ok(Err(e)) = r {
            error!("Failed to send file: {}", e);
        }
    });

    m.clear()?;

    let stop_time = Instant::now();

    let duration = stop_time - start_time;

    Ok(())
}

// #[tokio::main]
fn main() -> Result<()> {
    let args = Args::parse();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(args.log_level)
        .finish();

    set_global_default(subscriber.into())?;

    start_tokio_uring(async {
        if let Commands::Send(send_args) = args.command {
            send(send_args).await?;
        } else if let Commands::Receive(receive_args) = args.command {
            receive(receive_args).await?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use id_tree::{InsertBehavior, Node, NodeId, Tree, TreeBuilder};
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use std::{
        fs::create_dir_all,
        io::{Read, Write},
        thread::spawn as spawn_thread,
    };
    use tempfile::tempdir;
    use tokio::spawn;
    use tracing_test::traced_test;

    fn random_string(max_length: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(thread_rng().gen_range(1..max_length))
            .map(char::from)
            .collect()
    }

    #[derive(Debug, Clone, Copy)]
    struct RandomDirectoryParameters {
        max_name_length: usize,
        max_entries: usize,
        max_file_size: usize,
    }

    #[derive(Debug, Clone)]
    enum RandomDirectoryEntry {
        Directory(PathBuf),
        File(PathBuf),
    }

    impl RandomDirectoryEntry {
        fn random_in(parent: PathBuf, params: &RandomDirectoryParameters) -> Self {
            let mut rng = thread_rng();
            let random: u8 = rng.gen();
            if random % 2 == 0 {
                Self::dir_in(parent, params)
            } else {
                Self::file_in(parent, params)
            }
        }

        fn dir_in(parent: PathBuf, params: &RandomDirectoryParameters) -> Self {
            let name = random_string(params.max_name_length);
            let path = parent.join(name);
            Self::Directory(path)
        }

        fn file_in(parent: PathBuf, params: &RandomDirectoryParameters) -> Self {
            let name = random_string(params.max_name_length);
            let path = parent.join(name);
            Self::File(path)
        }

        fn create(&self, params: &RandomDirectoryParameters) -> Result<()> {
            match &*self {
                RandomDirectoryEntry::Directory(dir) => {
                    trace!("Creating directory {:?}", dir);
                    create_dir_all(&dir).map_err(|e| anyhow!("Failed to create {:?}: {}", dir, e))
                }
                RandomDirectoryEntry::File(file) => {
                    trace!("Creating file {:?}", file);
                    let contents = random_string(params.max_file_size);
                    let mut f = std::fs::File::create(&file)
                        .map_err(|e| anyhow!("Failed to create {:?}: {}", file, e))?;
                    write!(f, "{}", contents)
                        .map_err(|e| anyhow!("Failed to write {:?}: {}", file, e))
                }
            }
        }
    }

    #[derive(Debug)]
    struct RandomDirectory {
        tree: Tree<RandomDirectoryEntry>,
        _root_dir: PathBuf,
        root: NodeId,
        params: RandomDirectoryParameters,
    }

    #[derive(Debug, Clone, Copy)]
    enum RandomDirectoryAction {
        NewEntry,
        PopUp,
        EnterDir,
    }

    impl RandomDirectory {
        fn random_in(root_dir: PathBuf, params: RandomDirectoryParameters) -> Self {
            let mut rng = thread_rng();
            let mut tree = TreeBuilder::new()
                .with_node_capacity(params.max_entries)
                .build();
            let root = tree
                .insert(
                    Node::new(RandomDirectoryEntry::Directory(root_dir.clone())),
                    InsertBehavior::AsRoot,
                )
                .unwrap();

            let mut stack = vec![root.clone()];

            for i in 0..params.max_entries {
                trace!("Action {}", i);
                let top = stack.last().unwrap();
                let action = if stack.len() > 1 {
                    match rng.gen_range(0..3) {
                        0 => RandomDirectoryAction::NewEntry,
                        1 => RandomDirectoryAction::PopUp,
                        2 => RandomDirectoryAction::EnterDir,
                        _ => unreachable!(),
                    }
                } else {
                    match rng.gen_range(0..2) {
                        0 => RandomDirectoryAction::NewEntry,
                        1 => RandomDirectoryAction::EnterDir,
                        _ => unreachable!(),
                    }
                };

                trace!("Action {:?} on {:?}", action, top);

                let current_dir =
                    if let RandomDirectoryEntry::Directory(dir) = tree.get(top).unwrap().data() {
                        dir.clone()
                    } else {
                        unreachable!()
                    };

                match action {
                    RandomDirectoryAction::NewEntry => {
                        let entry = RandomDirectoryEntry::random_in(current_dir, &params);

                        let entry_id = tree
                            .insert(Node::new(entry.clone()), InsertBehavior::UnderNode(&top))
                            .unwrap();

                        if let RandomDirectoryEntry::Directory(_) = entry {
                            stack.push(entry_id);
                        }
                    }
                    RandomDirectoryAction::PopUp => {
                        stack.pop();
                    }
                    RandomDirectoryAction::EnterDir => {
                        let children = tree.get(top).unwrap().children();
                        // Get all the children that are directories
                        let dirs = children
                            .into_iter()
                            .filter(|node| {
                                if let RandomDirectoryEntry::Directory(_) =
                                    tree.get(node).unwrap().data()
                                {
                                    true
                                } else {
                                    false
                                }
                            })
                            .collect::<Vec<_>>();
                        if dirs.is_empty() {
                            // Create a directory and enter it
                            let dir = RandomDirectoryEntry::dir_in(current_dir, &params);
                            let dir_id = tree
                                .insert(Node::new(dir.clone()), InsertBehavior::UnderNode(&top))
                                .unwrap();
                            stack.push(dir_id);
                        } else {
                            // Enter a random directory
                            let dir = dirs[rng.gen_range(0..dirs.len())].clone();
                            stack.push(dir);
                        }
                    }
                }
            }

            Self {
                tree,
                _root_dir: root_dir,
                root,
                params,
            }
        }

        fn create(&self) -> Result<()> {
            self.tree
                .traverse_level_order(&self.root)
                .unwrap()
                .into_iter()
                .for_each(|node| {
                    node.data().create(&self.params).unwrap();
                });
            Ok(())
        }
    }

    // Test for the test fixture :) works well, so commented it out -- might just make this its
    // own crate later

    // #[test]
    // fn test_random_directory() {
    //     let temp_dir = tempdir().unwrap();
    //     let tempdir_path = temp_dir.path().to_path_buf();
    //     let rd = RandomDirectory::random_in(
    //         tempdir_path.clone(),
    //         RandomDirectoryParameters {
    //             max_name_length: 10,
    //             max_entries: 10,
    //             max_file_size: 65535,
    //         },
    //     );
    //     println!("Random directory in {:?}:\n{:#?}", tempdir_path, rd);
    //     println!("Random directory: {:?}", rd);
    //     rd.create().unwrap();
    // }

    #[tokio::test]
    #[traced_test]
    async fn test_file_listing_file() {
        let test_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_directory/1");

        let mut rng = thread_rng();
        let port: u16 = rng.gen_range(1024..65535);
        let files = file_listing(test_file.clone())
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        spawn(async move {
            let mut lstream = listen_one(port, true).await.unwrap();
            let files = file_listing(test_file)
                .await
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            lstream
                .send(Message::FileListing((None, files.clone())))
                .await
                .unwrap();
        });

        let mut rstream = connect(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
        .await
        .unwrap();

        if let Message::FileListing(received_files) = rstream.try_next().await.unwrap().unwrap() {
            assert_eq!(files, received_files.1);
        } else {
            panic!("Expected file listing");
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_file_listing_dir() {
        let test_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_directory/");

        let mut rng = thread_rng();
        let port: u16 = rng.gen_range(1024..65535);
        let files = file_listing(test_file.clone())
            .await
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        spawn(async move {
            let mut lstream = listen_one(port, true).await.unwrap();
            let files = file_listing(&test_file)
                .await
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            lstream
                .send(Message::FileListing((
                    Some(test_file.to_string_lossy().to_string()),
                    files.clone(),
                )))
                .await
                .unwrap();
        });

        let mut rstream = connect(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
        .await
        .unwrap();

        if let Message::FileListing(received_files) = rstream.try_next().await.unwrap().unwrap() {
            assert_eq!(files, received_files.1);
        } else {
            panic!("Expected file listing");
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_send_file() {
        let mut rng = thread_rng();
        let port = rng.gen_range(1024..65535);

        let sender_runtime = {
            let port = port.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let sender_args = Send {
                        local_path: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                            .join("tests/test_directory/1"),
                        port: port,
                        local: true,
                    };
                    send(sender_args).await
                })
                .expect("Sender failed");
            })
        };

        let mut rstream = connect(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
        .await
        .unwrap();

        if let Message::FileListing(_received_files) = rstream.try_next().await.unwrap().unwrap() {
            loop {
                match rstream.try_next().await.unwrap() {
                    Some(Message::FileChunk(file_chunk)) => {
                        println!("Received file chunk {:?}", file_chunk);
                    }
                    Some(Message::Done) => {
                        println!("Received done");
                        break;
                    }
                    _ => {
                        panic!("Expected file chunk or done");
                    }
                }
            }
        } else {
            panic!("Expected file listing");
        }

        sender_runtime.join().unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_send_dir() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let rd = RandomDirectory::random_in(
            dir_path.clone(),
            RandomDirectoryParameters {
                max_name_length: 10,
                max_entries: 10,
                max_file_size: 1024 * 1024,
            },
        );
        rd.create().unwrap();

        let mut rng = thread_rng();
        let port = rng.gen_range(1024..65535);

        let sender_runtime = {
            let port = port.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let sender_args = Send {
                        local_path: dir_path,
                        port: port,
                        local: true,
                    };
                    send(sender_args).await
                })
                .expect("Sender failed");
            })
        };

        let mut rstream = connect(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
        .await
        .unwrap();

        if let Message::FileListing(_received_files) = rstream.try_next().await.unwrap().unwrap() {
            loop {
                match rstream.try_next().await.unwrap() {
                    Some(Message::FileChunk(file_chunk)) => {
                        debug!("Received file chunk {:?}", file_chunk);
                    }
                    Some(Message::Done) => {
                        debug!("Received done");
                        break;
                    }
                    _ => {
                        panic!("Expected file chunk or done");
                    }
                }
            }
        } else {
            panic!("Expected file listing");
        }

        sender_runtime.join().unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_recv_file() {
        let mut rng = thread_rng();
        let port = rng.gen_range(1024..65535);
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();

        let sender_runtime = {
            let port = port.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let sender_args = Send {
                        local_path: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                            .join("tests/test_directory/1"),
                        port: port,
                        local: true,
                    };
                    send(sender_args).await
                })
                .expect("Sender failed");
            })
        };
        let receiver_runtime = {
            let port = port.clone();
            let dir_path = dir_path.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let receiver_args = Receive {
                        remote_path: dir_path,
                        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    };
                    receive(receiver_args).await
                })
                .expect("Receiver failed");
            })
        };

        receiver_runtime.join().unwrap();
        sender_runtime.join().unwrap();

        let file_1_path = dir_path.join("1");

        // Check contents
        let mut file_1 = std::fs::File::open(file_1_path).unwrap();
        let mut file_1_contents = String::new();
        file_1.read_to_string(&mut file_1_contents).unwrap();
        let mut orig_file_1 = std::fs::File::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_directory/1"),
        )
        .unwrap();
        let mut orig_file_1_contents = String::new();
        orig_file_1
            .read_to_string(&mut orig_file_1_contents)
            .unwrap();
        assert_eq!(file_1_contents, orig_file_1_contents);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_recv_dir() {
        let mut rng = thread_rng();
        let port = rng.gen_range(1024..65535);
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();

        let rand_dir = tempdir().unwrap();
        // let rand_dir_path = rand_dir.into_path();
        let rand_dir_path = rand_dir.path().to_path_buf();

        let rd = RandomDirectory::random_in(
            rand_dir_path.clone(),
            RandomDirectoryParameters {
                max_name_length: 10,
                max_entries: 10,
                max_file_size: 1024 * 1024,
            },
        );
        rd.create().unwrap();

        let sender_runtime = {
            let port = port.clone();
            let rand_dir_path = rand_dir_path.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let sender_args = Send {
                        local_path: rand_dir_path,
                        port: port,
                        local: true,
                    };
                    send(sender_args).await
                })
                .expect("Sender failed");
            })
        };
        let receiver_runtime = {
            let port = port.clone();
            let dir_path = dir_path.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let receiver_args = Receive {
                        remote_path: dir_path,
                        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    };
                    receive(receiver_args).await
                })
                .expect("Receiver failed");
            })
        };

        receiver_runtime.join().unwrap();
        sender_runtime.join().unwrap();
        println!("Transferring from: {:?}", rand_dir_path);
        println!("Transferring to: {:?}", dir_path);
        std::thread::sleep(std::time::Duration::from_secs(15));
    }

    #[tokio::test]
    async fn test_bench_recv_dir() {
        let mut rng = thread_rng();
        let port = rng.gen_range(1024..65535);
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();

        let rand_dir = tempdir().unwrap();
        // let rand_dir_path = rand_dir.into_path();
        let rand_dir_path = rand_dir.path().to_path_buf();

        let rd = RandomDirectory::random_in(
            rand_dir_path.clone(),
            RandomDirectoryParameters {
                max_name_length: 10,
                max_entries: 128,
                max_file_size: 1024 * 1024 * 64,
            },
        );
        rd.create().unwrap();

        let sender_runtime = {
            let port = port.clone();
            let rand_dir_path = rand_dir_path.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let sender_args = Send {
                        local_path: rand_dir_path,
                        port: port,
                        local: true,
                    };
                    send(sender_args).await
                })
                .expect("Sender failed");
            })
        };
        let receiver_runtime = {
            let port = port.clone();
            let dir_path = dir_path.clone();
            spawn_thread(move || {
                start_tokio_uring(async {
                    let receiver_args = Receive {
                        remote_path: dir_path,
                        address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                    };
                    receive(receiver_args).await
                })
                .expect("Receiver failed");
            })
        };

        receiver_runtime.join().unwrap();
        sender_runtime.join().unwrap();
        println!("Transferring from: {:?}", rand_dir_path);
        println!("Transferring to: {:?}", dir_path);
        std::thread::sleep(std::time::Duration::from_secs(15));
    }
}
