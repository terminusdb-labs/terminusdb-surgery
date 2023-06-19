use std::{
    io::{self, SeekFrom},
    path::PathBuf,
};

use bytes::Bytes;
use clap::*;
use futures::StreamExt;
use terminus_store::{
    layer::builder::{self, build_object_index_from_direct_files},
    storage::{
        archive::{ArchiveHeader, ArchiveLayerStore, ArchiveSliceReader, DirectoryArchiveBackend},
        consts::{LayerFileEnum, FILENAME_ENUM_MAP},
        directory::FileBackedStore,
        *,
    },
    store::sync::{open_sync_archive_store, SyncStore, SyncStoreLayer},
    structure::{stream::TfcDictStream, LogArray, parse_control_word},
    Layer,
};

use num::FromPrimitive;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Lookup a node id starting at the current layer
    NodeId {
        /// The node
        node: String,
        /// Layer in which to start the lookup
        layer: Option<String>,
        /// Label in which to start the lookup
        label: Option<String>,
        /// The workdir to store mappings in
        #[arg(short = 's', long = "store")]
        store: Option<String>,
    },
    /// Lookup the node for an id starting at the current layer
    IdNode {
        /// The node
        id: String,
        /// Layer in which to start the lookup
        #[arg(short = 'l', long = "layer")]
        layer: Option<String>,
        /// Label in which to start the lookup
        #[arg(short = 'g', long = "label")]
        label: Option<String>,
        /// The workdir to store mappings in
        #[arg(short = 's', long = "store")]
        store: Option<String>,
    },
    /// Node count of layer
    NodeCount {
        #[arg(short = 'l', long = "layer")]
        layer: Option<String>,
        /// Label in which to start the lookup
        #[arg(short = 'g', long = "label")]
        label: Option<String>,
        /// The workdir to store mappings in
        #[arg(short = 's', long = "store")]
        store: Option<String>,
    },
    /// Parse a larch header from a file
    ParseHeader {
        /// The file to parse the header from
        file_name: String,
        /// whether to sort by size
        #[arg(short, long)]
        sort: bool,
    },
    /// Print dicts
    PrintDict {
        file_name: String,
        #[arg(value_enum)]
        dict_type: DictType,
    },
    /// Validate LogArray
    ValidateLogArray {
        file_name: String,
        /// Whether the header is at the start or at the end of the
        /// logarray. Default is start (false).
        #[arg(short, long, default_value_t = false)]
        header_first: bool,
    },
    /// Extract a file from an archive
    Extract {
        layer_file_name: String,
        file_name: String,
    },
    /// Build an object index file set from input files
    BuildObjectIndex {
        sp_o_nums_file: String,
        sp_o_bits_file: String,
        o_ps_dir: String,
        #[arg(long)]
        objects_file: Option<String>,
    },
    /// Build a predicate index from the given s_p nums file
    BuildPredicateIndex {
        s_p_nums_file: String,
        predicate_index_dir: String,
    },
    /// Return a triple count of the given layer
    TripleCount {
        layer_file: String
    }
}

#[derive(ValueEnum, Clone, PartialEq, Eq)]
enum DictType {
    Nodes,
    Predicates,
    Values,
}

fn open_layer_or_label(
    store: SyncStore,
    layer: Option<String>,
    label: Option<String>,
) -> Box<SyncStoreLayer> {
    let res = match (layer, label) {
        (None, None) => panic!("You must specify either a layer or a label"),
        (None, Some(label_name)) => store.create(&label_name).unwrap().head(),
        (Some(layer_name), None) => {
            let layer = string_to_name(&layer_name).unwrap();
            store.get_layer_from_id(layer)
        }
        (Some(_), Some(_)) => panic!("You must specify either a layer or a label"),
    };
    Box::new(res.unwrap().unwrap())
}

fn node_id(store: &str, layer: Option<String>, label: Option<String>, node: &str) -> Option<u64> {
    let store = open_sync_archive_store(store, 512);
    let layer = open_layer_or_label(store, layer, label);
    layer.subject_id(node)
}

fn id_node(store: &str, layer: Option<String>, label: Option<String>, id: &str) -> Option<String> {
    let store = open_sync_archive_store(store, 512);
    let layer = open_layer_or_label(store, layer, label);
    layer.id_subject(id.parse().unwrap())
}

async fn node_count(store: &str, layer: Option<String>, label: Option<String>) -> Option<u64> {
    let backend = DirectoryArchiveBackend::new(store.into());
    let archive_store = ArchiveLayerStore::new(backend.clone(), backend);
    let store = open_sync_archive_store(store, 512);
    let layer_name = open_layer_or_label(store, layer, label).name();
    archive_store.get_node_count(layer_name).await.unwrap()
}

async fn get_triple_count(layer: String) -> io::Result<()> {
    let mut file = tokio::fs::File::open(layer).await.unwrap();
    let header = ArchiveHeader::parse_from_reader(&mut file).await?;
    let range = header.range_for(LayerFileEnum::PosSpOAdjacencyListNums).unwrap();
    let mut buf = [0;8];
    file.seek(SeekFrom::Current(range.end as i64 -8)).await?;
    file.read_exact(&mut buf).await?;
    let (size, _width) = parse_control_word(&buf);

    println!("{size}");
    Ok(())
}


async fn open_slice(
    file_name: PathBuf,
    file_type: LayerFileEnum,
) -> io::Result<ArchiveSliceReader> {
    let mut reader = tokio::fs::File::open(file_name).await?;
    let header = ArchiveHeader::parse_from_reader(&mut reader).await?;

    let range = header.range_for(file_type).unwrap();
    let remaining = range.len();
    reader.seek(SeekFrom::Current((range.start) as i64)).await?;

    Ok(ArchiveSliceReader::new(reader, remaining))
}

async fn print_dict(file_name: PathBuf, t: DictType) -> std::io::Result<()> {
    let file_type = match t {
        DictType::Nodes => LayerFileEnum::NodeDictionaryBlocks,
        DictType::Predicates => LayerFileEnum::PredicateDictionaryBlocks,
        DictType::Values => LayerFileEnum::ValueDictionaryBlocks,
    };
    let reader = open_slice(file_name, file_type).await?;

    let mut stream = TfcDictStream::new(reader).enumerate();
    while let Some((ix, element)) = stream.next().await {
        let (element, _) =
            element.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        println!("{}: {:?}", ix + 1, element.to_bytes());
    }

    Ok(())
}

async fn validate_logarray(file_name: PathBuf, header_first: bool) -> std::io::Result<()> {
    let mut file = tokio::fs::File::open(file_name).await?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await?;
    let contents = Bytes::from(contents);

    let _logarray = if header_first {
        LogArray::parse_header_first(contents).unwrap().0
    } else {
        LogArray::parse(contents).unwrap()
    };
    Ok(())
}

async fn extract_file(layer_path: PathBuf, file_name: &str) -> std::io::Result<()> {
    let mut file = tokio::fs::File::open(layer_path).await?;
    let header = ArchiveHeader::parse_from_reader(&mut file).await?;
    let file_type = FILENAME_ENUM_MAP[file_name];
    if let Some(range) = header.range_for(file_type) {
        file.seek(SeekFrom::Current(range.start as i64)).await?;
        let mut reader = ArchiveSliceReader::new(file, range.len());
        let mut output = tokio::io::stdout();
        tokio::io::copy(&mut reader, &mut output).await?;
        output.flush().await?;
    } else {
        panic!("layer did not contain {file_name}");
    }

    Ok(())
}

async fn build_object_index(
    sp_o_nums_file: String,
    sp_o_bits_file: String,
    o_ps_dir: String,
    objects_file: Option<String>,
) -> io::Result<()> {
    let o_ps_dir_path: PathBuf = o_ps_dir.into();
    tokio::fs::create_dir_all(&o_ps_dir_path).await?;

    let sp_o_nums_file = FileBackedStore::new(sp_o_nums_file);
    let sp_o_bits_file = FileBackedStore::new(sp_o_bits_file);
    let objects_file = objects_file.map(|p| FileBackedStore::new(p));

    let mut o_ps_nums_path = o_ps_dir_path.clone();
    o_ps_nums_path.push("nums");
    let mut o_ps_bits_path = o_ps_dir_path.clone();
    o_ps_bits_path.push("bits");
    let mut o_ps_bit_index_blocks_path = o_ps_dir_path.clone();
    o_ps_bit_index_blocks_path.push("bit_index_blocks");
    let mut o_ps_bit_index_sblocks_path = o_ps_dir_path.clone();
    o_ps_bit_index_sblocks_path.push("bit_index_sblocks");

    let o_ps_nums_file = FileBackedStore::new(o_ps_nums_path);
    let o_ps_bits_file = FileBackedStore::new(o_ps_bits_path);
    let o_ps_blocks_file = FileBackedStore::new(o_ps_bit_index_blocks_path);
    let o_ps_sblocks_file = FileBackedStore::new(o_ps_bit_index_sblocks_path);
    let o_ps_files = AdjacencyListFiles {
        bitindex_files: BitIndexFiles {
            bits_file: o_ps_bits_file,
            blocks_file: o_ps_blocks_file,
            sblocks_file: o_ps_sblocks_file,
        },
        nums_file: o_ps_nums_file,
    };

    build_object_index_from_direct_files(sp_o_nums_file, sp_o_bits_file, o_ps_files, objects_file, Some("/tmp/".into()))
        .await
}

async fn build_predicate_index(
    s_p_nums_file: String,
    predicate_index_dir: String,
) -> io::Result<()> {
    let predicate_index_dir_path: PathBuf = predicate_index_dir.into();
    tokio::fs::create_dir_all(&predicate_index_dir_path).await?;

    let s_p_nums_file = FileBackedStore::new(s_p_nums_file);
    let mut wavelet_bits_path = predicate_index_dir_path.clone();
    wavelet_bits_path.push("bits");
    let mut wavelet_blocks_path = predicate_index_dir_path.clone();
    wavelet_blocks_path.push("blocks");
    let mut wavelet_sblocks_path = predicate_index_dir_path.clone();
    wavelet_sblocks_path.push("sblocks");

    let wavelet_bits = FileBackedStore::new(wavelet_bits_path);
    let wavelet_blocks = FileBackedStore::new(wavelet_blocks_path);
    let wavelet_sblocks = FileBackedStore::new(wavelet_sblocks_path);

    builder::build_predicate_index(s_p_nums_file, wavelet_bits, wavelet_blocks, wavelet_sblocks)
        .await
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::NodeId {
            node,
            layer,
            label,
            store,
        } => {
            let store = store.unwrap_or_else(|| ".".to_string());
            let id_for_node = node_id(&store, layer, label, &node);
            match id_for_node {
                Some(id) => println!("{id}"),
                None => println!("None"),
            };
        }
        Commands::IdNode {
            id,
            layer,
            label,
            store,
        } => {
            let store = store.unwrap_or_else(|| ".".to_string());
            let node_for_id = id_node(&store, layer, label, &id);
            match node_for_id {
                Some(id) => println!("{id}"),
                None => println!("None"),
            };
        }
        Commands::NodeCount {
            layer,
            label,
            store,
        } => {
            let store = store.unwrap_or_else(|| ".".to_string());
            let node_count = node_count(&store, layer, label);
            match node_count.await {
                Some(id) => println!("{id}"),
                None => println!("None"),
            };
        }
        Commands::ParseHeader { file_name, sort } => {
            parse_and_print_header(file_name, sort).await;
        }
        Commands::PrintDict {
            file_name,
            dict_type,
        } => print_dict(file_name.into(), dict_type).await.unwrap(),
        Commands::ValidateLogArray {
            file_name,
            header_first,
        } => validate_logarray(file_name.into(), header_first)
            .await
            .unwrap(),
        Commands::Extract {
            layer_file_name,
            file_name,
        } => extract_file(layer_file_name.into(), &file_name)
            .await
            .unwrap(),
        Commands::BuildObjectIndex {
            sp_o_nums_file,
            sp_o_bits_file,
            o_ps_dir,
            objects_file,
        } => build_object_index(sp_o_nums_file, sp_o_bits_file, o_ps_dir, objects_file)
            .await
            .unwrap(),
        Commands::BuildPredicateIndex {
            s_p_nums_file,
            predicate_index_dir,
        } => build_predicate_index(s_p_nums_file, predicate_index_dir)
            .await
            .unwrap(),
        Commands::TripleCount { layer_file } => get_triple_count(layer_file).await.unwrap()
    }
}

async fn parse_and_print_header<P: Into<PathBuf>>(file_name: P, sort: bool) {
    let mut file = tokio::fs::File::open(file_name.into()).await.unwrap();
    let header = ArchiveHeader::parse_from_reader(&mut file).await.unwrap();

    let mut result = Vec::new();
    // annoying code to loop over the segments
    for i in 0..=(LayerFileEnum::Rollup as usize) {
        let file_type = LayerFileEnum::from_usize(i).unwrap();
        if let Some(range) = header.range_for(file_type) {
            let file_name = format!("{file_type:?}");
            result.push((file_name, range.start, range.end, range.len()));
        }
    }
    if sort {
        result.sort_by_key(|x| x.3);
        result.reverse();
    }

    for (file_name, start, end, len) in result {
        println!("{file_name: >50}:\t{: >10}..{: <10} ({})", start, end, len);
    }
}
