use std::{io::Read, path::PathBuf};

use bytes::Bytes;
use clap::*;
use terminus_store::{
    storage::{
        archive::{ArchiveHeader, ArchiveLayerStore, DirectoryArchiveBackend},
        consts::LayerFileEnum,
        *,
    },
    store::sync::{open_sync_archive_store, SyncStore, SyncStoreLayer},
    Layer,
};

use num::FromPrimitive;

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
            parse_and_print_header(file_name, sort);
        }
    }
}

fn parse_and_print_header<P: Into<PathBuf>>(file_name: P, sort: bool) {
    let mut file = std::fs::File::open(file_name.into()).unwrap();
    let mut data = Vec::new();
    file.read_to_end(&mut data).unwrap();

    let (header, _) = ArchiveHeader::parse(Bytes::from(data));
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
