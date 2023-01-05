use clap::*;
use std::io;
use terminus_store::{
    storage::*,
    store::sync::{open_sync_archive_store, SyncStore, SyncStoreLayer},
    Layer,
};

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
        layer: Option<String>,
        /// Label in which to start the lookup
        label: Option<String>,
        /// The workdir to store mappings in
        #[arg(short = 's', long = "store")]
        store: Option<String>,
    },
    /// Node count of layer
    NodeCount {
        layer: Option<String>,
        /// Label in which to start the lookup
        label: Option<String>,
        /// The workdir to store mappings in
        #[arg(short = 's', long = "store")]
        store: Option<String>,
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
    let store = open_sync_archive_store(store);
    let layer = open_layer_or_label(store, layer, label);
    layer.subject_id(node)
}

fn id_node(store: &str, layer: Option<String>, label: Option<String>, id: &str) -> Option<String> {
    let store = open_sync_archive_store(store);
    let layer = open_layer_or_label(store, layer, label);
    layer.id_subject(id.parse().unwrap())
}

fn main() -> io::Result<()> {
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
            Ok(())
        }
        Commands::IdNode {
            id,
            layer,
            label,
            store,
        } => {
            let store = store.unwrap_or_else(|| ".".to_string());
            let id_for_node = id_node(&store, layer, label, &id);
            match id_for_node {
                Some(id) => println!("{id}"),
                None => println!("None"),
            };
            Ok(())
        }
        Commands::NodeCount {
            layer,
            label,
            store,
        } => todo!(),
    }
}
