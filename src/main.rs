use clap::*;
use std::io;
use terminus_store::{storage::*, store::sync::SyncStore, Layer};

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
        store: String,
    },
    /// Lookup the node for an id starting at the current layer
    IdNode {
        /// The node
        id: String,
        /// Layer in which to start the lookup
        layer: String,
        /// Label in which to start the lookup
        label: Option<String>,
        /// The workdir to store mappings in
        #[arg(short = 's', long = "store")]
        store: String,
    },
}

/// make this more clever about finding where the store is.
fn open_store(store_path: &str) -> SyncStore {
    open_sync_archive_store(store_path)
}

fn open_layer_or_label(
    store: SyncStore,
    layer: Option<String>,
    label: Option<String>,
) -> Box<SyncStoreLayer> {
    match (layer, label) {
        (None, None) => panic!("You must specify either a layer or a label"),
        (None, Some(label_name)) => store.create(&label_name).unwrap().head(),
        (Some(layer_name), None) => store.get_layer_from_id(layer_name).unwrap(),
        (Some(_), Some(_)) => panic!("You must specify either a layer or a label"),
    };
}

fn node_id(store: &str, layer: Option<String>, label: Option<String>, node: &str) -> String {
    let store = open_store(store);
    let layer = open_layer_or_label(store, layer, label);
    todo!()
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
            let id_for_node = node_id(&store, layer, label, &node);
            println!("{id_for_node}");
            Ok(())
        }
        Commands::IdNode {
            id,
            layer,
            label,
            store,
        } => todo!(),
    }
}
