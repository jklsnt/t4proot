use ignore::Walk;
use std::sync::mpsc::{channel, Sender};
use rayon::prelude::*;
use std::path::PathBuf;
use rusqlite::{Connection, params};
use env_logger::{Builder, Target};
use serde_json::{json, to_string};


struct Entry {
    hash: String,
    path: String,
    contents: String,
}

fn main() {

    // initialize our logger
    Builder::new()
	.target(Target::Stdout)
	.filter(None, log::LevelFilter::Trace)
        .init();

    // open a connection to our database
    let conn = Connection::open("notes.db").unwrap();

    // create a table for our input files if it doesn't exist
    conn.execute(
	"CREATE TABLE IF NOT EXISTS input_files (
             hash TEXT,
             path TEXT,
             contents TEXT
         )",
	[],
    ).unwrap();

    let (sink, source) = channel::<Entry>(); // TODO @david add comments for these please
    let (tx, rx) = channel();

    // threading things: recursively traverse the notes directory and fetch all the files
    rayon::scope(|s| {
	s.spawn(move |_| {
	    for result in Walk::new("./notes") {
		match result {
		    Ok(entry) => {
			//log::trace!("Walker found path {}", entry.path().display());
			if let Some(filetype) = entry.file_type() {
			    if filetype.is_dir() { continue }
			} else { log::error!("Could not fetch file type of {}.", entry.path().display()); }
			tx.send(entry.into_path()).unwrap();
		    },
		    Err(e) => log::error!("Error during walk: {}", e),
		}
	    }
	});
	s.spawn(move |_| {
	    let writer = Connection::open("notes.db").unwrap();
	    for i in source.into_iter() {
		writer.execute(
		    "INSERT INTO input_files (hash, path, contents) VALUES (?1, ?2, ?3)",
		    params![i.hash, i.path, i.contents]
		).unwrap();
	    }
	});
	rx.into_iter().par_bridge().map_with(sink, process_file).collect::<()>();
    });
}

fn process_file(sink: &mut Sender<Entry>, path: PathBuf) {
    let path2 = path.clone(); // supremely dumb
    let contents = std::fs::read_to_string(path).unwrap(); // TODO Super naive
    let hash = seahash::hash(&contents.as_bytes());

    // convert to string
    let org = orgize::Org::parse(&contents);
    println!("{}", to_string(&org).unwrap());

    sink.send(Entry {
	hash: format!("{:016x}", hash),
	path: path2.into_os_string().into_string().unwrap(),
	contents,
    }).unwrap();
    //let kill_me = o.headlines().first().headline_node();
}

































