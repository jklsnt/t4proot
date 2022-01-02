use ignore::Walk;
use std::sync::mpsc::{channel, Sender, Receiver};
use rayon::prelude::*;
use std::path::PathBuf;
use rusqlite::Connection;
use env_logger::{Builder, Target};

fn main() {
    Builder::new()
	.target(Target::Stdout)
	.filter(None, log::LevelFilter::Trace)
        .init();
    let conn = Connection::open("notes.db").unwrap();
    conn.execute(
	"CREATE TABLE IF NOT EXISTS input_files (
             hash TEXT,
             path TEXT,
             contents TEXT
        )",
	[],
    ).unwrap();
    let (tx, rx) = channel();

    //    
    //  If you're bored, please help me figure out why in the love of God `rx` would be dropped here
    //  `tx` errors on the `send` method and that's defined to only ever error when the Receiver is deallocated.
    //  Why is this happening. help help help
    //  Ping me if you find out
    //
    
    rayon::scope(|s| {
	s.spawn(move |_| {
	    for result in Walk::new("./notes") {
		match result {
		    Ok(entry) => {
			log::trace!("Walker found path {}", entry.path().display());
			if let Some(filetype) = entry.file_type() {
			    if filetype.is_dir() { continue }
			} else { log::error!("Could not fetch file type of {}.", entry.path().display()); }			
			tx.send(entry.into_path()).unwrap_or(log::error!("Could not send Entry's PathBuf to processing thread."));			
		    },
		    Err(e) => log::error!("Error during walk: {}", e),
		}
	    }
	});
	rx.into_iter().par_bridge().map(process_file).collect::<()>();
    });	   
}

fn process_file(path: PathBuf) {
    // let conn = Connection::open("notes.db").unwrap();
    let contents = std::fs::read_to_string(path).unwrap(); // TODO Super naive
    let hash = seahash::hash(&contents.as_bytes());
    //conn.execute("INSERT INTO input_files VALUES 
}

