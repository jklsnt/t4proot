use ignore::Walk;
use std::sync::mpsc::{channel, Sender};
use rayon::prelude::*;
use std::path::PathBuf;
use rusqlite::{Connection, params};
use env_logger::{Builder, Target};
use serde_json::{json, to_string};
use std::io::Write;

struct Entry {
    hash: String,
    path: String,
    contents: String,
}

fn main() {
    env_logger::init();
    let conn = Connection::open("notes.db").unwrap();
    conn.execute(
	"CREATE TABLE IF NOT EXISTS input_files (
             hash TEXT,
             path TEXT,
             contents TEXT
         )",
	[],
    ).unwrap();

    // Channel for communication between threads reading files and thread that inserts into SQL db
    let (sink, source) = channel::<Entry>();
    // Channel for communication between walker thread and threads reading files
    let (tx, rx) = channel();

    rayon::scope(|s| {
	// Thread that recursively walks through input dir while respecting gitignore 
	s.spawn(move |_| {
	    for result in Walk::new("./notes") {
		match result {
		    Ok(entry) => {
			log::trace!("Walker found path {}", entry.path().display());
			if let Some(filetype) = entry.file_type() {
			    if filetype.is_dir() { continue }
			} else { log::error!("Could not fetch file type of {}.", entry.path().display()); }
			// Send file path to file reader threads
			tx.send(entry.into_path()).unwrap();
		    },
		    Err(e) => log::error!("Error during walk: {}", e),
		}
	    }
	});
	// Thread that inserts paths into db sequentially.
	s.spawn(move |_| {
	    let writer = Connection::open("notes.db").unwrap();
	    for i in source.into_iter() {
		writer.execute(
		    "INSERT INTO input_files (hash, path, contents) VALUES (?1, ?2, ?3)",
		    params![i.hash, i.path, i.contents]
		).unwrap();
	    }
	});
	// Read each path in parallel.
	rx.into_iter().par_bridge().map_with(sink, process_file).collect::<()>();
    });

    let mut stmt = conn.prepare("SELECT hash, path, contents FROM input_files").unwrap();
    let file_iter = stmt.query_map([], |row| {
	Ok(Entry{
	    hash: row.get(0).unwrap(),
	    path: row.get(1).unwrap(),
	    contents: row.get(2).unwrap(),
	})
    }).unwrap();
    file_iter.map(render_file).collect::<()>();
}

fn process_file(sink: &mut Sender<Entry>, path: PathBuf) {
    let path2 = path.clone(); // supremely dumb
    let contents = std::fs::read_to_string(path).unwrap(); // TODO Super naive
    let hash = seahash::hash(&contents.as_bytes());    
    sink.send(Entry {
	hash: format!("{:016x}", hash),
	path: path2.into_os_string().into_string().unwrap(),
	contents,
    }).unwrap();
}

fn render_file(entry: Result<Entry, rusqlite::Error>) {
    let entry = entry.unwrap();
    let mut writer = Vec::new();
    let org = orgize::Org::parse(&entry.contents);
    org.write_html(&mut writer).unwrap();
    println!("{}", String::from("./out") + &entry.path[7..entry.path.len()-3] + "html");
    std::fs::create_dir("./out");
    // FIXME Hardcoded constants bad
    let mut file = std::fs::File::create(String::from("./out") + &entry.path[7..entry.path.len()-3] + "html").unwrap();
    file.write_all(&writer);
}
