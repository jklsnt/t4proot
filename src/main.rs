use ignore::Walk;
use std::sync::mpsc::{channel, Sender};
use rayon::prelude::*;
use std::path::PathBuf;
use rusqlite::{Connection, params};
use serde_json::{json, to_string};
use std::io::Write;
use std::fs;
use notify::{Watcher, RecursiveMode, watcher, DebouncedEvent};
use std::time::SystemTime;

struct Entry {
    hash: String,
    path: String,
    contents: String,
}

/// Initializes the database if it does not exist.
///
/// Creates the `input_files` table, reads through all of the files in the notes
/// directory, and inserts their hash+path+contents into the `input_files` table.
fn init_db() {
    let conn = Connection::open("notes.db")
	.unwrap_or_else(|e| panic!("Cannot open database: {}", e));
    conn.execute(
	"CREATE TABLE IF NOT EXISTS input_files (
	     hash TEXT,
	     path TEXT,
	     contents TEXT
	 )",
	[],
    ).unwrap_or_else(|e| panic!("Cannot create table: {}", e));

    // Channel for communication between threads reading files and thread that inserts into SQL db
    let (sink, source) = channel::<Entry>();
    // Channel for communication between walker thread and threads reading files
    let (tx, rx) = channel();

    rayon::scope(|s| {
	// Thread that recursively walks through input dir while respecting gitignore
	s.spawn(move |_| {
	    for result in Walk::new("notes") {
		match result {
		    Ok(entry) => {
			log::trace!("Walker found path {}", entry.path().display());
			if let Some(filetype) = entry.file_type() {
			    if filetype.is_dir() { continue }
			} else { log::error!("Could not fetch file type of {}.", entry.path().display()); }
			// Send file path to file reader threads
			tx.send(entry.into_path())
			    .unwrap_or_else(|e| log::error!("Walker unable to send path to reader thread: {}", e));
		    },
		    Err(e) => log::error!("Error during walk: {}", e),
		}
	    }
	});
	// Thread that inserts paths into db sequentially.
	s.spawn(move |_| {
	    let writer = Connection::open("notes.db")
		.unwrap_or_else(|e| panic!("Cannot open database: {}", e));
	    for i in source.into_iter() {
		writer.execute(
		    "INSERT INTO input_files (hash, path, contents) VALUES (?1, ?2, ?3)",
		    params![i.hash, i.path, i.contents]
		).unwrap_or_else(|e| {log::error!("Insertion of {} into db failed: {}", i.path, e); 0});
	    }
	});
	// Read each path in parallel.
	rx.into_iter().par_bridge().map_with(sink, process_file).collect::<()>();
    });
}

fn main() {
    env_logger::init();
    if !fs::metadata("notes.db").is_ok() {
	init_db();
    }

    // Channel for receiving filesystem events
    let (tx, rx) = channel();
    let mut watcher = watcher(tx, std::time::Duration::from_millis(250))
	.unwrap_or_else(|e| panic!("Failed to create watcher: {}", e));
    watcher.watch("notes", RecursiveMode::Recursive)
	.unwrap_or_else(|e| panic!("Failed to watch directory: {}", e));
    loop {
	let mut revision_file: Option<PathBuf> = None;
	match rx.recv() {
	    Ok(event) => {
		match event {
		    DebouncedEvent::Write(p) => {
			log::trace!("Write to {} detected", p.display());
			revision_file = Some(p);
		    },
		    DebouncedEvent::Create(p) => {
			log::trace!("Creation of {} detected", p.display());
			revision_file = Some(p);
		    },
		    DebouncedEvent::Rename(o, n) => {
			log::trace!("Rename of {} to {} detected", o.display(), n.display());
			revision_file = Some(n);
		    },
		    _ => {
			log::trace!("Other file event noticed");			
		    }
		}
	    },
	    Err(e) => log::error!("File watcher error: {:?}", e),
	}
	if let Some(p) = revision_file {
	    let now = SystemTime::now();
	    process_revision(p);
	    log::trace!("Processed revision in {:?}.",  SystemTime::now().duration_since(now).unwrap());	
	}
    }

    // let conn = Connection::open("notes.db").unwrap();
    // let mut stmt = conn.prepare("SELECT hash, path, contents FROM input_files").unwrap();
    // let file_iter = stmt.query_map([], |row| {
    //	Ok(Entry{
    //	    hash: row.get(0).unwrap(),
    //	    path: row.get(1).unwrap(),
    //	    contents: row.get(2).unwrap(),
    //	})
    // }).unwrap();
    // file_iter.map(render_file).collect::<()>();
}

fn process_revision(path: PathBuf) -> anyhow::Result<()> {
    let ignore = ignore::gitignore::Gitignore::new("notes/.gitignore").0; // FIXME handle error
    if let ignore::Match::Ignore(_p) = ignore.matched(path.clone(), false) { return Ok(()); }
    log::trace!("Processing {}", path.clone().display());
    let file = fs::File::open(path.clone())?;
    let contents = fs::read_to_string(path.clone()).unwrap(); // TODO Super naive
    let hash = seahash::hash(&contents.as_bytes());
    let conn = Connection::open("notes.db").unwrap();
    let out = conn.query_row(
	"SELECT hash FROM input_files WHERE hash=(?1)",
	params![hash],
	|row| Ok(true),
    );
    let path = path.into_os_string().into_string().unwrap();
    if !out.unwrap_or(false) {
	match conn.execute(
	    "UPDATE input_files SET contents=?1 WHERE id=?2",
	    params![contents, hash]
	) {
	    Ok(_) => 1,
	    Err(_) => conn.execute(
		"INSERT INTO input_files (hash, path, contents) VALUES (?1, ?2, ?3)",
		params![format!("{:016x}", hash), path, contents]
	    ).unwrap()
	};
    }
    
    let mut writer = Vec::new();
    let org = orgize::Org::parse(&contents);
    org.write_html(&mut writer).unwrap();
    fs::create_dir("out");
    // FIXME Hardcoded constants bad
    log::trace!("{}", String::from("out") + &path[40..path.len()-3] + "html");
    let mut file = fs::File::create(String::from("out") + &path[40..path.len()-3] + "html").unwrap();
    file.write_all(&writer);
    Ok(())
}

/// Reads and hashes a file, then sends to thread writing to db.
///
/// Should only really be called at database generation.
///
/// # Arguments
///
/// * `sink` - A Sender to the thread responsible for writing to the database.
/// * `path` - Path to the file to be processed.
///
/// # Examples
///
/// ```
/// use std::sync::mpsc::channel;
/// let (tx, rx) = channel()
/// process_file(tx, Path::new("foo.org").to_path_buf());
/// ```
fn process_file(sink: &mut Sender<Entry>, path: PathBuf) {
    let contents = fs::read_to_string(path.clone()).unwrap(); // TODO Super naive
    let hash = seahash::hash(&contents.as_bytes());
    sink.send(Entry {
	hash: format!("{:016x}", hash),
	path: path.into_os_string().into_string().unwrap(),
	contents,
    }).unwrap();
}

fn render_file(entry: Result<Entry, rusqlite::Error>) {
    let entry = entry.unwrap();
    let mut writer = Vec::new();
    let org = orgize::Org::parse(&entry.contents);
    org.write_html(&mut writer).unwrap();
    println!("{}", String::from("out") + &entry.path[7..entry.path.len()-3] + "html");
    fs::create_dir("out");
    // FIXME Hardcoded constants bad
    let mut file = fs::File::create(String::from("out") + &entry.path[7..entry.path.len()-3] + "html").unwrap();
    file.write_all(&writer);
}
