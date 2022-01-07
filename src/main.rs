use ignore::Walk;
use std::sync::mpsc::{channel, Sender};
use rayon::prelude::*;
use std::path::PathBuf;
use rusqlite::{Connection, params};
use serde_json::Value;
use std::io::Write;
use std::fs;
use notify::{Watcher, RecursiveMode, watcher, DebouncedEvent};
use std::time::SystemTime;
use std::collections::HashMap;
use orgize::elements::*;

#[derive(Debug)]
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
    let now = SystemTime::now();
    let conn = Connection::open("notes.db")
	.unwrap_or_else(|e| panic!("Cannot open database: {}", e));
    conn.execute(
	"CREATE TABLE IF NOT EXISTS input_files (
	     hash TEXT,
	     path TEXT,
	     contents TEXT,
	     meta TEXT
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
		    "INSERT INTO input_files (hash, path, contents, meta) VALUES (?1, ?2, ?3, ?4)",
		    params![i.hash, i.path, i.contents, ""]
		).unwrap_or_else(|e| {log::error!("Insertion of {} into db failed: {}", i.path, e); 0});
	    }
	});
	// Read each path in parallel.
	rx.into_iter().par_bridge().map_with(sink, process_file).collect::<()>();
    });

    let conn = Connection::open("notes.db").unwrap();
    let mut stmt = conn.prepare("SELECT hash, path, contents FROM input_files").unwrap();
    let file_iter = stmt.query_map([], |row| {
	Ok(Entry{
	    hash: row.get(0).unwrap(),
	    path: row.get(1).unwrap(),
	    contents: row.get(2).unwrap(),
	})
    }).unwrap();
    let (metasink, metasource) = channel();
    let files: Vec<Entry> = file_iter.filter_map(|x| x.ok()).collect();
    files.par_iter().map_with(metasink, extract_metadata).collect::<Option<()>>();
    let mut ids = HashMap::new();
    rayon::scope(|s| {
	s.spawn(|_| {
	    for i in metasource.into_iter() {
		let writer = Connection::open("notes.db").unwrap();
		writer.execute(
		    "UPDATE input_files SET meta=?1 WHERE path=?2",
		    params![serde_json::to_string(&i.1).unwrap(), i.0]
		).unwrap();
		if let Some(id) = i.1.get("ID") {
		    ids.insert(id.clone(), i.0);
		}
	    }
	});
    });
    for i in files {
	render_file(&ids, i);
    }
    log::trace!("Built in {:?}.", SystemTime::now().duration_since(now).unwrap());
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
	    process_revision(p).unwrap_or_else(|e| log::error!("Build of revision failed: {}", e));
	    log::trace!("Processed revision in {:?}.", SystemTime::now().duration_since(now).unwrap());
	}
    }
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
	|_row| Ok(true),
    );
    let path = path.into_os_string().into_string().unwrap();
    if !out.unwrap_or(false) {
	match conn.execute(
	    "UPDATE input_files SET contents=?1 WHERE hash=?2",
	    params![contents, hash]
	) {
	    Ok(_) => 1,
	    Err(_) => conn.execute(
		"INSERT INTO input_files (hash, path, contents, meta) VALUES (?1, ?2, ?3, ?4)",
		params![format!("{:016x}", hash), path, contents, ""]
	    ).unwrap()
	};
    }
    Ok(())
    // let mut writer = Vec::new();
    // let org = orgize::Org::parse(&contents);
    // org.write_html(&mut writer).unwrap();
    // fs::create_dir("out")?;
    // // FIXME Hardcoded constants bad
    // log::trace!("{}", String::from("out") + &path[40..path.len()-3] + "html");
    // let mut file = fs::File::create(String::from("out") + &path[40..path.len()-3] + "html").unwrap();
    // file.write_all(&writer)?;
    // Ok(())
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
	path: path.strip_prefix("notes/").unwrap().to_str().unwrap().to_string(),
	contents,
    }).unwrap();
}

fn extract_metadata(sink: &mut Sender<(String, HashMap<String, String>)>, entry: &Entry) -> Option<()> {
    let mut meta: HashMap<String, String> = HashMap::new();
    let org = orgize::Org::parse(&entry.contents);
    let arena = org.arena();
    if let Some(section) = org.document().section_node() {
	for i in section.children(arena) {
	    match arena.get(i).unwrap().get() {
		Element::Drawer(d) => {
		    for j in i.children(arena) {
			for k in j.children(arena) {
			    if let Element::Text{ value } = arena.get(k).unwrap().get() {
				if let Some(parsed) = sscanf::scanf!(value, ":ID:       {}", String) {
				    meta.insert("ID".to_string(), parsed);
				}
			    }
			}
		    }
		},
		Element::Keyword(k) => {meta.insert(k.key.to_string().to_uppercase(), k.value.to_string());},
		_ => (),
	    }
	    // println!("{:?}", arena.get(i).unwrap().get());
	    // println!("{:?}", arena.get(i.children(arena)[0]).unwrap().get());
	}
    }
    // let root: Value = serde_json::from_str(&serde_json::to_string(&org).unwrap()).unwrap();
    // let mut meta: HashMap<String, String> = HashMap::new();
    // for i in root["children"].as_array()? {
    //	if i["type"].as_str()? == "section" {
    //	    for j in i["children"].as_array()? {
    //		if j["type"].as_str()? == "drawer" {
    //		    let id_string = j["children"].as_array()?[0]
    //			["children"].as_array()?[0]["value"].as_str()?;
    //		    if let Some(parsed) = sscanf::scanf!(id_string, ":ID:       {}", String) {
    //			meta.insert("ID".to_string(), parsed);
    //		    }
    //		}
    //		else {meta.insert(j["key"].to_string(), j["value"].to_string());}
    //	    }
    //	    break;
    //	}
    // }
    sink.send((entry.path.clone(), meta));
    Some(())
}

fn render_file(ids: &HashMap<String, String>, entry: Entry) -> Option<()> {
    let mut org = orgize::Org::parse(&entry.contents);
    let mut stack = Vec::new();
    stack.push(org.document().section_node()?);
    while stack.len() > 0 {
	let cur = stack.pop()?;
	let children: Vec<orgize::indextree::NodeId> = cur.children(org.arena()).collect();
	for i in children {
	    let mut elem = org.arena_mut().get_mut(i)?.get_mut();
	    match elem {
		Element::Link(l) => if let Some(s) = sscanf::scanf!(l.path, "id:{}", String) {
		    if let Some(path) = ids.get(&s) {
			*elem = Element::Link(orgize::elements::Link {path: (String::from("file:")+path).into(), desc: l.desc.clone()});\
		    }
		},
		_ => (),
	    }
	    stack.push(i.clone());
	}
    }
    let mut stack = Vec::new();
    stack.push(org.document().section_node()?);
    while stack.len() > 0 {
	let cur = stack.pop()?;
	let children: Vec<orgize::indextree::NodeId> = cur.children(org.arena()).collect();
	for i in children {
	    let mut elem = org.arena().get(i)?;
	    println!("{:?}", elem);
	    match elem {
		Element::Link(l) => println!("{:?}", l),
		_ => (),
	    }
	    stack.push(i.clone());
	}
    }

    println!("{:?}\n\n{}\n\n-----------\n\n", org.arena(), serde_json::to_string(&org).unwrap());
    let mut writer = Vec::new();
    org.write_html(&mut writer).unwrap();
    fs::create_dir("out");
    // FIXME Hardcoded constants bad
    let mut file = fs::File::create(String::from("out/") + &entry.path[..entry.path.len()-3] + "html").unwrap();
    file.write_all(&writer);
    Some(())
}
