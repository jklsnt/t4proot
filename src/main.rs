use ignore::Walk;

fn main() {
    for result in Walk::new("./notes") {
	match result {
	    Ok(entry) => {
		if !entry.file_type().unwrap().is_dir() {
		    println!("{:016x}", seahash::hash(
			&std::fs::read_to_string(entry.path().to_str().unwrap()).unwrap().as_bytes()
		    ))
		}
	    }
	    Err(_err) => panic!("Error encountered!")
	}
    }
}

