use std::comm::SharedChan;
use std::hashmap::HashMap;

fn main() {
    // Reading 10 books from Project Gutenberg
    let mut docs: ~[~str] = ~[~"books/Beowulf.txt", ~"books/Adventures_in_Wonderland.txt", ~"books/Pride_and_Prejudice.txt",
                                ~"books/herlock_Holmes.txt", ~"books/The_Prince.txt", ~"books/Dorian_Gray.txt", ~"books/Dracula.txt",
                                ~"books/Dubliners.txt", ~"books/Great_Expectations.txt", ~"books/Siddhartha.txt"];

    docs = docs.map(|file| {
        readFile(file.to_owned())
    });

    // Defining instance of mapper to get word counts
    fn mapper(doc: ~str) -> ~[(~str, ~str)] {
        let words: ~[~str] = doc.split_iter(' ')
            .filter(|&x| x != "")
            .map(|x| {
                x.to_owned()
            }).collect();

        let mut ret: ~[(~str, ~str)] = ~[];
        for w in words.iter() {
            let tup: (~str, ~str) = (w.to_str(), ~"1");
            ret.push(tup);
        }

        ret
    }

    // Defining instance of reducer to get word counts
    fn reducer(key: ~str, vals: ~[~str]) -> ~[(~str, ~str)] {
        let mut result: int = 0;
        for val in vals.iter() {
            match from_str::<int>(val.to_owned()) {
                None => (),
                Some(a) => {
                    result += a;
                }
            }
        }
        ~[(key, result.to_str())]
    }

    // Launching job
    docs.mapreduce(mapper, reducer);
}

// Exposing trait
trait MapReduce {
    fn mapreduce( &self, extern fn(~str) -> ~[(~str, ~str)], extern fn(~str, ~[~str]) -> ~[(~str, ~str)]);
}

// Implementation of mapreduce trait
impl MapReduce for ~[~str] {
    fn mapreduce( &self, mapper: extern fn(~str) -> ~[(~str, ~str)], reducer: extern fn(~str, ~[~str]) -> ~[(~str, ~str)] ) {
        // First we map in parallel
        let (port, chan): (Port<~[(~str, ~str)]>, Chan<~[(~str, ~str)]>) = stream();
        let chan = SharedChan::new(chan);
        let mut chans: int = 0;
        for doc in self.iter() {
            let child_chan = chan.clone();
            let doc_owned = doc.to_owned();
            chans += 1;
            do spawn {
                let ivals: ~[(~str, ~str)] = mapper(doc_owned.clone());
                child_chan.send(ivals);
            }
        }

        // Aggregate intermediate keys
        let mut key_vals_map: HashMap<~str, ~[~str]> = HashMap::new();
        for _ in range(0, chans) {
            let ivals: ~[(~str, ~str)] = port.recv();

            for ival in ivals.iter() {

		let mut key: ~str;
		let mut val: ~str;
	    	match (*ival).clone() {
		      (a, b) => { key = a; val = b;},
		}		

//                let key = ival.n0();
//                let val = ival.n1();

                if key_vals_map.contains_key_equiv(&key) {
                    let list = key_vals_map.get_mut(&key);
                    list.push(val);
                } else {
                    key_vals_map.find_or_insert(key, ~[val]);
                }
            }
        }

        // Finally, reduce in parallel
        chans = 0;
        key_vals_map.each_key(|key| {
                let values = key_vals_map.get(key);
                let child_chan = chan.clone();
                let key_owned = key.to_owned();
                let values_owned = values.to_owned();
                chans += 1;
                do spawn {
                    let rvals: ~[(~str, ~str)] = reducer(key_owned.clone(), values_owned.clone());
                    child_chan.send(rvals);
                }
                true
            });

        // Print reduced values
        for _ in range(0, chans) {
            let rvals: ~[(~str, ~str)] = port.recv();
            println(fmt!("%?", rvals));
        }
    }
}

// Auxiliary function to read entire file
fn readFile(filename: ~str) -> ~str {
    let contents = std::io::read_whole_file_str(&std::path::PosixPath{
        is_absolute: false,
        components: ~[filename]
    });

    let ret = match contents {
        Ok(T) => T,
        _ => ~""
    };
    ret
}
