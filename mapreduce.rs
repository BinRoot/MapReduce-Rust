use std::comm::SharedChan;
use std::hashmap::HashMap;
use std::hash::Hash;

fn main() {
    // Reading 10 books from Project Gutenberg
    let mut docs: ~[~str] = ~[~"books/Beowulf.txt", ~"books/Adventures_in_Wonderland.txt", ~"books/Pride_and_Prejudice.txt",
                                ~"books/Sherlock_Holmes.txt", ~"books/The_Prince.txt", ~"books/Dorian_Gray.txt", ~"books/Dracula.txt",
                                ~"books/Dubliners.txt", ~"books/Great_Expectations.txt", ~"books/Siddhartha.txt"];

    docs = docs.map(|file| {
        readFile(file.to_owned())
    });

    // Defining instance of mapper to get word counts
    fn mapper(doc: ~str) -> ~[(~str, uint)] {
        let words: ~[~str] = doc.split_iter(' ')
            .filter(|&x| x != "")
            .map(|x| {
                x.to_owned()
            }).collect();

        let mut ret: ~[(~str, uint)] = ~[];
        for w in words.iter() {
            let tup: (~str, uint) = (w.to_str(), 1);
            ret.push(tup);
        }

        ret
    }

    // Defining instance of reducer to get word counts
    fn reducer(key: ~str, vals: ~[uint]) -> ~[(~str, uint)] {
        let mut result: uint = 0;
        for val in vals.iter() {
            result += *val;
        }
        ~[(key, result)]
    }

    // Launching job
    docs.mapreduce::<~str,uint>(mapper, reducer);

}

// Exposing trait
trait MapReduce {
    fn mapreduce<K2: Clone + Send + Hash + Equiv<K2> + Eq, V2: Clone + Send>( &self, extern fn(~str) -> ~[(K2, V2)], extern fn(K2, ~[V2]) -> ~[(K2, V2)] );
}

// Implementation of mapreduce trait
impl MapReduce for ~[~str] {
    fn mapreduce<K2: Clone + Send + Hash + Equiv<K2> + Eq, V2: Clone + Send>( &self, mapper: extern fn(~str) -> ~[(K2, V2)], reducer: extern fn(K2, ~[V2]) -> ~[(K2, V2)] ) {
        // First we map in parallel
        let (port, chan): (Port<~[(K2, V2)]>, Chan<~[(K2, V2)]>) = stream();
        let chan = SharedChan::new(chan);
        let mut chans: int = 0;
        for doc in self.iter() {
            let child_chan = chan.clone();
            let doc_owned = doc.clone();
            chans += 1;
            do spawn {
                let ivals: ~[(K2, V2)] = mapper(doc_owned.clone());
                child_chan.send(ivals);
            }
        }

        // Aggregate intermediate keys
        let mut key_vals_map: HashMap<K2, ~[V2]> = HashMap::new();
        for _ in range(0, chans) {
            let ivals: ~[(K2, V2)] = port.recv();

            for ival in ivals.iter() {

		let mut key: K2;
		let mut val: V2;
	    	match (*ival).clone() {
		      (a, b) => { key = a; val = b;},
		}		

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
                let key_owned = key.clone();
                let values_owned = values.clone();
                chans += 1;
                do spawn {
                    let rvals: ~[(K2, V2)] = reducer(key_owned.clone(), values_owned.clone());
                    child_chan.send(rvals);
                }
                true
            });

        // Print reduced values
        for _ in range(0, chans) {
            let rvals: ~[(K2, V2)] = port.recv();
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
