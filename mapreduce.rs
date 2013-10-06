use std::comm::SharedChan;
use std::hashmap::HashMap;


fn main() {
    let docs: ~[~str] = ~[~"hello world", ~"hello guys", ~"world"];

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

    docs.mapreduce(mapper, reducer);
}

trait MapReduce {
    fn mapreduce( &self, extern fn(~str) -> ~[(~str, ~str)], extern fn(~str, ~[~str]) -> ~[(~str, ~str)]);
}

impl MapReduce for ~[~str] {
    fn mapreduce( &self, mapper: extern fn(~str) -> ~[(~str, ~str)], reducer: extern fn(~str, ~[~str]) -> ~[(~str, ~str)] ) {
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
        
        let mut key_vals_map: HashMap<~str, ~[~str]> = HashMap::new();
        for _ in range(0, chans) {
            let ivals: ~[(~str, ~str)] = port.recv();
//            println(fmt!("%?", ivals));

//            let mut argv: ~[~str] = ivals.map(|x| x.to_owned()).collect();

            for ival in ivals.iter() {
//                println(fmt!("%?", ival));

                let key = ival.n0();
                let val = ival.n1();

 //               println(fmt!("%?,  %?", key, val));

                if key_vals_map.contains_key_equiv(&key) {
                    let list = key_vals_map.get_mut(&key);
                    list.push(val);
                } else {
                    key_vals_map.find_or_insert(key, ~[val]);
                }
            }
        }


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
        
        for _ in range(0, chans) {
            let rvals: ~[(~str, ~str)] = port.recv();
            println(fmt!("%?", rvals));            
        }
    }
}