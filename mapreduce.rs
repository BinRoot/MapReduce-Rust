use std::comm::SharedChan;

fn main() {
    let (port, chan): (Port<int>, Chan<int>) = stream();
    let chan = SharedChan::new(chan);

    let docs: ~[~str] = ~[~"hello world", ~"hello guys", ~"world"];

    fn mapper(doc: ~str) -> ~str {
        let words: ~[~str] = doc.split_iter(' ')
            .filter(|&x| x != "")
            .map(|x| {
                x.to_owned()
            }).collect();

        for w in words.iter() {
            println(w.to_str() + ", 1");
        }

        ~"in mapper"
    }

    fn reducer(key: ~str, vals: ~[~str]) {

    }

    docs.mapreduce(mapper, reducer);

    

    let vals = [40, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10];
    for i in range(0, vals.len()) {
        let child_chan = chan.clone();
        do spawn {
            let res = fib(vals[i]);
            print("("+i.to_str()+")");
            child_chan.send(res);
        }
    }

    let mut result = 0;
    for i in range(0, vals.len()) {
        result += port.recv();
    }
    
    println("\nresult: "+result.to_str());
}

fn fib(n: int) -> int {
    match n {
        1 => 1,
        2 => 1,
        _ => fib(n-1) + fib(n-2)
    }
}

//fn mapreduce(mapper : extern fn(~str), reducer : extern fn(~str, ~[~str]) ) {


trait MapReduce {
    fn mapreduce( &self, extern fn(~str) -> ~str, extern fn(~str, ~[~str]) );
}

impl MapReduce for ~[~str] {
    fn mapreduce( &self, mapper: extern fn(~str) -> ~str, reducer: extern fn(~str, ~[~str]) ) {
        println("map reducing!!");
        for doc in self.iter() {
            mapper(doc.to_owned());
        }
    }
}