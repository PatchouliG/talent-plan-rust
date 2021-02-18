use std::net::TcpStream;
use std::io::{Write, Read};
use log::debug;
use stderrlog;
use clap::{App, SubCommand, Arg};
use kvs::{Command, RequestResp, str_to_request_result, resp_to_str};
use std::process::exit;

fn main() {
    // stderrlog::new()
    //     .module(module_path!())
    //     .verbosity(3)
    //     .timestamp(stderrlog::Timestamp::Millisecond)
    //     .init()
    //     .unwrap();

    let args = App::new("My Super Program")
        .version("1.0")
        .author("Kevin K. <kbknapp@gmail.com>")
        .about("Does awesome things")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(Arg::with_name("addr")
            // .short("c")
            .long("addr")
            .value_name("addr")
            .help("serve address")
            .default_value("127.0.0.1:4004")
            .takes_value(true)
        )
        .arg(Arg::with_name("key")
            // .short("c")
            // .long("key")
            .index(2)
            .value_name("key")
            .help("key name")
            .takes_value(true).required(true)
        )
        .arg(Arg::with_name("value")
            // .short("c")
            // .long("value")
            .index(3)
            .value_name("value")
            .help("value")
            .takes_value(true)
        )
        .arg(Arg::with_name("operation")
            // .short("c")
            // .long("operation")
            .index(1)
            .value_name("operation")
            .help("key name")
            .takes_value(true).
            required(true)
        )
        .get_matches();

    debug!("args is {:?}", args);
    let operation = args.value_of("operation").unwrap();
    debug!("operation is {}", operation);
    let key = args.value_of("key").unwrap().to_owned();
    debug!("key is {}", key);
    let value = args.value_of("value").unwrap_or("None").to_owned();
    debug!("value is {}", value);
    let address = args.value_of("addr").unwrap().to_owned();
    debug!("address is {}", address);

    let c = match operation {
        "get" => kvs::Command::Get(key),
        "set" => kvs::Command::Set(key, value),
        "rm" => kvs::Command::Remove(key),
        _ => { panic!("command parse error") }
    };
    request(address, c);
}

fn request(address: String, c: Command) -> RequestResp {
    let mut ts = TcpStream::connect(address.as_str()).unwrap();
    let s = c.toString().into_bytes();
    ts.write(&s);
    let mut buf = [0; 3000];
    let len = ts.read(&mut buf).unwrap();
    let resp = String::from_utf8_lossy(&buf[0..len]).to_string();
    // debug!("resp is {}", resp);
    let res = str_to_request_result(resp);
    debug!("resp is {:?}", res);
    match res.clone() {
        RequestResp::Value(v) => {
            println!("{}", v);
        }
        RequestResp::None => {
            if let Command::Set(_, _) = c {} else if let Command::Get(_) = c {
                println!("Key not found");
            } else {
            }
        }
        RequestResp::Error => {
            if let Command::Set(_, _) = c {} else if let Command::Get(_) = c {
                println!("Key not found");
            } else {
                eprintln!("Key not found");
                exit(1);
            }
        }
    }
    res
}