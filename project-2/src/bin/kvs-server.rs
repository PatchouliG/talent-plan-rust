use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use clap::{App, Arg, SubCommand};
use log::{debug, error, info, warn};
use warp::Filter;
use kvs::{Command, KvsEngine, getStore, EngineType, resp_to_str, RequestResp, GetRequestResp};
use std::path::{PathBuf, Path};

fn main() {
    // stderrlog::new()
    //     .module(module_path!())
    //     .verbosity(3)
    //     .timestamp(stderrlog::Timestamp::Millisecond)
    //     .init()
    //     .unwrap();

    let args = App::new("kvs server")
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
                 .takes_value(true),
        )
        // kvs or sled
        .arg(Arg::with_name("engine")
                 .short("e")
                 .long("engine")
                 .value_name("engine")
                 .help("engine type")
                 .default_value("kvs")
                 .takes_value(true),
        )
        .get_matches();

    debug!("args is {:?}", args);
    let address = args.value_of("addr").unwrap();
    debug!("address is {}", address);
    let engine = args.value_of("engine").unwrap();
    debug!("engine is {}", engine);

    let engineType = match engine {
        "kvs" => EngineType::kvs,
        "sled" => EngineType::sled,
        _ => panic!("engine type "),
    };
    let p = workDir(&engineType);
    let mut store = getStore(engineType, p.as_path());

    let mut l = TcpListener::bind(address).unwrap();

    for streamResult in l.incoming() {
        if streamResult.is_err() {
            error!("create connection error");
            continue;
        }
        handleRequest(streamResult.unwrap(), store.as_mut());
    }
}

fn workDir(et: &EngineType) -> PathBuf {
    let path = std::env::current_dir().unwrap();
    let res = match et {
        EngineType::sled => {
            path.join("sled")
        }
        EngineType::kvs => {
            path.join("kvs")
        }
    };
    if !Path::new(&res).exists() {
        std::fs::create_dir(&res);
    }

    res.to_owned()
}

fn handleRequest(mut ts: TcpStream, store: &mut KvsEngine) {
    let mut buf = [0; 3000];
    let len = ts.read(&mut buf).unwrap();
    let request_str = String::from_utf8_lossy(&buf[0..len]).to_string();
    let c = Command::fromString(&request_str);
    let resp: RequestResp = match c {
        Command::Get(key) => {
            let res = store.get(key);
            match res {
                Err(_) => {
                    RequestResp::Error
                }
                Ok(value) => {
                    match value {
                        None => RequestResp::None,
                        Some(v) => RequestResp::Value(v)
                    }
                }
            }
        }
        Command::Set(key, value) => {
            let res = store.set(key, value);
            match res {
                Err(_) => RequestResp::Error,
                Ok(_) => RequestResp::None,
            }
        }

        Command::Remove(key) => {
            let res = store.remove(key);
            match res {
                Err(_) => RequestResp::Error,
                Ok(_) => RequestResp::None,
            }
        }
    };
    let res = resp_to_str(resp);
    ts.write(res.as_bytes());
    // ts.write(&mut buf);
}
