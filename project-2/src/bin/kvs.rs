use std::process::exit;

use clap::{App, Arg, SubCommand};

use kvs::{KvStore, Result};
use std::path::Path;


fn main() -> Result<()> {
    let matches = App::new("My Super Program")
        .version("1.0")
        .author("Kevin K. <kbknapp@gmail.com>")
        .about("Does awesome things")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(SubCommand::with_name("set")
            .arg(Arg::with_name("key")
                .index(1)
                .value_name("key"))
            .arg(Arg::with_name("value")
                .index(2)
                .value_name("value"))
        )
        .subcommand(SubCommand::with_name("get")
            .arg(Arg::with_name("key")
                .index(1)
                .value_name("key"))
        )
        .subcommand(SubCommand::with_name("rm")
            .arg(Arg::with_name("key")
                .value_name("key"))
        )
        .get_matches();

    let mut kvs = KvStore::open(std::env::current_dir().unwrap().as_path())?;
    if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("key").unwrap();
        let value = kvs.get(key.to_owned())?;
        match value {
            None => { println!("Key not found"); }
            Some(v) => {
                println!("{}", v);
            }
        };
        exit(0);
    } else if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("key").unwrap().to_owned();
        let value = matches.value_of("value").unwrap().to_owned();
        kvs.set(key, value)?;
        exit(0);
    } else if let Some(matches) = matches.subcommand_matches("rm") {
        let key = matches.value_of("key").unwrap().to_owned();
        kvs.remove(key).map_err(|_| {
            println!("Key not found");
            exit(1);
        });
        exit(0);
    } else {
        exit(1);
    }
}