use log::{info, error, warn, debug};
use stderrlog::Timestamp;
use std::collections::HashMap;


fn main() {
    let res = reqwest::blocking::get("http://bilibili.com").
        unwrap().text().unwrap();

    println!("body = {:?}", res);
}