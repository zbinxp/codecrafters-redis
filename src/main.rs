#![allow(unused_imports)]
use std::{error::Error, io::{BufRead, BufReader, Write}, net::{TcpListener, TcpStream}};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(_s) => {
                println!("accepted new connection");
                
                match handle_connection(_s) {
                    Ok(_) => {

                    },
                    Err(e) => {
                        println!("got error when handling connection: {}", e);
                    }
                }
            },
            Err(e) => {
                println!("Error occured:{}", e)
            }
        }
    }

    println!("redis server down");
}


fn handle_connection(mut stream: TcpStream) -> Result<(), std::io::Error> {
    // use BufReader to read data
    let reader = BufReader::new(&stream);
    let request_line = reader.lines().next().unwrap()?;
    println!("request line:{}", request_line);

    let response = "+PONG\r\n";
    stream.write_all(response.as_bytes()).unwrap();
    Ok(())
}