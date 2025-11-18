mod storage;

use std::sync::Arc;
use papaya::HashMap;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::*;
use tokio::sync::{Mutex, RwLock};
use tracing::{Level, debug};
use tracing_subscriber::FmtSubscriber;
use tokio::signal;

enum Response<T>{
    Success(T),
    Failure,
}





impl Response<String>  {
    fn parse(&self) -> Vec<u8> {
        let mut result = match self {
            Response::Success(data) => data.to_string().into_bytes(),
            Response::Failure => "error".as_bytes().to_vec(),
        };
        result.push(b'\r');
        result
    }

}

impl Response<&[u8]>  {
    fn parse(&self) -> Vec<u8> {
        let mut result = match self {
            Response::Success(data) => data.to_vec(),
            Response::Failure => "error".as_bytes().to_vec(),
        };
        result.push(b'\r');
        result
    }

}

enum Command {
    Read,
    Reads,
    Write,
    Delete,
    Status,
    Keys,
    Error,
}

impl  Command {
    fn from_op(op: &[u8]) -> Command {
        match String::from_utf8_lossy(&op).replace('\0', "").trim() {
            "read" => Command::Read,
            "reads" => Command::Reads,
            "write" => Command::Write,
            "delete" => Command::Delete,
            "status" => Command::Status,
            "keys" => Command::Keys,
            v => {
                debug!("Unknown command: ~{:?}~", v);
                Command::Error
            },
        }
    }
}


async fn handle_connection(socket: TcpStream, state: Arc<RwLock<storage::Storage>>) {
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        let mut buffer = Vec::new();
        reader.read_until(b'\r', &mut buffer).await.unwrap();


        if buffer.len() == 0 {
            debug!("Connection closed");
            return;
        }

        match String::from_utf8(buffer.to_vec()).unwrap().split_once(' ')  {
            Some((op, rest)) => {
                debug!("Operation: ~{}~", op.trim());
                debug!("AAAAAAAAa: ~{}~", rest.trim());

                let _ =  match Command::from_op(op.trim().as_bytes()) {
                    Command::Read =>
                        match state.read().await.get_by_btree(rest.trim().as_bytes()){
                            Some(v) => {
                            debug!("Read key: '{}' value: '{:?}'", rest.trim(), v);
                            write_half.write(&Response::Success(v).parse()).await.unwrap()
                        }
                            None => {
                                debug!("Read key: '{}' not found", rest.trim());
                                write_half.write(&Response::<&[u8]>::Failure.parse()).await.unwrap()
                            }
                        },
                    Command::Reads => write_half.write(&Response::<String>::Success("Reads".into()).parse()).await.unwrap(),
                    Command::Write => {
                        match rest.trim().split_once("|") {
                            Some((key, value)) => {
                                debug!("Writing key: '{}' value: '{}'", key.trim(), value.trim());
                                state.write().await.add(key.trim().as_bytes(), value.trim().as_bytes()).unwrap();
                                write_half.write(&Response::<String>::Success("Success".into()).parse()).await.unwrap()
                            },
                            None => {
                                write_half.write(&Response::<String>::Failure.parse()).await.unwrap()
                            }
                        }
                    },
                    Command::Delete => write_half.write(&Response::<String>::Success("Delete".into()).parse()).await.unwrap(),
                    Command::Error => write_half.write(&Response::<String>::Success("Error".into()).parse()).await.unwrap(),
                    _ => write_half.write(&Response::<String>::Failure.parse()).await.unwrap(),
                };
            }
            None => {
                let _ = match Command::from_op(String::from_utf8(buffer.to_vec()).unwrap().trim().as_bytes()) {
                    Command::Status => {
                        write_half.write(&Response::<String>::Success("well going our operation".into()).parse()).await.unwrap()
                    },
                    Command::Keys => write_half.write(&Response::<String>::Success("key1,key2,key3".into()).parse()).await.unwrap(),
                    _ => {
                        write_half.write(&Response::<String>::Failure.parse()).await.unwrap()
                    }
                };
            },
        }
    }
}


#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let addr = "127.0.0.1:6969";
    let listener = TcpListener::bind(addr).await.unwrap();

    debug!("Server running on {}", addr);

    let state = Arc::new(RwLock::new(storage::Storage::open(std::path::PathBuf::from("data.store"), 10 * 1024 * 1024 * 1024).unwrap()));


    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, _)) => {
                        debug!("New connection from {}", socket.peer_addr().unwrap());
                        let state = Arc::clone(&state);
                        tokio::spawn(async move {
                            handle_connection(socket, state).await;
                        });
                    }
                    Err(e) => {
                        debug!("Error accepting connection: {}", e);
                    }
                }
            }
            _ = signal::ctrl_c() => {
                debug!("Received shutdown signal");
                state.write().await.flush().unwrap();
                break;
            }
        }
    }
}