use std::borrow::Cow;
use std::sync::Arc;
use papaya::HashMap;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::*;
use tracing::{Level, debug};
use tracing_subscriber::FmtSubscriber;

enum Response<T> {
    Success(T),
    Failure,
}

impl<T> Response<T> where T: ToString {
    fn parse(&self) -> Vec<u8> {
        let mut result = match self {
            Response::Success(data) => data.to_string().into_bytes(),
            Response::Failure => "false".as_bytes().to_vec(),
        };
        result.push(b'\n');
        result
    }

}

enum Command {
    Read,
    Write,
    Delete,
    Status,
    Error,
}

impl  Command {
    fn from_op(op: &[u8]) -> Command {
        match String::from_utf8_lossy(&op).replace('\0', "").as_str() {
            "read" => Command::Read,
            "write" => Command::Write,
            "delete" => Command::Delete,
            "status" => Command::Status,
            v => {
                debug!("Unknown command: ~{:?}~", v);
                Command::Error
            },
        }
    }
}


async fn handle_connection(mut socket: TcpStream, state: Arc<HashMap<String, String>>) {
    let mut op = [0; 6];
    let mut found = false;

    if let Ok(byte) = socket.read_u8().await {
        match byte {
            b'r' | b's' | b'd' | b'w' => {
                found = true;
                op[0] = byte;
                match byte {
                    b'r' => {
                        socket.read_exact(&mut op[1..4]).await.unwrap();
                        socket.read_u8().await.unwrap();
                    }
                    b's' => {
                        socket.read_exact(&mut op[1..6]).await.unwrap();
                    }
                    b'd' => {
                        socket.read_exact(&mut op[1..6]).await.unwrap();
                    }
                    b'w' => {
                        socket.read_exact(&mut op[1..5]).await.unwrap();
                    }
                    _ => return,
                }
            }
            _ => {
                socket.write_all(Response::<bool>::Failure.parse().as_ref()).await.unwrap();
                return;
            }
        }
    }

    debug!("-{}- found: {}", String::from_utf8_lossy(&op), found);
    if found {
        parse_commands(state, &mut socket, &op).await;
    }


}


async fn parse_commands(state: Arc<HashMap<String, String>>, socket: &mut TcpStream, op: &[u8]) {
    let mut buffer = Vec::new();
    debug!("Parsing command...");

    match Command::from_op(&op) {
        Command::Write => {
            socket.read_to_end(&mut buffer).await.unwrap();

            match String::from_utf8(buffer.to_vec()).unwrap().split_once("|") {
                Some((key, value)) => {
                    debug!("Writing key: '{}' value: '{}'", key.trim(), value.trim());
                    state.pin().insert(key.trim().to_string(), value.trim().to_string());
                    socket.write_all(&Response::<bool>::Success(true).parse()).await.unwrap();
                },
                None => {
                    socket.write_all(&Response::<bool>::Failure.parse()).await.unwrap();
                }
            }

        },
        Command::Read => {
            debug!("Reading...");
            socket.read_to_end(&mut buffer).await.unwrap();

            let key = String::from_utf8(buffer.to_vec()).unwrap().trim().to_string();


            let r = {
                let state_ref = state.pin();
                state_ref.get(&key).map(|v| v.to_string())
            };


            debug!("Read {} bytes, '{}' <{:?}>", buffer.len(), key, r);

            match r {
                Some(v) => {
                    socket.write_all(&Response::<String>::Success(v.into()).parse()).await.unwrap();
                },
                None => {
                    socket.write_all(&Response::<bool>::Failure.parse()).await.unwrap();
                }
            };

        },
        Command::Delete => {
            socket.read_to_end(&mut buffer).await.unwrap();

            let key = String::from_utf8(buffer.to_vec()).unwrap();

            if state.pin().remove(&key).is_some() {
                socket.write_all(&Response::<bool>::Success(true).parse()).await.unwrap();
            } else {
                socket.write_all(&Response::<bool>::Failure.parse()).await.unwrap();
            }


        },
        Command::Status => socket.write_all(&Response::<u32>::Success(10).parse()).await.unwrap(),
        Command::Error => socket.write_all(&Response::<bool>::Failure.parse()).await.unwrap(),
    }
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let addr = "127.0.0.1:6969";
    let listener = TcpListener::bind(addr).await.unwrap();

    debug!("Server running on {}", addr);

    let state: Arc<HashMap<String, String>> = Arc::new(HashMap::new());


    loop {
        let (socket, _) = listener.accept().await.unwrap();


        let state = Arc::clone(&state);
        tokio::spawn(async move{
            handle_connection(socket, state).await;
        });
    }
}