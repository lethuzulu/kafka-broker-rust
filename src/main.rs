#![allow(unused_imports)]

use std::io::{Read, Write};
use std::mem::size_of;
use std::net::Shutdown;
use std::net::{TcpListener, TcpStream};
use std::str::{from_utf8, from_utf8_unchecked};
use std::thread;
use uuid::Uuid;

#[derive(Debug, Copy, Clone)]
#[repr(i16)]
enum ErrorCode {
    NoError = 0,
    UnsupportedVersion = 35,
}

#[derive(Debug)]
struct ApiVersions {
    error_code: ErrorCode,
    num_api_keys: i8,
    api_keys: Vec<ApiKeys>,
    throttle_time_ms: i32,
}

impl ApiVersions {
    fn size(&self) -> usize {
        size_of::<i16>() +          // error_code
            size_of::<i8>() +           // num_api_keys
            self.api_keys.iter().map(|api_key|api_key.size()).sum::<usize>() +      // api_keys
            size_of::<i32>() // throttle_time_ms
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&(self.error_code as i16).to_be_bytes());
        result.extend_from_slice(&self.num_api_keys.to_be_bytes());
        result.extend(self.api_keys.iter().flat_map(|api_key| api_key.to_bytes()));
        result.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        result
    }
}

#[derive(Debug)]
struct Response {
    message_size: i32,
    header: ResponseHeader,
    body: ResponseBody,
}

impl Response {
    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.message_size.to_be_bytes());
        result.extend_from_slice(&self.header.to_bytes());
        result.extend_from_slice(&self.body.to_bytes());
        result
    }
}

#[derive(Debug)]
struct ResponseHeader {
    correlation_id: i32,
}

impl ResponseHeader {
    fn size(&self) -> usize {
        size_of::<i32>()
    }

    fn to_bytes(&self) -> Vec<u8> {
        self.correlation_id.to_be_bytes().to_vec()
    }
}

#[derive(Debug)]
struct ResponseBody {
    api_versions: ApiVersions,
    tag_buffer: i8,
}

impl ResponseBody {
    fn size(&self) -> usize {
        self.api_versions.size() + size_of::<i8>() * 2 // tag_buffer (used twice in serialization)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.api_versions.to_bytes());
        result.extend_from_slice(&self.tag_buffer.to_be_bytes());
        result.extend_from_slice(&self.tag_buffer.to_be_bytes());
        result
    }
}

#[derive(Debug)]
struct ApiKeys {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl ApiKeys {
    fn size(&self) -> usize {
        size_of::<i16>() * 3 // api_key, min_version, max_version
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&self.api_key.to_be_bytes());
        result.extend_from_slice(&self.min_version.to_be_bytes());
        result.extend_from_slice(&self.max_version.to_be_bytes());
        result
    }
}

#[derive(Debug)]
struct Request {
    message_size: i32,
    header: RequestHeader,
}

#[derive(Debug)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                thread::spawn(move || {
                    println!("accepted new connection");
                    handle_connection(&_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
fn handle_connection(mut stream: &TcpStream) {
    loop {
        let request = parse_request(stream);

        let response = encode_response(&request);
        let kafka_response = response.to_bytes();

        let _ = stream
            .write_all(&kafka_response)
            .map_err(|e| eprintln!("error  {}", e));
    }
}

fn parse_message_size(mut stream: &TcpStream) -> i32 {
    let mut message_size_buf = [0u8; 4];

    let _ = stream
        .read_exact(&mut message_size_buf)
        .map_err(|e| eprintln!("error {}", e));
    let message_size_slice = &message_size_buf[..];
    let message_size_i32 = i32::from_be_bytes(message_size_slice.try_into().unwrap());
    message_size_i32
}

fn decode_request_message(message_size_i32: i32, mut stream: &TcpStream) -> RequestHeader {
    let mut message_buf = vec![0u8; message_size_i32 as usize];
    let _ = stream
        .read_exact(&mut message_buf)
        .map_err(|e| eprintln!("error {}", e));
    let header = decode_request_header(&message_buf);
    header
}

fn decode_request_header(message_buf: &Vec<u8>) -> RequestHeader {
    //request_api_key
    let request_api_key_slice = &message_buf[0..2];
    let request_api_key = i16::from_be_bytes(request_api_key_slice.try_into().unwrap());

    //request_api_version
    let request_api_version_slice = &message_buf[2..4];
    let request_api_version = i16::from_be_bytes(request_api_version_slice.try_into().unwrap());

    //correlation_id
    let correlation_id_slice = &message_buf[4..8];

    let correlation_id = i32::from_be_bytes(correlation_id_slice.try_into().unwrap());

    //client_id
    let size_of_client_id_string = &message_buf[8..10];
    let size_of_client_id_i16 = i16::from_be_bytes(size_of_client_id_string.try_into().unwrap());
    let client_id = if size_of_client_id_i16 > 0 {
        let client_id_string_slice = &message_buf[10..10 + size_of_client_id_i16 as usize];
        let client_id = unsafe { from_utf8_unchecked(client_id_string_slice) };
        Some(client_id.into())
    } else {
        None
    };
    RequestHeader {
        request_api_key,
        request_api_version,
        correlation_id,
        client_id,
    }
}

fn parse_request(stream: &TcpStream) -> Request {
    let message_size = parse_message_size(stream);
    let header = decode_request_message(message_size, stream);
    Request {
        message_size,
        header,
    }
}

fn encode_response_header(request: &Request) -> ResponseHeader {
    ResponseHeader {
        correlation_id: request.header.correlation_id,
    }
}

fn encode_response_body(request: &Request) -> ResponseBody {
    let mut api_keys = Vec::new();
    api_keys.push(ApiKeys {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    });
    api_keys.push(ApiKeys {
        api_key: 1,
        min_version: 0,
        max_version: 16,
    });

    let error_code = match request.header.request_api_version {
        0..=4 => ErrorCode::NoError,
        _ => ErrorCode::UnsupportedVersion,
    };
    let api_versions = ApiVersions {
        error_code,
        num_api_keys: 2,
        api_keys,
        throttle_time_ms: 0,
    };
    let throttle_time_ms = 0;
    ResponseBody {
        api_versions,
        tag_buffer: 0,
    }
}

fn encode_response(request: &Request) -> Response {
    let header = encode_response_header(&request);
    let body = encode_response_body(&request);
    let message_size = (header.size() + body.size()) as i32;
    Response {
        message_size,
        header,
        body,
    }
}

