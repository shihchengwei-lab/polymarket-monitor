use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub(crate) const GAMMA_FIXTURE: &str = r#"{"slug":"fixture","title":"Fixture","closed":false,"negRisk":true,"markets":[
    {"question":"First","conditionId":"a","outcomes":["Yes","No"],
     "clobTokenIds":["1","2"],"orderPriceMinTickSize":0.01,"closed":false},
    {"question":"Second","conditionId":"b","outcomes":["Yes","No"],
     "clobTokenIds":["3","4"],"orderPriceMinTickSize":0.01,"closed":false}
]}"#;
pub(crate) const CLOSED: &str = r#"{"slug":"fixture","closed":true}"#;

pub(crate) async fn http_request(listener: &TcpListener) -> (TcpStream, String) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut buf = [0; 1024];
        let size = socket.read(&mut buf).await.unwrap();
        assert!(size > 0 && request.len() < 8192);
        request.extend_from_slice(&buf[..size]);
    }
    let request = String::from_utf8(request).unwrap();
    (socket, request.lines().next().unwrap().to_owned())
}

pub(crate) async fn http_response(socket: TcpStream, body: &str) {
    http_response_with_status(socket, "200 OK", body).await;
}

pub(crate) async fn http_response_with_status(mut socket: TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();
}
