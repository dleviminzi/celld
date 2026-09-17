// Copyright 2026 Deno Land Inc. Apache-2.0 license.

/// Preserve nested transport errors that reqwest's Display omits.
/// Strip the request URL, which can contain credentials or signed queries.
pub(super) fn response_stream_error(error: reqwest::Error) -> String {
    let error = anyhow::Error::new(error.without_url());
    format!("response stream: {error:#}")
}

#[cfg(test)]
mod tests {
    use super::response_stream_error;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    async fn accept_request(listener: TcpListener) -> TcpStream {
        let (socket, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(socket);
        let mut line = String::new();
        loop {
            line.clear();
            assert!(reader.read_line(&mut line).await.unwrap() > 0);
            if line == "\r\n" {
                return reader.into_inner();
            }
        }
    }

    #[tokio::test]
    async fn response_body_timeout_preserves_the_cause_without_the_url() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/?token=synthetic-secret",
            listener.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let mut socket = accept_request(listener).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nfirst")
                .await
                .unwrap();
            // Keep the connection open: the client must hit its request deadline,
            // not an EOF or connection-reset error.
            let mut remaining = Vec::new();
            let _ = socket.read_to_end(&mut remaining).await;
        });
        let response = reqwest::Client::new()
            .get(&url)
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .unwrap();
        let error = response.bytes().await.unwrap_err();
        server.abort();

        assert!(error.is_timeout());
        // Attach a sensitive URL explicitly, even if this reqwest version does
        // not attach one to body errors, to exercise the redaction boundary.
        let message = response_stream_error(error.with_url(url.parse().unwrap()));
        assert_eq!(
            message,
            "response stream: error decoding response body: request or response body error: operation timed out"
        );
        assert!(!message.contains("synthetic-secret"));
        assert!(!message.contains("127.0.0.1"));
    }

    #[tokio::test]
    async fn truncated_response_preserves_the_non_timeout_cause() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut socket = accept_request(listener).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nfirst")
                .await
                .unwrap();
            socket.shutdown().await.unwrap();
        });
        let response = reqwest::Client::new()
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();
        let error = response.bytes().await.unwrap_err();
        server.await.unwrap();

        assert!(!error.is_timeout());
        let message = response_stream_error(error);
        assert!(message.starts_with("response stream: error decoding response body: "));
        assert!(
            message.contains("end of file before message length reached"),
            "{message}"
        );
        assert!(!message.contains("timed out"));
    }
}
