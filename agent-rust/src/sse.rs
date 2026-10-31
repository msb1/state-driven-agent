//! SSE event delivery and response-drop notification have independent lifetimes.
use axum::response::sse::Event;
use futures_util::Stream;
use serde_json::Value;
use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};

pub struct EventStream {
    receiver: mpsc::Receiver<Value>,
    disconnected: Option<oneshot::Sender<()>>,
}

pub fn event_channel() -> (mpsc::Sender<Value>, EventStream, oneshot::Receiver<()>) {
    let (sender, receiver) = mpsc::channel(32);
    let (disconnected, notification) = oneshot::channel();
    (
        sender,
        EventStream {
            receiver,
            disconnected: Some(disconnected),
        },
        notification,
    )
}

impl Stream for EventStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(Some(Ok({
                let mut output = Event::default().data(crate::tools::python_json(&event));
                if let Some(id) = event["event_id"].as_i64() {
                    output = output.id(id.to_string());
                }
                output
            }))),
            Poll::Ready(None) => {
                // All run senders are gone and all queued events were delivered.
                // Close the watcher without reporting a client disconnect.
                self.disconnected.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if let Some(disconnected) = self.disconnected.take() {
            let _ = disconnected.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, response::Sse, routing::get};
    use serde_json::json;
    use std::{sync::Arc, time::Duration};
    use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};

    fn test_app(output: EventStream) -> Router {
        let output = Arc::new(tokio::sync::Mutex::new(Some(output)));
        Router::new().route(
            "/",
            get(move || {
                let output = output.clone();
                async move { Sse::new(output.lock().await.take().unwrap()) }
            }),
        )
    }

    #[tokio::test]
    async fn http_stream_drains_events_and_ends_without_disconnect_on_every_terminal_path() {
        for status in ["complete", "failed", "active", "cancelled"] {
            let (sender, output, disconnected) = event_channel();
            let watcher = tokio::spawn(disconnected);
            let app = test_app(output);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            sender
                .send(json!({"type":"status","message":"started"}))
                .await
                .unwrap();
            sender
                .send(json!({"type":"run_finished","status":status}))
                .await
                .unwrap();
            sender
                .send(json!({"type":"status","message":"finished"}))
                .await
                .unwrap();
            drop(sender);

            let body = timeout(Duration::from_secs(5), async {
                reqwest::get(format!("http://{address}/"))
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap()
            })
            .await
            .expect("HTTP response must end after the run queue drains");
            let events: Vec<Value> = body
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .map(|data| serde_json::from_str(data).unwrap())
                .collect();
            assert_eq!(
                events,
                vec![
                    json!({"type":"status","message":"started"}),
                    json!({"type":"run_finished","status":status}),
                    json!({"type":"status","message":"finished"}),
                ]
            );
            assert!(
                watcher.await.unwrap().is_err(),
                "normal EOF must not request cancellation"
            );
            server.abort();
        }
    }

    #[tokio::test]
    async fn real_socket_disconnect_notifies_while_the_producer_is_still_running() {
        use tokio::io::AsyncReadExt;
        let (sender, output, disconnected) = event_channel();
        let app = test_app(output);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        sender
            .send(json!({"type":"status","message":"running"}))
            .await
            .unwrap();
        let mut received = Vec::new();
        timeout(Duration::from_secs(5), async {
            while !String::from_utf8_lossy(&received).contains("running") {
                let mut buffer = [0; 1024];
                let count = socket.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0);
                received.extend_from_slice(&buffer[..count]);
            }
        })
        .await
        .unwrap();
        socket.shutdown().await.unwrap();
        drop(socket);
        timeout(Duration::from_secs(5), disconnected)
            .await
            .expect("disconnect must be reported before the producer finishes")
            .unwrap();
        assert!(sender.send(json!({"type":"interim_result"})).await.is_err());
        server.abort();
    }
}
