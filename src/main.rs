use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use anyhow::Result;
use serde_json::json;
use futures_util::{SinkExt, StreamExt};
use url::Url;

#[tokio::main]
async fn main() -> Result<()>{
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let url = Url::parse("wss://api.hyperliquid.xyz/ws")?;
    let (mut ws_stream, _) = connect_async(url.as_str()).await?;
    println!("Websocket client connected");

    let message = json!(
        { 
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": "BTC"}
        });

    ws_stream.send(Message::Text(message.to_string().into())).await?;

    while let Some(msg) = ws_stream.next().await {
        match msg? {
            Message::Text(text) => {
                println!("Received message from server: {}", text);
            }
            _ => {}
        }
    }
    Ok(())
}
