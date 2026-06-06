use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use futures_util::{SinkExt, StreamExt};
use url::Url;

#[derive(Debug, Deserialize, Serialize)]
struct Quote {
    n: u64,
    px: String,
    sz: String
}

#[derive(Debug, Deserialize, Serialize)]
struct Bbo {
    bbo: Vec<Quote>,
    coin: String,
    time: u64
}

#[derive(Debug, Deserialize, Serialize)]
struct WsMessage {
    channel: String,
    data: serde_json::Value
}

#[tokio::main]
async fn main() -> Result<()>{
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let url = Url::parse("wss://api.hyperliquid.xyz/ws")?;
    let (mut ws_stream, _) = connect_async(url.as_str()).await?;
    println!("Websocket client connected");

    // subscribing message
    let message = json!(
        { 
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": "BTC"}
        });

    ws_stream.send(Message::Text(message.to_string().into())).await?;

    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;

        if msg.is_text() {
            let text = msg.to_text()?;
            let ws_message: WsMessage = serde_json::from_str(text)?;

            if ws_message.channel == "bbo" {
                let bbo: Bbo = serde_json::from_value(ws_message.data)?;

                // Quote has String, and String does not implement Copy so without &, it moves ownership but then Vector loses its value.
                let bid: &Quote = &bbo.bbo[0];
                let ask: &Quote = &bbo.bbo[1];

                println!(
                    "{} bid={}, ask{}, time{}",
                    bbo.coin,
                    bid.px,
                    ask.px,
                    bbo.time
                )
            }
        }
    }
    Ok(())
}
