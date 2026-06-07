use tokio::time;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use futures_util::{SinkExt, StreamExt};
use url::Url;
use std::fs::OpenOptions;
use csv;

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
    data: Option<serde_json::Value>
}

#[tokio::main]
async fn main() -> Result<()>{
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let url = Url::parse("wss://api.hyperliquid.xyz/ws")?;
    let (ws_stream, _) = connect_async(url.as_str()).await?;
    let (mut write, mut read) = ws_stream.split();

    println!("Websocket client connected");

    // subscribing message
    let message = json!(
        { 
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": "BTC"}
        });

    write.send(Message::Text(message.to_string().into())).await?;

    tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_secs(30));

        loop {
            interval.tick().await;

            let ping: Value = json!(
                {
                    "method": "ping"
                }
            );

            if let Err(e) = write.send(Message::Text(ping.to_string().into())).await {
                eprintln!("heartbeat failed: {e}");
                break;
            }
            

        }


    });

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("./data/btc_bbo.csv")
        .unwrap();

    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(&["time", "bid_price", "bid_size", "ask_price", "ask_size"])?;


    let mut count: u32 = 1;
    while let Some(msg) = read.next().await {
        let msg = msg?;

        if msg.is_text() {
            let text = msg.to_text()?;
            let ws_message: WsMessage = serde_json::from_str(text)?;

            if ws_message.channel == "pong" {
                println!("Pong");
                continue;
            }

            if ws_message.channel == "bbo" {
                let data = ws_message.data.unwrap();
                let bbo: Bbo = serde_json::from_value(data)?;

                // Quote has String, and String does not implement Copy so without &, it moves ownership but then Vector loses its value.
                let bid: &Quote = &bbo.bbo[0];
                let ask: &Quote = &bbo.bbo[1];

                println!(
                    "{} bid=({}, {}), ask=({}, {}), time={}",
                    bbo.coin,
                    bid.px,
                    bid.sz,
                    ask.px,
                    ask.sz,
                    bbo.time
                );

                wtr.serialize((bbo.time, &bid.px, &bid.sz, &ask.px, &ask.sz))?;

                if count == 100 {
                    wtr.flush()?;
                    break;
                }
                count += 1;
            }
        }
    }
    Ok(())
}
