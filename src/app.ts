import { WebSocket } from "ws";

type WebSocketResponse = {
    topic: string;
    data: object;
    ts?: number;
    type?: string;
}

// Subscription handlers
const handleTradeSub = (response: WebSocketResponse) => {
    
}

const handleLiquidationSub = (response: WebSocketResponse) => {
    
}

// Connect
const stream: WebSocket = new WebSocket(
  "wss://stream.bybit.com/v5/public/linear"
);

const ping = {
    "req_id": "100001", 
    "op": "ping"
}

// Subscribe
stream.on("open", () => {
    const message = {
        "req_id": "subs",
        "op": "subscribe",
        // Subscriptions
        "args": [
            "liquidation.BTCUSDT",
            "publicTrade.BTCUSDT",
        ],
    };
    stream.send(JSON.stringify(message));
});

stream.on("message", (data) => {
    const response: WebSocketResponse = JSON.parse(data.toString());
    response.topic === "publicTrade.BTCUSDT" ? handleTradeSub(response) : handleLiquidationSub(response);
});

// Send ping keep alive every 20 secs
setInterval(() => {
    stream.send(JSON.stringify(ping));
}, 20000)

