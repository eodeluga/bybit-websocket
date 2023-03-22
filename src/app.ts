import { WebSocket } from "ws";
import * as fs from "fs/promises";
import path from "path";

type WebSocketResponse = {
    topic: string;
    data: [];
    ts?: number;
    type?: string;
}

// Subscription handlers
const handleTradeSub = (response: WebSocketResponse) => {
    response.data.forEach(async ele => {
        const {T: timestamp, S: direction, v: size, p: price, BT: blocktrade} = ele;
        const csv = `${timestamp},${direction},${size},${price},${blocktrade}\n`;
        await writeString("data/trade.csv", csv)
    })
}

const handleLiquidationSub = (response: WebSocketResponse) => {
    response.data.forEach(async ele => {
        const {ts: updateTime, side: direction, size: size, price: price} = ele;
        const csv = `${updateTime},${direction},${size},${price}\n`;
        await writeString("data/liquidation.csv", csv)
    })
}

const setupDownloadDir = async (folder: string): Promise<void | boolean> => {
    // Check if folder exists
    return await fs.access(folder)
        .then(() => (true))
        .catch(() => {
            // Create it
            fs.mkdir(folder)
        });
}

const writeString = async (file: string, data: string) => {
    await fs.appendFile(file, data);
};

// Initialise output files
(async () => {
    const folder = "data";
    console.log("Starting!!!");
    
    const folderExists = await setupDownloadDir(folder);
    
    if (!folderExists) {
        let header = "timestamp,direction,size,price,blocktrade\n"
        await writeString(`${folder}/trade.csv`, header);
        
        header = "timestamp,direction,size,price\n"
        await writeString(`${folder}/liquidation.csv`, header);    
    }
})()


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

stream.on("message", async (data) => {
    const response: WebSocketResponse = JSON.parse(data.toString());
    if (response.topic === "publicTrade.BTCUSDT") { 
        await handleTradeSub(response);
    } else if (response.topic === "liquidation.BTCUSDT") {
        await handleLiquidationSub(response);
    }
});

// Send ping keep alive every 20 secs
setInterval(() => {
    stream.send(JSON.stringify(ping));
}, 20000)

