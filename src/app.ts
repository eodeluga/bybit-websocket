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
    response.data.forEach(ele => {
        const {T, S, v, p} = ele;
        console.log("");
    })
}

const handleLiquidationSub = (response: WebSocketResponse) => {
    console.log(response);
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

const initialiseDownload = async (file: string, csvHeader: string) => {
    // Nothing to resume so initialise download
    const folderExists = await setupDownloadDir(file);
    
    let header = "timestamp,direction,size,price,blocktrade"
    await writeString(`${file}/trade.csv`, header);
    
    header = "timestamp,direction,size,price"
    await writeString(`${file}/liquidation.csv`, header);
}


// Initialise output files
(async () => {
    const folder = "data";
    console.log("Starting!!!");
    
    const folderExists = await setupDownloadDir(folder);
    
    if (!folderExists) {
        let header = "timestamp,direction,size,price,blocktrade"
        await writeString(`${folder}/trade.csv`, header);
        
        header = "timestamp,direction,size,price"
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
    response.topic === "publicTrade.BTCUSDT" ? 
        await handleTradeSub(response) : 
        await handleLiquidationSub(response);
});

// Send ping keep alive every 20 secs
setInterval(() => {
    stream.send(JSON.stringify(ping));
}, 20000)

