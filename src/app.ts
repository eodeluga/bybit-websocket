import { WebSocket } from "ws";
import * as fs from "fs/promises";
import path from "path";

type WebSocketResponse = {
    topic: string;
    ts?: number;
    type?: string;
    data: Trade [] | Liquidation;
}

type Trade = {
    T: number;
    S: string;
    v: string;
    p: string;
    BT: boolean;
}

type Liquidation = {
    updatedTime: number;
    symbol: string;
    side: string;
    size: string;
    price: string;    
}

type Path = {
    root?: string;
    dir: string;
    base: string;
}

const folder = "data";

const tradeFilepath: Path = {
    dir: folder,
    base: "trade.csv",
}

const liquidationFilepath: Path = {
    dir: folder,
    base: "liquidation.csv",
}

// Subscription handlers
const handleTradeSub = async (response: WebSocketResponse) => {
    const data = response.data as Trade [];
    data.forEach(async trade => {
        const {T: timestamp, S: direction, v: size, p: price, BT: blocktrade} = trade;
        const csv = `${timestamp},${direction},${size},${price},${blocktrade}\n`;
        // Write data
        await writeString(path.format(tradeFilepath), csv);
    })
}

const handleLiquidationSub = async (response: WebSocketResponse) => {
    const data = response.data as Liquidation;
    const {updatedTime, side: direction, size, price} = data;
    const csv = `${updatedTime},${direction},${size},${price}\n`;
    // Write data
    await writeString(path.format(liquidationFilepath), csv);
}

const initialiseDataFolder = async () => {
    let file: string;

    // Check if folder exists
    await fs.access(folder)
    .catch(async () => {
        // Create it
        await fs.mkdir(folder);
    });
    
    // Check if files exist
    file = path.format(tradeFilepath);
    await fs.access(file)
    .catch(async () => {
        // Create it
        let header = "timestamp,direction,size,price,blocktrade\n";
        await writeString(file, header);
    });
        
    file = path.format(liquidationFilepath);
    await fs.access(file)
    .catch(async () => {
        // Create it
        let header = "timestamp,direction,size,price\n";
        await writeString(file, header);   
    });
}

const writeString = async (file: string, data: string) => {
    await fs.appendFile(file, data);
};

// Initialise output files
(async () => {
    console.log("Starting!!!");
    await initialiseDataFolder();
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
}, 20000);

