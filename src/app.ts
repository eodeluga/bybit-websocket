import { WebSocket } from "ws";
import * as fs from "fs/promises";
import path from "path";

type WebSocketResponse = {
    topic: string;
    ts?: number;
    type?: string;
    data: Trade [] | Liquidation;
}

type WebSocketStatusResponse = {
    success: boolean;
    ret_msg: string,
    conn_id: string,
    op: string,
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

// Connection
let stream: WebSocket;

const folder = "data";

const tradeFilepath: Path = {
    dir: folder,
    base: "trade.csv",
}

const liquidationFilepath: Path = {
    dir: folder,
    base: "liquidation.csv",
}

const ping = {
    "req_id": "100001", 
    "op": "ping"
}

let pongReceived: number;
let pingInterval: NodeJS.Timer;

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

const handlePong = (response: WebSocketStatusResponse) => {
    response.success ? pongReceived = 1 : pongReceived--;
}

const handlePing = () => {
    if (pongReceived < -1) {
        restartWebSocket();
    } else {
        stream.send(JSON.stringify(ping));
        pongReceived--
    }  
}

const startPingInterval = () => {
    // Send ping keep alive every 20 secs
    pingInterval = setInterval(() => {
        handlePing();
    }, 20000);
}

const stopPingInterval = () => {
    clearInterval(pingInterval);
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

const initialiseWebSocket = () => {
    stream  = new WebSocket("wss://stream.bybit.com/v5/public/linear");
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
        const response: WebSocketResponse | WebSocketStatusResponse = JSON.parse(data.toString());
        
        if ( (response as WebSocketResponse).topic === "publicTrade.BTCUSDT" ) { 
            await handleTradeSub(response as WebSocketResponse);
        
        } else if ( (response as WebSocketResponse).topic === "liquidation.BTCUSDT") {
            await handleLiquidationSub(response as WebSocketResponse);
        
        } else if ( (response as WebSocketStatusResponse).ret_msg === "pong" ) {
            handlePong(response as WebSocketStatusResponse);
        }
    });
    
    pongReceived = 0;
    startPingInterval();
}

const writeString = async (file: string, data: string) => {
    await fs.appendFile(file, data);
};

const restartWebSocket = () => {
    if(stream.readyState != WebSocket.CLOSED || WebSocket.CLOSING) {
        console.log("WebSocket disconnected. Attempting resume!");
        stream.pause();
        stopPingInterval();
        stream.resume();
        startPingInterval();
    
    } else {
        console.log("WebSocket broken. Attempting restart!");
        stream.terminate();
        stream.removeAllListeners();
        initialiseWebSocket();
    }
}

// Startup
(async () => {
    console.log("Starting WebSocket!");
    await initialiseDataFolder();
    initialiseWebSocket();
})();




