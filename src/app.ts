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
    S: 'Buy' | 'Sell';
    v: string;
    p: string;
    BT: boolean;
}

type Liquidation = {
    updatedTime: number;
    symbol: string;
    side: 'Buy' | 'Sell';
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
let restartCount: number;

const getDate = () => new Date().toISOString();

/**
 * Handles the response from the WebSocket subscription for the publicTrade.BTCUSDT topic.
 * Writes trade data to a CSV file.
 *
 * @param {WebSocketResponse} response - The response from the WebSocket subscription.
 * @returns {Promise<void>} A Promise that resolves when the trade data has been written to the CSV file.
 */
const handleTradeSub = async (response: WebSocketResponse) => {
    const data = response.data as Trade [];
    data.forEach(async trade => {
        const {T: timestamp, S: direction, v: size, p: price, BT: blocktrade} = trade;
        const csv = `${timestamp},${direction},${size},${price},${blocktrade}\n`;
        // Write data
        await writeString(path.format(tradeFilepath), csv);
    })
}

/**
 * Handles the response from the WebSocket subscription for the liquidation.BTCUSDT topic.
 * Writes liquidation data to a CSV file.
 *
 * @param {WebSocketResponse} response - The response from the WebSocket subscription.
 * @returns {Promise<void>} A Promise that resolves when the liquidation data has been written to the CSV file.
 */
const handleLiquidationSub = async (response: WebSocketResponse) => {
    const data = response.data as Liquidation;
    const {updatedTime, side: direction, size, price} = data;
    const csv = `${updatedTime},${direction},${size},${price}\n`;
    // Write data
    await writeString(path.format(liquidationFilepath), csv);
}

/**
 * Handles the pong response from the WebSocket status subscription.
 *
 * @param {WebSocketStatusResponse} response - The pong response from the WebSocket status subscription.
 * @returns {void}
 */
const handlePong = (response: WebSocketStatusResponse) => {
    if (response.success) {            
        pongReceived = 1;       
    } else {
        pongReceived--;
    }
}

/**
 * Sends a ping message to the WebSocket and handles the response.
 * If the WebSocket is not responding, the connection is restarted.
 *
 * @returns {void}
 */
const handlePing = () => {
    if (pongReceived <= -1) {
        restartWebSocket();
    } else {
        stream.send(JSON.stringify(ping));
        pongReceived--
    }  
}

/**
 * Starts the ping keep-alive interval for the WebSocket connection.
 *
 * @returns {void}
 */
const startPingInterval = () => {
    // Send ping keep alive every 20 secs
    pingInterval = setInterval(() => {
        handlePing();
    }, 20000);
}

/**
 * Stops the ping keep-alive interval for the WebSocket connection.
 *
 * @returns {void}
 */
const stopPingInterval = () => {
    clearInterval(pingInterval);
}

/**
 * Initialises the data folder and creates the necessary CSV files if they don't exist.
 *
 * @returns {Promise<void>} A Promise that resolves when the data folder has been initialised.
 */
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

/**
 * Initialises the WebSocket connection and subscribes to the necessary topics.
 *
 * @returns {void}
 */
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
        if (restartCount > 0) {
            restartCount = 0;
            console.log(`${getDate()} - Resumed Websocket!`);
        }
            
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
    restartCount = 0;
    startPingInterval();
}

/**
 * Writes a string of data to a file.
 *
 * @param {string} file - The path to the file to write to.
 * @param {string} data - The data to write to the file.
 * @returns {Promise<void>} A Promise that resolves when the data has been written to the file.
 */
const writeString = async (file: string, data: string) => {
    await fs.appendFile(file, data);
};

/**
 * Restarts the WebSocket connection if it is disconnected or not responding.
 *
 * @returns {void}
 */
const restartWebSocket = () => {
    // Exit out to process manager with restart count as error code
    if (restartCount >= 3) {
        stream.terminate();
        stream.removeAllListeners();
        process.exit(restartCount);
    }
    
    const date = getDate();
    
    if(stream.readyState != WebSocket.CLOSED || WebSocket.CLOSING) {
        console.log(`${date} - WebSocket disconnected. Attempting resume!`);
        stream.pause();
        stopPingInterval();
        stream.resume();
        startPingInterval();
    
    } else {
        console.log(`${date} - WebSocket broken. Attempting restart!`);
        stream.terminate();
        stream.removeAllListeners();
        initialiseWebSocket();
    }
    restartCount++;
    console.log(`${getDate()} - Restart attempt: ${restartCount}`);
}

// Startup
(async () => {
    const date = getDate();
    console.log(`${date} - Starting WebSocket!`);
    await initialiseDataFolder();
    initialiseWebSocket();
})();




