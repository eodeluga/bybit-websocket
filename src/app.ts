import { WebSocket } from "ws";

// Connect
const stream: WebSocket = new WebSocket(
  "wss://stream.bybit.com/v5/public/linear"
);

// Subscribe
/* stream.on("open", () => {
    const subRequest = {
      action: "SubAdd",
      subs: ["publicTrade.BTCUSDT"],
    };
    stream.send(JSON.stringify(subRequest));
});
 */
stream.on("open", () => {
    const subRequest = {
      action: "SubAdd",
      subs: ["liquidation.BTCUSDT"],
    };
    stream.send(JSON.stringify(subRequest));
});

stream.on("message", (data) => {
    console.log(data.toString());
});