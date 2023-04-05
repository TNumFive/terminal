class BinanceClient extends TerminalClient {

    constructor(uid, uri = "ws://localhost:8080", authFunc = noAuth, baseWsUrl = "wss://stream.binance.com", initStream = "btcusdt@kline_1m") {
        super(uid, uri, authFunc);
        this.baseWsUrl = baseWsUrl + "/stream?streams=" + initStream;
        this.isInitialized = false;
        this.streamMap = new Map();
        this.streamMap.set(initStream, []);
    }

    checkAlive(uid, requestId) {
        const response = {
            "check_alive": {"id": requestId}
        }
        this.send([uid], js(response))
    }

    checkInitialized(uid, requestId) {
        const response = {
            "checkInitialized": {"id": requestId, "status": this.isInitialized}
        }
        this.send([uid], js(response))
    }

    binanceSubscribe(uid, stream) {
        if (!this.streamMap.has(stream)) {
            this.streamMap.set(stream, []);
            const obj = {
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": Date.now()
            }
            this.binanceWebsocket.send(js(obj))
        }
        const dest = this.streamMap.get(stream);
        if (!dest.includes(uid)) {
            dest.push(uid);
        }
    }

    binanceUnsubscribe(uid, stream) {
        if (!this.streamMap.has(stream)) {
            return;
        }
        let dest = this.streamMap.get(stream);
        if (dest.includes(uid)) {
            dest = dest.filter(e => e !== uid)
            this.streamMap.set(stream, dest)
        }
        if (!dest.length) {
            this.streamMap.delete(stream);
            const obj = {
                "method": "UNSUBSCRIBE",
                "params": [stream],
                "id": Date.now()
            }
            this.binanceWebsocket.send(js(obj))
        }
    }

    handleBinance(event) {
        const data = jp(event.data);
        const stream = data["stream"];
        if (stream && this.streamMap.has(stream)) {
            const dest = this.streamMap.get(stream);
            if (dest.length) {
                const rawData = {"stream": stream, "data": data["data"]};
                const content = js(rawData);
                this.send(dest, content);
            }
        }
    }

    connectBinance() {
        this.binanceWebsocket = new WebSocket(this.baseWsUrl)
        this.binanceWebsocket.addEventListener('open', () => this.isInitialized = true);
        this.binanceWebsocket.addEventListener('message', event => this.handleBinance(event));
        this.binanceWebsocket.addEventListener('close', event => console.log(event));
        this.binanceWebsocket.addEventListener('error', event => console.log(event));
    }

    react(packet) {
        const data = jp(packet["content"]);
        let method = ""
        if ("method" in Object(data)) {
            method = data["method"]
        }
        switch (method) {
            case "check_alive":
                this.checkAlive(packet["source"], data["id"]);
                return;
            case "check_initialized":
                this.checkInitialized(packet["source"], data["id"]);
                return;
            case "subscribe":
                this.binanceSubscribe(packet["source"], data["params"]);
                return;
            case "unsubscribe":
                this.binanceUnsubscribe(packet["source"], data["params"]);
                return;
        }
    }

    run() {
        this.connectBinance();
        super.run();
    }
}