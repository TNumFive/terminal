function js(obj) {
    return JSON.stringify(obj);
}

function jp(str) {
    return JSON.parse(str);
}

function noAuth(uid) {
    return {"uid": uid};
}

class TerminalClient {
    constructor(uid, uri, authFunc = noAuth) {
        this.uid = uid;
        this.uri = uri;
        this.authFunc = authFunc;
    }

    login() {
        const authObj = this.authFunc(this.uid);
        this.websocket.send(js(authObj));
        console.log(`client:${this.uid} logged in`);
    }

    /**
     *
     * @param message
     * @returns {Object}
     */
    static load(message) {
        const packet = jp(message);
        console.assert(typeof packet === "object");
        console.assert(Object.keys(packet).length === 4);
        console.assert(typeof packet["timestamp"] === "number");
        console.assert(typeof packet["source"] === "string");
        console.assert(typeof packet["action"] === "string");
        console.assert(typeof packet["content"] === "string");
        return packet;
    }

    /**
     *
     * @param {string[]}dest
     * @param {string}content
     */
    send(dest, content) {
        const message = js({"destination": dest, "content": content});
        this.websocket.send(message);
    }

    react(packet) {
        // do nothing
    }

    handler(event) {
        let packet;
        try {
            packet = TerminalClient.load(event.data);
        } catch (e) {
            console.log(`client:${this.uid}  loading failed: ${e}`);
        }
        this.react(packet);
    }

    connect() {
        this.websocket = new WebSocket(this.uri);
        const ws = this.websocket;
        ws.addEventListener('open', () => this.login());
        ws.addEventListener('message', event => this.handler(event));
        ws.addEventListener('close', event => console.log(event));
        ws.addEventListener('error', event => console.log(event));
    }

    run() {
        this.connect();
    }
}