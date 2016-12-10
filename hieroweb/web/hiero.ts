import Rx from 'rx.all.js';
import DOM from 'rx.dom.js';

const HieroServiceUrl : string = "ws://localhost:8080";
const RpcRequestUrl = HieroServiceUrl + "/rpc";

// A streaming RPC request: for each request made
// we expect a stream of replies.  The requests are made
// over web sockets.  When the last reply has been received
// the web socked is closed.
export class RpcRequest {
    readonly version  : number = 5;
    readonly requestId: number;
    socket   : any;  // result of Rx.DOM.fromWebSocket

    static requestCounter : number = 0;

    constructor(public objectId : number,
                public method : string,
                public args : string[]) {
        this.requestId = RpcRequest.requestCounter++;
        this.socket = null;
    }

    onOpen() : void {
        console.log('socket open');
        var reqStr = JSON.stringify(this);
        console.log("Sending message " + reqStr);
        this.socket.onNext(reqStr);
    }

    // Function to call to execute the RPC.
    // onReply is the continuation function which is invoked for
    // each result received by the streaming RPC.
    invoke(onReply : (r : any) => void) : void {
        // Invoked when the socked is opened
        var openObserver = Rx.Observer.create(this.onOpen);
        // Invoked when the socket is closed
        var closeObserver = Rx.Observer.create(function (unused) {
            console.log('socket closing');
        });

        // Create a web socked and send the request
        this.socket = DOM.fromWebSocket(RpcRequestUrl, null, openObserver, closeObserver);
        console.log('socket created');
        this.socket.subscribe(
            onReply,
            function(e) { console.log('error: ' + e.toString()); },
            function() { console.log('socket closed'); });
    };
}

