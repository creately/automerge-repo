import { BrowserWebSocketClientAdapter as BaseAdapter } from '@automerge/automerge-repo-network-websocket';
import { cbor, PeerId } from '@automerge/automerge-repo/slim';
import { AuthMessage, FromClientMessage, FromServerMessage, isAuthResultMessage } from './messages.js';

export class BrowserWebSocketClientAdapter extends BaseAdapter {

    constructor(
        url: string,
        private authToken: string = '',
        retryInterval = 5000
      ) {
        super(url, retryInterval);
      }

    authenticate() {
        if (this.socket!.readyState === WebSocket.OPEN) {
            this.send(authenticateMessage(this.peerId!, this.authToken))
        }
    }

    send(message: FromClientMessage): void {
        super.send(message as any);
    }

    receiveMessage(messageBytes: Uint8Array): void {
        const message: FromServerMessage = cbor.decode(new Uint8Array(messageBytes));
        if (messageBytes.byteLength === 0)
            throw new Error("received a zero-length message")
        if (isAuthResultMessage(message)) {
            return;
        }
        super.receiveMessage(messageBytes);
    }

    join(): void {
        super.join();
        if ( this.authToken !== '' ) {
            setTimeout(() => {
                this.authenticate();
            });
        }
    }

    setAuthToken(authToken: string): void {
        this.authToken = authToken;
        this.authenticate();
    }
}

function authenticateMessage(
    senderId: PeerId,
    authToken: string
  ): AuthMessage {
    return {
      type: "auth",
      senderId,
      authToken
    }
  }