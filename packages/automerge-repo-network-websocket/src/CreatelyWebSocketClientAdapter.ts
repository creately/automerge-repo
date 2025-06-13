import { WebSocketClientAdapter as BaseAdapter } from './WebSocketClientAdapter.js';
import { cbor, PeerId, PeerMetadata } from '@creately/automerge-repo/slim';
import { AuthMessage, CreatelyFromClientMessage, CreatelyFromServerMessage, isAuthResultMessage, isConnectionClosedMessage } from './messages.js';
import WebSocket from "isomorphic-ws"

export class CreatelyWebSocketClientAdapter extends BaseAdapter {

    #connectRetryCount = 0;

    constructor(
        url: string,
        private authToken: string = '',
        retryInterval = 5000,
        private options?: {
            connectionFailureCallback?: () => void;
            connectionClosedCallback?: () => void;
            forceReadyAfterMS?: number;
        }
      ) {
        super(url, retryInterval, options?.forceReadyAfterMS || 3000);
      }

    authenticate() {
        if (this.socket!.readyState === WebSocket.OPEN) {
            this.send(authenticateMessage(this.peerId!, this.authToken))
        }
    }

    send(message: CreatelyFromClientMessage): void {
        super.send(message as any);
    }

    receiveMessage(messageBytes: Uint8Array): void {
        const message: CreatelyFromServerMessage = cbor.decode(new Uint8Array(messageBytes));
        if (messageBytes.byteLength === 0)
            throw new Error("received a zero-length message")
        if (isAuthResultMessage(message)) {
            return;
        }
        if (isConnectionClosedMessage(message)) {
            if (this.options?.connectionClosedCallback) {
                this.options.connectionClosedCallback();
            }
            this.disconnect();
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

    connect(peerId: PeerId, peerMetadata?: PeerMetadata): void {
        if (!this.options?.connectionFailureCallback) {
            super.connect(peerId, peerMetadata);
            return;
        }
        if (this.socket) {
            this.#connectRetryCount++;
            this.socket.removeEventListener('open', this.onOpen1);
        }
        if (this.#connectRetryCount > 2) { // 2 consecutive retry attempt failures
            this.options.connectionFailureCallback();
            return;
        }
        super.connect(peerId, peerMetadata);
        this.socket!.addEventListener('open', this.onOpen1, { once: true });
    }

    onOpen1 = () => {
        this.#connectRetryCount = 0;
    };
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