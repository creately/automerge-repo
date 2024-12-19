import { NodeWSServerAdapter as BaseAdapter } from '@automerge/automerge-repo-network-websocket';
import { WebSocket } from 'ws';
import { type WebSocketServer } from "isomorphic-ws";
import { AuthMessage, FromClientMessage, isAuthMessage } from './messages.js';

import {
    cbor as cborHelpers,
    PeerId,
    RequestMessage,
    SyncMessage
} from "@automerge/automerge-repo/slim";

const { /* encode,  */decode } = cborHelpers;

type SyncOrRequestMessage = SyncMessage | RequestMessage;

export type MessageHandler<X, M = SyncOrRequestMessage> = (
    message: M,
    socket: WebSocketWithIdentity,
    context: NodeWSServerAdapter<X> // thisArg
) => void;
export type ClientMessageHandler<X, M = SyncOrRequestMessage> = (
    message: M,
    socket: WebSocketWithIdentity,
    context: NodeWSServerAdapter<X>, // thisArg
    next: MessageHandler<X, M>
) => void;

export class NodeWSServerAdapter<T> extends BaseAdapter {
    peerIdentity: {
        [peerId: PeerId]: T
    } = {};

    private syncMessageHandler: ClientMessageHandler<T, SyncMessage> = (message, socket, ctx, next) => {
        next(message, socket, ctx);
    };

    private requestMessageHandler: ClientMessageHandler<T, RequestMessage> = (message, socket, ctx, next) => {
        next(message, socket, ctx);
    };

    constructor(
        server: WebSocketServer,
        keepAliveInterval = 5000,
        private userIdentityResolver: (authToken: string) => Promise<T>,
        options: {
            syncMessageHandler?: ClientMessageHandler<T, SyncMessage>,
            requestMessageHandler?: ClientMessageHandler<T, RequestMessage>,
        } = {}
    ) {
        super(server, keepAliveInterval);
        const { syncMessageHandler, requestMessageHandler } = options;
        if (syncMessageHandler) {
            this.syncMessageHandler = syncMessageHandler;
        }
        if (requestMessageHandler) {
            this.requestMessageHandler = requestMessageHandler;
        }
    }

    receiveClientMessage(message: FromClientMessage, socket: WebSocketWithIdentity): void {
        if (isAuthMessage(message)) {
            if (!this.sockets[message.senderId]) {
                // wait for join message
                setTimeout(() => {
                    if (!this.sockets[message.senderId]) {
                        throw new Error(`Peer ${message.senderId} did not send a join message`);
                    }
                    this.authenticate(message, socket);
                }, 100);
                return;
            }
            this.authenticate(message, socket);
            return;
        }
        if (socket.peerId && socket.peerId !== message.senderId) {
            console.warn(`Peer ${socket.peerId} trying to send a message as ${message.senderId}`);
            return;
        }
        if (message.type === "sync") {
            this.syncMessageHandler(message as SyncMessage, socket, this, (m, s) => {
                super.receiveClientMessage(m, s);
            });
        } else if (message.type === "request") {
            this.requestMessageHandler(message as RequestMessage, socket, this, (m, s) => {
                super.receiveClientMessage(m, s);
            });
        } else {
            super.receiveClientMessage(message, socket);
        }
    }

    protected async authenticate(message: AuthMessage, socket: WebSocketWithIdentity) {
        if (this.sockets[message.senderId] !== socket) {
            // something fishy is going on
            console.error(`Peer ${message.senderId} sent an auth message from a different socket`);
            this.send({
                type: "auth_result",
                success: false,
                senderId: this.peerId!,
                targetId: message.senderId
            } as any);
            return;
        }
        socket.authenticated = false;
        socket.peerId = message.senderId;
        try {
            const payload = await this.userIdentityResolver(message.authToken);
            socket.authenticated = !!payload;
            // TODO: set peer identity here
            this.peerIdentity[message.senderId] = payload;
        } finally {
            if (socket.authenticated) {
                this.send({
                    type: "auth_result",
                    success: true,
                    senderId: this.peerId!,
                    targetId: message.senderId
                } as any)
            } else {
                this.send({
                    type: "auth_result",
                    success: false,
                    senderId: this.peerId!,
                    targetId: message.senderId
                } as any)
            }
        }
    }
}

export interface WebSocketWithIdentity extends WebSocket {
    authenticated: boolean;
    peerId?: PeerId;
}