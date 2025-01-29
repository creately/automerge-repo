import { NodeWSServerAdapter as BaseAdapter } from '@automerge/automerge-repo-network-websocket';
import { type WebSocketServer, type WebSocket } from "isomorphic-ws";
import { AuthMessage, FromClientMessage, isAuthMessage } from './messages.js';

import {
    cbor as cborHelpers,
    PeerId,
    Message,
    RequestMessage,
    SyncMessage
} from "@automerge/automerge-repo/slim";

const { /* encode,  */decode } = cborHelpers;

export type MessageHandler<X, M = Message> = (
    message: M,
    socket: WebSocketWithIdentity,
    context: NodeWSServerAdapter<X> // thisArg
) => void;
export type ClientMessageHandler<X, M = Message> = (
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

    private logger = console;

    constructor(
        server: WebSocketServer,
        private userIdentityResolver: (authToken: string) => Promise<T|null>,
        keepAliveInterval = 5000,
        options: {
            syncMessageHandler?: ClientMessageHandler<T, SyncMessage>,
            requestMessageHandler?: ClientMessageHandler<T, RequestMessage>,
            logger?: typeof console
        } = {}
    ) {
        super(server, keepAliveInterval);
        const { syncMessageHandler, requestMessageHandler, logger } = options;
        if (syncMessageHandler) {
            this.syncMessageHandler = syncMessageHandler;
        }
        if (requestMessageHandler) {
            this.requestMessageHandler = requestMessageHandler;
        }
        if (logger) {
            this.logger = logger;
        }
    }

    setSyncMessageHandler(handler: ClientMessageHandler<T, SyncMessage>) {
        this.syncMessageHandler = handler;
    }

    setRequestMessageHandler(handler: ClientMessageHandler<T, RequestMessage>) {
        this.requestMessageHandler = handler;
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
            this.logger.warn(`Peer ${socket.peerId} trying to send a message as ${message.senderId}`);
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
            this.logger.error(`Peer ${message.senderId} sent an auth message from a different socket`);
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
            if ( payload ) {
                this.peerIdentity[message.senderId] = payload;
            }
        } catch (e) {
            this.logger.error(`Error authenticating peer ${message.senderId}`, e);
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
    peerId: PeerId;
}