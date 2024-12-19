import { PeerId } from "@automerge/automerge-repo/slim"
import { FromClientMessage as BaseFromClientMessage, FromServerMessage as BaseFromServerMessage } from "@automerge/automerge-repo-network-websocket"

export type AuthMessage = {
    type: "auth";
    /** The PeerID of the client */
    senderId: PeerId;

    /** Metadata presented by the peer  */
    authToken: string;
}

export type AuthResultMessage = {
    type: "auth_result";    /** The PeerID of the client */
    success: boolean;
}

export const isAuthMessage = (
    message: FromClientMessage
): message is AuthMessage => message.type === "auth"

export const isAuthResultMessage = (
    message: FromServerMessage
): message is AuthResultMessage => message.type === "auth_result"

export type FromClientMessage = BaseFromClientMessage | AuthMessage;

/** A message from the server to the client */
export type FromServerMessage = BaseFromServerMessage | AuthResultMessage