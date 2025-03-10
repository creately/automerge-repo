import { DocHandle } from "./DocHandle.js";
import { Repo } from "./Repo.js";
import { CappedRepo } from "./CappedRepo.js";
import { AnyDocumentId, DocumentId } from "./types.js";

type AnyRepo = Repo | CappedRepo;

export class ProxyRepo {
    #repoByDocId: Record<string, AnyRepo> = {}
    #repoById: Record<string, AnyRepo> = {}
    find<T>(docId: AnyDocumentId): DocHandle<T> {
        return this.#repoByDocId[docId as string].find<T>(docId);
    }

    async moveDoc(docId: DocumentId, from: AnyRepo, to: AnyRepo) {
        to.importDoc(docId, await from.export(docId) as Uint8Array);
        from.releaseDoc(docId);
        this.#repoByDocId[docId] = to;
    }

    assignRepoDocs(docIds: string[], repo: AnyRepo) {
        docIds.forEach(docId => {
            this.#repoByDocId[docId] = repo;
        });
    }

    addRepo(id: string, repo: AnyRepo) {
        this.#repoById[id] = repo;
    }

    getRepo(id: string) {
        return this.#repoById[id];
    }
}