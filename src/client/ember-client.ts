import { EventEmitter } from 'events';
import { S101Client } from '../socket/s101.client';
import * as BER from '../ber';

import { Request } from './ember-client.request';
import { S101ClientEvent } from '../socket/s101.client';
import { EmberClientEvent } from './ember-client.events';
import { S101SocketError,
    EmberTimeoutError,
    InvalidEmberNodeError,
    ErrorMultipleError,
    InvalidEmberResponseError,
    PathDiscoveryFailureError } from '../error/errors';
import { TreeNode } from '../common/tree-node';
import { InvocationResult } from '../common/invocation-result';
import { QualifiedFunction } from '../common/function/qualified-function';
import { FunctionArgument } from '../common/function/function-argument';
import { Matrix, MatrixConnections } from '../common/matrix/matrix';
import { MatrixOperation } from '../common/matrix/matrix-operation';
import { MatrixConnection } from '../common/matrix/matrix-connection';
import { Parameter } from '../common/parameter';
import { QualifiedParameter } from '../common/qualified-parameter';
import { Element } from '../common/element';
import { QualifiedNode } from '../common/qualified-node';
import { Function } from '../common/function/function';

import { StreamCollection } from '../common/stream/stream-collection';
import { StreamEntry } from '../common/stream/stream-entry';
import { LoggingService, LogLevel } from '../logging/logging.service';
import { ClientLogs } from './ember-client-logs';
import { ParameterContents } from '../common/parameter-contents';
import { SocketStatsInterface } from '../socket/s101.socket';
import { createTreeBranch } from '../common/common';
import { ParameterType } from '../common/parameter-type';

export const DEFAULT_PORT = 9000;
export const DEFAULT_TIMEOUT = 3000;
export interface EmberClientOptions {
    timeoutValue?: number;
    logger?: LoggingService;
    host: string;
    port?: number;
    keepAlive?: boolean;
    keepAliveTimeout?: number;
}

export class EmberClient extends EventEmitter {
    public timeoutValue: number;
    public root: TreeNode;
    private _logger: LoggingService|null;
    private _host: string;
    private _port: number;
    private _streams: StreamCollection;
    private _pendingRequests: Request[];
    private _activeRequest: Request;
    private _timeout: NodeJS.Timeout;
    private _callback?: (e?: Error, data?: any) => void;
    private _requestID: number;
    private socket: S101Client;
    private _keepAlive: boolean;
    private _keepAliveInterval: number;

    private get logger(): LoggingService|null {
        return this._logger;
    }

    constructor(options: EmberClientOptions) {
        super();
        // copy options info
        this.timeoutValue = options.timeoutValue || DEFAULT_TIMEOUT;
        this._host = options.host;
        this._port = options.port || DEFAULT_PORT;
        this._logger = options.logger;
        this._keepAlive = options.keepAlive || false;
        this._keepAliveInterval = options.keepAliveTimeout || DEFAULT_TIMEOUT;

        // initialise internals data
        this._pendingRequests = [];
        this._activeRequest = null;
        this._timeout = null;
        this._callback = undefined;
        this._requestID = 0;

        // Setup hash object to held link between stream identifier and path.
        this._streams = new StreamCollection();
        // Setup root of Ember Tree.
        this.root = new TreeNode();

        // Setup socket
        this.socket = new S101Client(this._host , this._port);

        this.socket.on(S101ClientEvent.CONNECTING, () => {
            this.emit(EmberClientEvent.CONNECTING);
        });

        this.socket.on(S101ClientEvent.CONNECTED, () => {
            this.emit(EmberClientEvent.CONNECTED);
            this.socket.startDeadTimer();
            if (this._keepAlive) {
                this.socket.setKeepAliveInterval(this._keepAliveInterval);
                this.socket.startKeepAlive();
            }
            if (this._callback != null) {
                this._callback();
            }
        });

        this.socket.on(S101ClientEvent.DISCONNECTED, () => {
            this.emit(EmberClientEvent.DISCONNECTED);
        });

        this.socket.on(S101ClientEvent.ERROR, e => {
            if (this._callback != null) {
                this._callback(e);
            }
            this.emit(EmberClientEvent.ERROR, e);
        });

        this.socket.on(S101ClientEvent.EMBER_TREE, root => {
            try {
                if (root instanceof InvocationResult) {
                    this.emit(EmberClientEvent.INVOCATION_RESULT, root);
                    if (this._callback) {
                        this._callback(undefined, root);
                    }
                } else if (root instanceof TreeNode) {
                    this.handleRoot(root);
                }
            } catch (e) {
                this.logger?.log(ClientLogs.INVALID_EMBER_MESSAGE_RECEIVED(e));
                if (this._callback) {
                    this._callback(e);
                }
            }
        });
    }

    static isDirectSubPathOf(path: string, parent: string): boolean {
        return path === parent || (path.lastIndexOf('.') === parent.length && path.startsWith(parent));
    }

    connectAsync(timeout = 2): Promise<void> {
        return new Promise((resolve, reject) => {
            this._callback = e => {
                this._callback = undefined;
                if (e === undefined) {
                    this.logger?.log(ClientLogs.CONNECTED(this.socket.remoteAddress));
                    return resolve();
                }
                this.logger?.log(ClientLogs.CONNECTION_FAILED(this.socket.remoteAddress, e));
                return reject(e);
            };
            if (this.socket.isConnected()) {
                throw new S101SocketError('Already connected');
            }
            this.logger?.log(ClientLogs.CONNECTING(`${this._host}:${this._port}`));
            this.socket.connect(timeout);
        });
    }

    disconnectAsync(): Promise<void> {
        if (this.isConnected()) {
            this.logger?.log(ClientLogs.DISCONNECTING(this.socket.remoteAddress));
            return this.socket.disconnectAsync();
        }
        return Promise.resolve();
    }

    async expandAsync(node?: TreeNode, callback?: (d: TreeNode) => void): Promise<void> {
        this.logger?.log(ClientLogs.EXPANDING_NODE(node));
        let errors: Error[] = [];
        if (node?.isTemplate()) {
            // nothing to expand.
            this.logger?.log(ClientLogs.EXPAND_NODE_COMPLETE(node));
            return;
        }
        if (node != null && (node.isParameter() || node.isMatrix() || node.isFunction())) {
            await this.getDirectoryAsync(node, callback);
            if (node.isMatrix()) {
                // Some Matrix parameters are hidden.  You need to specifically request them
                const matrix: Matrix = node as Matrix;
                if (matrix.parametersLocation) {
                    let paramRoot = createTreeBranch(this.root, matrix.parametersLocation as string);
                    await this.getDirectoryAsync(paramRoot, callback);
                    paramRoot = this.root.getElementByPath(matrix.parametersLocation as string);
                    try {
                        await this.expandAsync(paramRoot, callback);
                    } catch (e) {
                        // We had an error on some expansion
                        if (e instanceof ErrorMultipleError) {
                            errors = errors.concat(e.errors);
                        } else {
                            this.logger?.log(ClientLogs.EXPAND_NODE_ERROR(paramRoot, e));
                            errors.push(e);
                        }
                    }
                }
                if (matrix.labels != null) {
                    for (const label of matrix.labels) {
                        let labelRoot = createTreeBranch(this.root, label.basePath);
                        await this.getDirectoryAsync(labelRoot, callback);
                        labelRoot = this.root.getElementByPath(label.basePath);
                        // Make sure this is qualified node
                        labelRoot = labelRoot.toQualified();
                        await this.expandAsync(labelRoot, callback);
                    }
                }
            }
            return;
        }
        const res: TreeNode = await this.getDirectoryAsync(node, callback) as TreeNode;
        if (res == null) {
            this.logger?.log(ClientLogs.EXPAND_WITH_NO_CHILD(node));
            return;
        }
        const children = res?.getChildren();
        if (children == null) {
            this.logger?.log(ClientLogs.EXPAND_WITH_NO_CHILD(node));
            return;
        }
        for (const child of children) {
            // if (child.isParameter()) {
            //     // Parameter can only have a single child of type Command. So ignore.
            //     continue;
            // }
            try {
                await this.expandAsync(child as TreeNode, callback);
            } catch (e) {
                // We had an error on some expansion
                if (e instanceof ErrorMultipleError) {
                    errors = errors.concat(e.errors);
                } else {
                    this.logger?.log(ClientLogs.EXPAND_NODE_ERROR(child as TreeNode, e));
                    errors.push(e);
                }
            }
        }
        this.logger?.log(ClientLogs.EXPAND_NODE_COMPLETE(node));
        if (errors.length > 0) {
            throw new ErrorMultipleError(errors);
        }
    }

    async getDirectoryAsync(qNode?: TreeNode, callback?: (d: TreeNode) => void): Promise<TreeNode|null> {
        if (qNode == null) {
            this.root.clear();
            qNode = this.root;
        }
        const response: TreeNode = await this.makeRequestAsync(
            // Action to Perform
            () => {
                this.logger?.log(ClientLogs.GETDIRECTORY_SENDING_QUERY(qNode));
                const data = qNode.getDirectory(callback);
                this.socket.sendBERNode(qNode.getDirectory(callback));
            },
            // Response Parsing
            (err: Error, node: TreeNode) => {
                const requestedPath = qNode.getPath();
                if (err) {
                    this.logger?.log(ClientLogs.GETDIRECTORY_ERROR(err));
                    throw err;
                }
                if (!qNode.isQualified() && qNode.isRoot()) {
                    const elements = qNode.getChildren();
                    if (elements == null || elements.length === 0) {
                        throw new InvalidEmberResponseError('Get root directory');
                    }
                    const nodeElements = node?.getChildren();
                    if (nodeElements != null
                        && nodeElements.every((el: TreeNode) => el._parent instanceof TreeNode)) {
                        this.logger?.log(ClientLogs.GETDIRECTORY_RESPONSE(node));
                        return node; // make sure the info is treated before going to next request.
                    } else {
                        throw new InvalidEmberResponseError(`getDirectory ${requestedPath}`);
                    }
                } else if (node.getElementByPath(requestedPath) != null) {
                    this.logger?.log(ClientLogs.GETDIRECTORY_RESPONSE(node));
                    if (node.isStream()) {
                        const streamIdentifier = (node as Parameter).streamIdentifier;
                        const streamEntry = this._streams.getEntry(streamIdentifier);
                        if (streamEntry != null && streamEntry.value !== requestedPath) {
                            // Duplicate Stream Entry.
                            this.logger?.log(ClientLogs.DUPLICATE_STREAM_IDENTIFIER(streamIdentifier, requestedPath, streamEntry.value));
                        } else {
                            this.logger?.log(ClientLogs.ADDING_STREAM_IDENTIFIER(streamIdentifier, requestedPath));
                            this._streams.addEntry(
                                new StreamEntry(
                                    streamIdentifier,
                                    requestedPath
                                )
                            );
                        }
                    }
                    return node; // make sure the info is treated before going to next request.
                } else {
                    const nodeElements = node?.getChildren();
                    if (nodeElements != null &&
                        ((qNode.isMatrix() &&
                            nodeElements.length === 1 &&
                            (nodeElements[0] as Matrix).getPath() === requestedPath) ||
                            (
                                !qNode.isMatrix() &&
                                nodeElements.every((el: TreeNode) => EmberClient.isDirectSubPathOf(el.getPath(), requestedPath))
                            ))) {
                        this.logger?.log(ClientLogs.GETDIRECTORY_RESPONSE(node));
                        return node; // make sure the info is treated before going to next request.
                    } else {
                        this.logger?.log(ClientLogs.GETDIRECTORY_UNEXPECTED_RESPONSE(qNode, node));
                        return undefined; // to signal we should continue to wait
                    }
                }
            },
            qNode
        );
        return response;
    }

    async getElementByPathAsync(path: string, callback?: (d: TreeNode) => void): Promise<TreeNode> {
        const pathError = new PathDiscoveryFailureError(path);
        const TYPE_NUM = 1;
        const TYPE_ID = 2;
        let type = TYPE_NUM;
        let pathArray: (string | number)[] = [];

        this.logger?.log(ClientLogs.GET_ELEMENT_REQUEST(path));

        if (path.indexOf('/') >= 0) {
            type = TYPE_ID;
            pathArray = path.split('/');
        } else {
            pathArray = path.split('.');
            if (pathArray.length === 1) {
                if (isNaN(Number(pathArray[0]))) {
                    type = TYPE_ID;
                }
            }
        }
        let pos = 0;
        let lastMissingPos = -1;
        let currentNode = this.root;
        const getNext: () => Promise<TreeNode> = async () => {
            let node: TreeNode;
            if (type === TYPE_NUM) {
                const number = Number(pathArray[pos]);
                node = currentNode.getElementByNumber(number) as TreeNode;
            } else {
                const children = currentNode.getChildren();
                const identifier = pathArray[pos];
                if (children != null) {
                    let i = 0;
                    for (i = 0; i < children.length; i++) {
                        node = children[i] as Element;
                        if (node.contents != null && node.contents.identifier === identifier) {
                            break;
                        }
                    }
                    if (i >= children.length) {
                        node = null;
                    }
                }
            }
            if (node != null) {
                // We have this part already.
                pos++;
                if (pos >= pathArray.length) {
                    // Let's query this element as well
                    // This will auto subscribe if parameter
                    await this.getDirectoryAsync(node, callback);
                    return node;
                }
                currentNode = node as TreeNode;
                return getNext();
            }
            // We do not have that node yet.
            if (lastMissingPos === pos) {
                throw pathError;
            }
            lastMissingPos = pos;
            await this.getDirectoryAsync(currentNode, callback);
            return getNext();
        };
        const response = await getNext();
        this.logger?.log(ClientLogs.GET_ELEMENT_RESPONSE(path, response));
        return response;
    }

    getStats(): SocketStatsInterface {
        return this.socket.getStats();
    }

    async invokeFunctionAsync(fnNode: Function | QualifiedFunction, params: FunctionArgument[]): Promise<InvocationResult> {
        const res = await this.makeRequestAsync(
            () => {
                this.logger?.log(ClientLogs.INVOCATION_SENDING_QUERY(fnNode));
                this.socket.sendBERNode(fnNode.invoke(params));
            },
            (err: Error, result: InvocationResult) => {
                if (err) {
                    this.logger?.log(ClientLogs.INVOCATION_ERROR(err));
                    throw err;
                } else {
                    this.logger?.log(ClientLogs.INVOCATION_RESULT_RECEIVED(result));
                    return result;
                }
            },
            fnNode
        );
        return res;
    }

    isConnected(): boolean {
        return this.socket?.isConnected();
    }

    async matrixConnectAsync(matrixNode: Matrix, targetID: number, sources: number[]): Promise<Matrix> {
        this.logger?.log(ClientLogs.MATRIX_CONNECTION_REQUEST(matrixNode, targetID, sources));
        return this.matrixOperationAsync(matrixNode, targetID, sources, MatrixOperation.connect);
    }

    async matrixDisconnectAsync(matrixNode: Matrix, targetID: number, sources: number[]): Promise<Matrix> {
        this.logger?.log(ClientLogs.MATRIX_DISCONNECTION_REQUEST(matrixNode, targetID, sources));
        return this.matrixOperationAsync(matrixNode, targetID, sources, MatrixOperation.disconnect);
    }

    async matrixSetConnection(matrixNode: Matrix, targetID: number, sources: number[]): Promise<Matrix> {
        this.logger?.log(ClientLogs.MATRIX_ABSOLUTE_CONNECTION_REQUEST(matrixNode, targetID, sources));
        return this.matrixOperationAsync(matrixNode, targetID, sources, MatrixOperation.absolute);
    }

    saveTree(f: (x: Buffer) => void): void {
        const writer = new BER.ExtendedWriter();
        this.root.encode(writer);
        f(writer.buffer);
    }

    setLogLevel(logLevel: LogLevel): void {
        this.logger?.setLogLevel(logLevel);
    }

    async setValueAsync(node: Parameter|QualifiedParameter, value: string | number | boolean | Buffer, type?: ParameterType): Promise<void> {
        if (!node.isParameter()) {
            throw new InvalidEmberNodeError(node.getPath(), 'not a Parameter');
        } else {
            return this.makeRequestAsync(
                () => {
                    this.logger?.log(ClientLogs.SETVALUE_REQUEST(node, value));
                    this.socket.sendBERNode(node.setValue(ParameterContents.createParameterContent(value, type)));
                },
                (err: Error, n: any) => {
                    if (err) {
                        this.logger?.log(ClientLogs.SETVALUE_REQUEST_ERROR(node, value));
                        throw err;
                    } else {
                        this.logger?.log(ClientLogs.SETVALUE_REQUEST_SUCCESS(node, value));
                        return n;
                    }
                },
                node
            );
        }
    }

    async subscribeAsync(
        qNode: Parameter | Matrix | QualifiedParameter,
        callback?: (d: TreeNode) => void): Promise<void> {
        if ((qNode.isParameter() || qNode.isMatrix()) && qNode.isStream()) {
            return this.makeRequestAsync(
                () => {
                    this.logger?.log(ClientLogs.SUBSCRIBE_REQUEST(qNode));
                    this.socket.sendBERNode(qNode.subscribe(callback));
                    this._streams.addEntry(
                        new StreamEntry(
                            (qNode as Parameter).contents.streamIdentifier,
                            qNode.getPath()));
                    return;
                },
                null,
                qNode
            );
        } else {
            this.logger?.log(ClientLogs.INVALID_SUBSCRIBE_REQUEST(qNode));
        }
    }

    async unsubscribeAsync(
        qNode: Parameter | QualifiedParameter,
        callback: (d: TreeNode) => void): Promise<void> {
        if (qNode.isParameter() && qNode.isStream()) {
            return this.makeRequestAsync(
                () => {
                    this.logger?.log(ClientLogs.UNSUBSCRIBE_REQUEST(qNode));
                    this.socket.sendBERNode(qNode.unsubscribe(callback));
                    const entry = this._streams.getEntry(qNode.contents.streamIdentifier);
                    if (entry != null) {
                        this._streams.removeEntry(entry);
                    }
                },
                undefined,
                qNode
            );
        } else {
            this.logger?.log(ClientLogs.INVALID_UNSUBSCRIBE_REQUEST(qNode));
        }
    }

    private async matrixOperationAsync(
        matrixNode: Matrix, targetID: number, sources: number[],
        operation: MatrixOperation = MatrixOperation.connect): Promise<Matrix> {
        matrixNode.validateConnection(targetID, sources);
        const connections: MatrixConnections = {};
        const targetConnection = new MatrixConnection(targetID);
        targetConnection.operation = operation;
        targetConnection.setSources(sources);
        connections[targetID] = targetConnection;

        return this.makeRequestAsync(
            () => {
                this.socket.sendBERNode(matrixNode.connect(connections));
            },
            (err, node) => {
                const requestedPath = matrixNode.getPath();
                if (err) {
                    this.logger?.log(ClientLogs.MATRIX_OPERATION_ERROR(matrixNode, targetID, sources));
                    throw err;
                }
                if (node == null) {
                    this.logger?.log(ClientLogs.MATRIX_OPERATION_UNEXPECTED_ANSWER(matrixNode, targetID, sources));
                    // ignore this response.
                    return undefined;
                }
                let matrix = null;
                if (node != null) {
                    matrix = node.getElementByPath(requestedPath);
                }
                if (matrix != null && matrix.isMatrix() && matrix.getPath() === requestedPath) {
                    return matrix;
                } else {
                    this.logger?.log(ClientLogs.MATRIX_OPERATION_UNEXPECTED_ANSWER(matrixNode, targetID, sources));
                    // ignore this response.
                    return undefined;
                }
            },
            matrixNode
        );
    }

    private handleRoot(root: TreeNode): void {
        this.logger?.log(ClientLogs.EMBER_MESSAGE_RECEIVED());
        this.root.update(root);
        if (root.elements != null) {
            const elements = root.getChildren();
            for (let i = 0; i < elements.length; i++) {
                if (elements[i].isQualified()) {
                    this.handleQualifiedNode(this.root, elements[i] as QualifiedNode);
                } else {
                    this.handleNode(this.root, elements[i] as Element);
                }
            }
        }
        if (root.getStreams() != null) {
            // StreamCollection received.  Updates the different parameters.
            const streams = root.getStreams();
            for (const streamEntry of streams) {
                const pathContainer: StreamEntry = this._streams.getEntry(streamEntry.identifier);
                if (pathContainer == null) {
                    this.logger?.log(ClientLogs.UNKOWN_STREAM_RECEIVED(streamEntry.identifier));
                    continue;
                }
                const element: Parameter = this.root.getElementByPath(pathContainer.value as string) as Parameter;
                if (element != null && element.isParameter() && element.isStream()
                    && element.contents.value !== streamEntry.value) {
                    element.contents.value = streamEntry.value;
                    element.updateSubscribers();
                }
            }
        }
        if (this._callback) {
            this._callback(null, root);
        }
    }

    private finishRequest(): void {
        this.clearTimeout();
        this._callback = undefined;
        this._activeRequest = null;
        try {
            this.makeRequest();
        } catch (e) {
            if (this._callback != null) {
                this._callback(e);
            } else {
                this.logger?.log(ClientLogs.REQUEST_FAILURE(e));
            }
            this.emit(EmberClientEvent.ERROR, e);
        }
    }

    private makeRequest(): void {
        if (this._activeRequest == null && this._pendingRequests.length > 0) {
            this._activeRequest = this._pendingRequests.shift();
            const req = `${this._requestID++} - ${this._activeRequest.node.getPath()}`;
            this._activeRequest.timeoutError = new EmberTimeoutError(`Request ${req} timed out`);

            this.logger?.log(ClientLogs.MAKING_REQUEST());
            this._timeout = setTimeout(() => {
                this.logger?.log(ClientLogs.REQUEST_FAILURE(this._activeRequest.timeoutError));
                if (this._callback != null) {this._callback(this._activeRequest.timeoutError); }
            }, this.timeoutValue);
            this._activeRequest.func();
        }
    }

    private makeRequestAsync(action: () => void, cb?: (e: Error, data: any) => any, node?: TreeNode): Promise<any> {
        return new Promise((resolve, reject) => {
            const req = new Request(node, () => {
                if (cb != null) {
                    this._callback = (e: Error, d: any) => {
                        try {
                            const res = cb(e, d);
                            // undefined response means ignore and wait for next message
                            if (res === undefined) {
                                return;
                            }
                            this.finishRequest();
                            resolve(res);
                        } catch (error) {
                            this.finishRequest();
                            reject(error);
                        }
                    };
                }
                try {
                    const res: any = action();
                    if (cb == null) {
                        this.finishRequest();
                        resolve(res);
                    }
                } catch (e) {
                    reject(e);
                }
            });
            this._pendingRequests.push(req);
            this.makeRequest();
        });
    }

    private clearTimeout(): void {
        if (this._timeout != null) {
            clearTimeout(this._timeout);
            this._timeout = null;
        }
    }

    private handleNode(parent: TreeNode, node: TreeNode): void {
        if (!(node instanceof TreeNode)) {
            throw new InvalidEmberNodeError(parent.getPath(), 'children not a valid TreeNode');
        }
        let originalNode: TreeNode = parent.getElementByNumber(node.getNumber()) as TreeNode;
        if (originalNode == null) {
            parent.addChild(node);
            originalNode = node;
        } else if (originalNode.update(node)) {
            originalNode.updateSubscribers();
            this.emit(EmberClientEvent.VALUE_CHANGE, originalNode);
        }

        const children = node.getChildren();
        if (children !== null) {
            for (let i = 0; i < children.length; i++) {
                this.handleNode(originalNode as TreeNode, children[i] as TreeNode);
            }
        }
    }

    private handleQualifiedNode(parent: TreeNode, node: TreeNode): void {
        let element = parent.getElementByPath(node.path);
        if (element !== null) {
            if (element.update(node)) {
                element.updateSubscribers();
                this.emit(EmberClientEvent.VALUE_CHANGE, element);
            }
        } else {
            const path = node.path.split('.');
            if (path.length === 1) {
                this.root.addChild(node);
            } else {
                // Let's try to get the parent
                const parentPath = path.slice(0, path.length - 1).join('.');
                parent = this.root.getElementByPath(parentPath);
                if (parent == null) {
                    this.logger?.log(ClientLogs.UNKOWN_ELEMENT_RECEIVED(parentPath));
                    return;
                }
                parent.addChild(node);
                parent.update(parent);
            }
            element = node;
        }

        const children = node.getChildren();
        if (children !== null) {
            for (let i = 0; i < children.length; i++) {
                if (children[i].isQualified()) {
                    this.handleQualifiedNode(element, children[i] as QualifiedNode);
                } else {
                    this.handleNode(element, children[i] as Element);
                }
            }
        }
    }

}
