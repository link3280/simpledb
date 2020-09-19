package me.whitewood.simpledb.engine.json.rpc;

/**
 * Base interface for RPC service.
 *
 * @param <CT> Command message type.
 * @param <QT> Query message type.
 * @param <RT> Query response message type.
 **/
public interface RpcService<CT, QT, RT> {

    /**
     * Start the daemon thread or process.
     * @param isLocal Start as a thread if true, and as a process otherwise.
     */
    void start(boolean isLocal);

    /**
     * Serve command requests.
     * @param message Command messages.
     */
    void onReceiveCommand(CT message);

    /**
     * Serve query requests.
     * @param message Query messages.
     * @return Query result.
     */
    RT onReceiveQuery(QT message);

    /**
     * Stop the daemon.
     */
    void stop();

}
