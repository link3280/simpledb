package me.whitewood.simpledb.engine.json.rpc;

/**
 * RpcClient interacts with {@link RpcService} via messages.
 *
 * @param <CT> Command message type.
 * @param <QT> Query message type.
 * @param <RT> Query response message type.
 **/
public interface RpcClient<CT, QT, RT> {

    /**
     * Establish connection with the service.
     */
    void open();

    /**
     * Send a command request to the service.
     * This would block the client until a response is returned or reaches the timeout.
     * @param object The command message.
     *
     */
    void command(CT object);

    /**
     * Send a request request to the service.
     * This would block the client until a response is returned or reaches the timeout.
     * @param object The query message.
     */
    RT quert(QT object);

    /**
     * Close connection and other resources.
     */
    void close();
}
