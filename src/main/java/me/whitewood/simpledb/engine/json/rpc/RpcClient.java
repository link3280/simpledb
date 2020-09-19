/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
