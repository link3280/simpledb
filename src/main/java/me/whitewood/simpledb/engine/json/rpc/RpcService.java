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
