/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rpc;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.PingRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.AddPeerRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.ChangePeersRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.GetLeaderRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.GetPeersRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.RemovePeerRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.ResetPeerRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.SnapshotRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.cli.TransferLeaderRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.core.GetFileRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.core.InstallSnapshotRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.core.ReadIndexRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.core.RequestVoteRequestProcessor;
import com.alipay.sofa.jraft.rpc.impl.core.TimeoutNowRequestProcessor;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * Raft RPC server factory.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 7:56:40 PM
 */
public class RaftRpcServerFactory {

    public static final Logger LOG = LoggerFactory.getLogger(RaftRpcServerFactory.class);

    /**
     * Creates a raft RPC server with default request executors.
     *
     * @param endpoint server address to bind
     * @return a rpc server instance
     */
    public static RpcServer createRaftRpcServer(Endpoint endpoint) {
        return createRaftRpcServer(endpoint, null, null);
    }

    /**
     * Creates a raft RPC server with executors to handle requests.
     *
     * @param endpoint      server address to bind
     * @param raftExecutor  executor to handle RAFT requests.
     * @param cliExecutor   executor to handle CLI service requests.
     * @return a rpc server instance
     */
    public static RpcServer createRaftRpcServer(Endpoint endpoint, Executor raftExecutor, Executor cliExecutor) {
        final RpcServer rpcServer = new RpcServer(endpoint.getPort(), true, true);
        addRaftRequestProcessors(rpcServer, raftExecutor, cliExecutor);
        return rpcServer;
    }

    /**
     * Adds RAFT and CLI service request processors with default executor.
     *
     * @param rpcServer rpc server instance
     */
    public static void addRaftRequestProcessors(RpcServer rpcServer) {
        addRaftRequestProcessors(rpcServer, null, null);
    }

    /**
     * Adds RAFT and CLI service request processors
     *
     * @param rpcServer     rpc server instance
     * @param raftExecutor  executor to handle RAFT requests.
     * @param cliExecutor   executor to handle CLI service requests.
     */
    public static void addRaftRequestProcessors(RpcServer rpcServer, Executor raftExecutor, Executor cliExecutor) {
        // raft core processors
        final AppendEntriesRequestProcessor appendEntriesRequestProcessor = new AppendEntriesRequestProcessor(
                raftExecutor);
        //添加事件处理器，当连接关闭时会调用appendEntriesRequestProcessor
        rpcServer.addConnectionEventProcessor(ConnectionEventType.CLOSE, appendEntriesRequestProcessor);
        rpcServer.registerUserProcessor(appendEntriesRequestProcessor);
        //文件请求处理器
        rpcServer.registerUserProcessor(new GetFileRequestProcessor(raftExecutor));
        //安装快照
        rpcServer.registerUserProcessor(new InstallSnapshotRequestProcessor(raftExecutor));
        //选举处理器
        rpcServer.registerUserProcessor(new RequestVoteRequestProcessor(raftExecutor));
        //响应ping请求
        rpcServer.registerUserProcessor(new PingRequestProcessor());
        //处理立马超时的processor
        rpcServer.registerUserProcessor(new TimeoutNowRequestProcessor(raftExecutor));
        //一致读请求处理器
        rpcServer.registerUserProcessor(new ReadIndexRequestProcessor(raftExecutor));
        // raft cli service
        //新加节点
        rpcServer.registerUserProcessor(new AddPeerRequestProcessor(cliExecutor));
        //移除节点
        rpcServer.registerUserProcessor(new RemovePeerRequestProcessor(cliExecutor));
        //重置节点
        rpcServer.registerUserProcessor(new ResetPeerRequestProcessor(cliExecutor));
        //将某个节点设置为新的节点
        rpcServer.registerUserProcessor(new ChangePeersRequestProcessor(cliExecutor));
        //获取leader节点
        rpcServer.registerUserProcessor(new GetLeaderRequestProcessor(cliExecutor));
        //生成快照
        rpcServer.registerUserProcessor(new SnapshotRequestProcessor(cliExecutor));
        //指定某个节点成为leader
        rpcServer.registerUserProcessor(new TransferLeaderRequestProcessor(cliExecutor));
        //获取所有的节点
        rpcServer.registerUserProcessor(new GetPeersRequestProcessor(cliExecutor));
    }

    /**
     * Creates a raft RPC server and starts it.
     *
     * @param endpoint server address to bind
     * @return a rpc server instance
     */
    public static RpcServer createAndStartRaftRpcServer(Endpoint endpoint) {
        return createAndStartRaftRpcServer(endpoint, null, null);
    }

    /**
     * Creates a raft RPC server and starts it.
     *
     * @param endpoint      server address to bind
     * @param raftExecutor  executor to handle RAFT requests.
     * @param cliExecutor   executor to handle CLI service requests.
     * @return a rpc server instance
     */
    public static RpcServer createAndStartRaftRpcServer(Endpoint endpoint, Executor raftExecutor, Executor cliExecutor) {
        final RpcServer server = createRaftRpcServer(endpoint, raftExecutor, cliExecutor);
        server.start();
        return server;
    }
}
