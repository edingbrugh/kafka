/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 更高级别的消费者访问网络层，基本支持请求未来。该类是线程安全的，但不为响应回调提供同步。这保证了在调用锁时不持有锁。
 */
public class ConsumerNetworkClient implements Closeable {
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    private final Logger log;
    private final KafkaClient client;
    private final UnsentRequests unsent = new UnsentRequests();
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;
    private final int maxPollTimeoutMs;
    private final int requestTimeoutMs;
    private final AtomicBoolean wakeupDisabled = new AtomicBoolean();

    // We do not need high throughput, so use a fair lock to try to avoid starvation
    private final ReentrantLock lock = new ReentrantLock(true);

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    public ConsumerNetworkClient(LogContext logContext,
                                 KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 int requestTimeoutMs,
                                 int maxPollTimeoutMs) {
        this.log = logContext.logger(ConsumerNetworkClient.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int defaultRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    /**
     * 使用默认超时发送请求. See {@link #send(Node, AbstractRequest.Builder, int)}.
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        return send(node, requestBuilder, requestTimeoutMs);
    }

    /**
     * 发送新请求。请注意，在调用 { @link poll(Timer)} 变体之一之前，该请求实际上并未在网络上传输。此时，请求要么成功传输，要么失败。
     * 使用返回的未来获取发送的结果。请注意，无需在 {@link ClientResponse} 对象上显式检查断开连接；相反，future 将因
     * {@link DisconnectException} 而失败。
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @param requestTimeoutMs Maximum time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may be cancelled sooner if the socket disconnects
     *                         for any reason.
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              AbstractRequest.Builder<?> requestBuilder,
                                              int requestTimeoutMs) {
        long now = time.milliseconds();
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
            requestTimeoutMs, completionHandler);
        unsent.put(node, clientRequest);

        // 唤醒客户端以防它在轮询中阻塞，以便我们可以发送排队的请求
        client.wakeup();
        return completionHandler.future;
    }

    public Node leastLoadedNode() {
        lock.lock();
        try {
            return client.leastLoadedNode(time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    public boolean hasReadyNodes(long now) {
        lock.lock();
        try {
            return client.hasReadyNodes(now);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻止等待元数据刷新超时。
     *
     * @return true if update succeeded, false otherwise.
     */
    public boolean awaitMetadataUpdate(Timer timer) {
        int version = this.metadata.requestUpdate();
        do {
            poll(timer);
        } while (this.metadata.updateVersion() == version && timer.notExpired());
        return this.metadata.updateVersion() > version;
    }

    /**
     * 使用默认超时发送请求
     */
    boolean ensureFreshMetadata(Timer timer) {
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(timer.currentTimeMs()) == 0) {
            return awaitMetadataUpdate(timer);
        } else {
            // the metadata is already fresh
            return true;
        }
    }

    /**
     * 唤醒一个活动的轮询。这将导致轮询线程在当前轮询(如果一个轮询是活动的)或下一个轮询上抛出异常。
     */
    public void wakeup() {
        // wakeup在不持有客户端锁的情况下应该是安全的，因为它只是委托给Selector的唤醒，这是线程安全的
        log.debug("Received user wakeup");
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * 无限期阻塞，直到给定的请求完成。
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        while (!future.isDone())
            poll(time.timer(Long.MAX_VALUE), future);
    }

    /**
     * 阻塞，直到所提供的请求(未来请求)已完成或超时已过期。
     * @param future The request future to wait for
     * @param timer Timer bounding how long this method can block
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public boolean poll(RequestFuture<?> future, Timer timer) {
        do {
            poll(timer, future);
        } while (!future.isDone() && timer.notExpired());
        return future.isDone();
    }

    /**
     * 轮询任何网络IO。
     * @param timer Timer bounding how long this method can block
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(Timer timer) {
        poll(timer, null);
    }

    /**
     * 轮询任何网络IO。
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     */
    public void poll(Timer timer, PollCondition pollCondition) {
        poll(timer, pollCondition, false);
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     * @param disableWakeup If TRUE disable triggering wake-ups
     */
    public void poll(Timer timer, PollCondition pollCondition, boolean disableWakeup) {
        // 如果唤醒之前对poll的调用，可能需要调用一些处理程序
        firePendingCompletedRequests();

        lock.lock();
        try {
            // 在尝试任何发送之前处理异步断开连接
            handlePendingDisconnects();

            // 把我们现在能发的请求都发了
            long pollDelayMs = trySend(timer.currentTimeMs());

            // 检查调用方是否仍然需要轮询。注意，如果在调用shouldBlock()之后满足了预期的完成条件(因为触发了完成处理程序)，
            // 客户端将被唤醒。
            if (pendingCompletion.isEmpty() && (pollCondition == null || pollCondition.shouldBlock())) {
                // 如果没有正在执行的请求，则不要阻塞超过重试回退时间
                long pollTimeout = Math.min(timer.remainingMs(), pollDelayMs);
                if (client.inFlightRequestCount() == 0)
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);
                client.poll(pollTimeout, timer.currentTimeMs());
            } else {
                client.poll(0, timer.currentTimeMs());
            }
            timer.update();

            // 通过失败活动请求来处理任何断开连接。注意，在poll之后必须立即检查断开连接，
            // 因为任何后续对client.ready()的调用都会重置断开连接状态
            checkDisconnects(timer.currentTimeMs());
            if (!disableWakeup) {
                // 在检查断开连接后触发唤醒，以便回调准备好在下次调用poll()时触发。
                maybeTriggerWakeup();
            }
            // 如果线程被中断，抛出InterruptException
            maybeThrowInterruptException();

            // 请再次尝试发送请求，因为缓冲区空间可能已被清除，或者连接已在轮询中完成
            trySend(timer.currentTimeMs());

            // 如果请求已过期，则无法发送的请求失败
            failExpiredRequests(timer.currentTimeMs());

            // 清理未发送请求收集，以防止map无限增长
            unsent.clean();
        } finally {
            lock.unlock();
        }

        // 如果处理程序需要获取锁，则在不带锁的情况下调用以避免可能发生死锁
        firePendingCompletedRequests();

        metadata.maybeThrowAnyException();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     */
    public void pollNoWakeup() {
        poll(time.timer(0), null, true);
    }

    /**
     * 尽最大努力轮询网络IO，只尝试传输准备发送的请求。不要检查任何挂起的请求或元数据错误，
     * 这样就不会抛出异常，也不会触发唤醒，也不会中断异常。
     */
    public void transmitSends() {
        Timer timer = time.timer(0);

        // 不要试图处理任何断开连接，预请求失败，元数据异常等;试一次，然后马上回来
        lock.lock();
        try {
            // 把我们现在能发的请求都发了
            trySend(timer.currentTimeMs());

            client.poll(0, timer.currentTimeMs());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻塞，直到来自给定节点的所有挂起请求完成。
     * @param node The node to await requests from
     * @param timer Timer bounding how long this method can block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, Timer timer) {
        while (hasPendingRequests(node) && timer.notExpired()) {
            poll(timer);
        }
        return !hasPendingRequests(node);
    }

    /**
     * 获取对给定节点的挂起请求的计数。这既包括已传输的请求(即飞行中的请求)，也包括正在等待传输的请求。
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        lock.lock();
        try {
            return unsent.requestCount(node) + client.inFlightRequestCount(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 检查是否有对给定节点的待处理请求。这既包括已传输的请求(即飞行中的请求)，也包括正在等待传输的请求。
     * @param node The node in question
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests(Node node) {
        if (unsent.hasRequests(node))
            return true;
        lock.lock();
        try {
            return client.hasInFlightRequests(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取来自所有节点的待挂请求总数。这包括已经传输的请求(即正在传输的请求)和正在等待传输的请求。
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        lock.lock();
        try {
            return unsent.requestCount() + client.inFlightRequestCount();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 检查是否有待处理的请求。这包括已经传输的请求(即正在传输的请求)和正在等待传输的请求。
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests() {
        if (unsent.hasRequests())
            return true;
        lock.lock();
        try {
            return client.hasInFlightRequests();
        } finally {
            lock.unlock();
        }
    }

    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;

            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        // 唤醒客户端，以防它在轮询中阻塞，等待未来的完成
        if (completedRequestsFired)
            client.wakeup();
    }

    private void checkDisconnects(long now) {
        // 任何影响已发送请求的断开连接都将由NetworkClient处理，因此我们只需要检查未发送请求的连接是否已断开;
        // 如果有，那么我们完成相应的future并在ClientResponse中设置disconnect标志
        for (Node node : unsent.nodes()) {
            if (client.connectionFailed(node)) {
                // 在调用请求回调之前删除条目，以避免回调处理协调器失败，再次遍历未发送列表。
                Collection<ClientRequest> requests = unsent.remove(node);
                for (ClientRequest request : requests) {
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    AuthenticationException authenticationException = client.authenticationException(node);
                    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                            request.callback(), request.destination(), request.createdTimeMs(), now, true,
                            null, authenticationException, null));
                }
            }
        }
    }

    private void handlePendingDisconnects() {
        lock.lock();
        try {
            while (true) {
                Node node = pendingDisconnects.poll();
                if (node == null)
                    break;

                failUnsentRequests(node, DisconnectException.INSTANCE);
                client.disconnect(node.idString());
            }
        } finally {
            lock.unlock();
        }
    }

    public void disconnectAsync(Node node) {
        pendingDisconnects.offer(node);
        client.wakeup();
    }

    private void failExpiredRequests(long now) {
        // 清除所有过期的未发送请求并使其相应的未来失败
        Collection<ClientRequest> expiredRequests = unsent.removeExpiredRequests(now);
        for (ClientRequest request : expiredRequests) {
            RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
            handler.onFailure(new TimeoutException("Failed to send request after " + request.requestTimeoutMs() + " ms."));
        }
    }

    private void failUnsentRequests(Node node, RuntimeException e) {
        // 清除未发送到节点的请求并使其相应的未来失败
        lock.lock();
        try {
            Collection<ClientRequest> unsentRequests = unsent.remove(node);
            for (ClientRequest unsentRequest : unsentRequests) {
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                handler.onFailure(e);
            }
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing
    long trySend(long now) {
        long pollDelayMs = maxPollTimeoutMs;

        // 发送任何现在可以发送的请求
        for (Node node : unsent.nodes()) {
            Iterator<ClientRequest> iterator = unsent.requestIterator(node);
            if (iterator.hasNext())
                pollDelayMs = Math.min(pollDelayMs, client.pollDelayMs(node, now));

            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                if (client.ready(node, now)) {
                    client.send(request, now);
                    iterator.remove();
                } else {
                    // 当当前节点未准备好时，尝试下一个节点
                    break;
                }
            }
        }
        return pollDelayMs;
    }

    public void maybeTriggerWakeup() {
        if (!wakeupDisabled.get() && wakeup.get()) {
            log.debug("Raising WakeupException in response to user wakeup");
            wakeup.set(false);
            throw new WakeupException();
        }
    }

    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        wakeupDisabled.set(true);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            client.close();
        } finally {
            lock.unlock();
        }
    }


    /**
     * 检查代码是否断开连接并且无法立即重新连接(即，断开连接后是否在重新连接回退窗口中)。
     */
    public boolean isUnavailable(Node node) {
        lock.lock();
        try {
            return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 检查给定节点上的身份验证错误，如果有，则引发异常。
     */
    public void maybeThrowAuthFailure(Node node) {
        lock.lock();
        try {
            AuthenticationException exception = client.authenticationException(node);
            if (exception != null)
                throw exception;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 如果当前可能，启动一个连接。这只对重置套接字的失败状态有用。如果有实际的请求要发送，那么
     * {@link #send(Node, AbstractRequest.Builder)} 应该使用。
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        lock.lock();
        try {
            client.ready(node, time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    private class RequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        private RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else if (response.authenticationException() != null) {
                future.raise(response.authenticationException());
            } else if (response.wasDisconnected()) {
                log.debug("Cancelled request with header {} due to node {} being disconnected",
                        response.requestHeader(), response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            pendingCompletion.add(this);
        }

        @Override
        public void onComplete(ClientResponse response) {
            this.response = response;
            pendingCompletion.add(this);
        }
    }

    /**
     * 当从多线程环境中调用poll时，调用者等待的条件可能在调用poll之前已经满足。
     * 因此，我们引入这个接口来尽可能地将条件检查推到poll调用的附近。特别是，
     * 检查将在持有用于保护并发访问的锁时完成 {@link org.apache.kafka.clients.NetworkClient},
     * 这意味着如果回调必须获得额外的锁，则实现必须非常小心锁定顺序。
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

    /*
     * 一个线程安全的helper类，用于保存尚未发送的每个节点的请求
     */
    private static final class UnsentRequests {
        private final ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent;

        private UnsentRequests() {
            unsent = new ConcurrentHashMap<>();
        }

        public void put(Node node, ClientRequest request) {
            // 锁可以防止并发删除节点的队列
            synchronized (unsent) {
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.computeIfAbsent(node, key -> new ConcurrentLinkedQueue<>());
                requests.add(request);
            }
        }

        public int requestCount(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? 0 : requests.size();
        }

        public int requestCount() {
            int total = 0;
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                total += requests.size();
            return total;
        }

        public boolean hasRequests(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests != null && !requests.isEmpty();
        }

        public boolean hasRequests() {
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                if (!requests.isEmpty())
                    return true;
            return false;
        }

        private Collection<ClientRequest> removeExpiredRequests(long now) {
            List<ClientRequest> expiredRequests = new ArrayList<>();
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                Iterator<ClientRequest> requestIterator = requests.iterator();
                while (requestIterator.hasNext()) {
                    ClientRequest request = requestIterator.next();
                    long elapsedMs = Math.max(0, now - request.createdTimeMs());
                    if (elapsedMs > request.requestTimeoutMs()) {
                        expiredRequests.add(request);
                        requestIterator.remove();
                    } else
                        break;
                }
            }
            return expiredRequests;
        }

        public void clean() {
            // 锁保护从并发放操作中删除，否则在从映射中删除队列后可能会改变队列
            synchronized (unsent) {
                Iterator<ConcurrentLinkedQueue<ClientRequest>> iterator = unsent.values().iterator();
                while (iterator.hasNext()) {
                    ConcurrentLinkedQueue<ClientRequest> requests = iterator.next();
                    if (requests.isEmpty())
                        iterator.remove();
                }
            }
        }

        public Collection<ClientRequest> remove(Node node) {
            // 锁保护从并发放操作中删除，否则在从映射中删除队列后可能会改变队列
            synchronized (unsent) {
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.remove(node);
                return requests == null ? Collections.<ClientRequest>emptyList() : requests;
            }
        }

        public Iterator<ClientRequest> requestIterator(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? Collections.<ClientRequest>emptyIterator() : requests.iterator();
        }

        public Collection<Node> nodes() {
            return unsent.keySet();
        }
    }

}
