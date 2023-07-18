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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochClient.OffsetForEpochResult;
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * 该类通过代理管理获取过程。
 * <p>
 * 线程安全:
 *    Fetcher的请求和响应可以由不同的线程处理，因为心跳线程可以处理响应。其他操作是单线程的，只能从轮询使用者的线程调用。
 * <ul>
 *     <li>
 *         如果响应处理程序访问Fetcher的任何共享状态 (e.g. FetchSessionHandler),
 *     对该状态的所有访问必须在Fetcher实例上同步。</li>
 *     <li>
 *         如果响应处理程序访问协调器的任何共享状态(例如SubscriptionState)，则假定调用者在协调器实例上同步了对该状态的所有访问。
 *     </li>
 *     <li>
 *         整理来自多个代理的部分响应(例如列出偏移量)的响应将在响应未来同步。
 *     </li>
 *     <li>在任何时候，每个节点最多有一个请求挂起。处理响应后，将跟踪和更新具有挂起请求的节点。
 *     这确保了在一个线程上处理响应时更新的任何状态(例如epoch)在另一个线程上创建后续请求时是可见的。</li>
 * </ul>
 */
public class Fetcher<K, V> implements Closeable {
    private final Logger log;
    private final LogContext logContext;
    private final ConsumerNetworkClient client;
    private final Time time;
    private final int minBytes;
    private final int maxBytes;
    private final int maxWaitMs;
    private final int fetchSize;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final int maxPollRecords;
    private final boolean checkCrcs;
    private final String clientRackId;
    private final ConsumerMetadata metadata;
    private final FetchManagerMetrics sensors;
    private final SubscriptionState subscriptions;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final BufferSupplier decompressionBufferSupplier = BufferSupplier.create();
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final IsolationLevel isolationLevel;
    private final Map<Integer, FetchSessionHandler> sessionHandlers;
    private final AtomicReference<RuntimeException> cachedListOffsetsException = new AtomicReference<>();
    private final AtomicReference<RuntimeException> cachedOffsetForLeaderException = new AtomicReference<>();
    private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
    private final Set<Integer> nodesWithPendingFetchRequests;
    private final ApiVersions apiVersions;
    private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);

    private CompletedFetch nextInLineFetch = null;

    public Fetcher(LogContext logContext,
                   ConsumerNetworkClient client,
                   int minBytes,
                   int maxBytes,
                   int maxWaitMs,
                   int fetchSize,
                   int maxPollRecords,
                   boolean checkCrcs,
                   String clientRackId,
                   Deserializer<K> keyDeserializer,
                   Deserializer<V> valueDeserializer,
                   ConsumerMetadata metadata,
                   SubscriptionState subscriptions,
                   Metrics metrics,
                   FetcherMetricsRegistry metricsRegistry,
                   Time time,
                   long retryBackoffMs,
                   long requestTimeoutMs,
                   IsolationLevel isolationLevel,
                   ApiVersions apiVersions) {
        this.log = logContext.logger(Fetcher.class);
        this.logContext = logContext;
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.clientRackId = clientRackId;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.sensors = new FetchManagerMetrics(metrics, metricsRegistry);
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.isolationLevel = isolationLevel;
        this.apiVersions = apiVersions;
        this.sessionHandlers = new HashMap<>();
        this.offsetsForLeaderEpochClient = new OffsetsForLeaderEpochClient(client, logContext);
        this.nodesWithPendingFetchRequests = new HashSet<>();
    }

    /**
     * 表示有关代理返回的偏移量的数据。
     */
    static class ListOffsetData {
        final long offset;
        final Long timestamp; //  如果代理不支持返回时间戳，则为空
        final Optional<Integer> leaderEpoch; // empty if the leader epoch is not known

        ListOffsetData(long offset, Long timestamp, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.leaderEpoch = leaderEpoch;
        }
    }

    /**
     * 返回我们是否有任何已完成的获取等待返回给用户。此方法是线程安全的。具有测试可见性。
     * @return true if there are completed fetches, false otherwise
     */
    protected boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    /**
     * 返回我们是否有任何可提取的已完成提取。此方法是线程安全的。 @return 如果存在可以返回的已完成提取，则返回 true，否则返回 false
     */
    public boolean hasAvailableFetches() {
        return completedFetches.stream().anyMatch(fetch -> subscriptions.isFetchable(fetch.partition));
    }

    /**
     * 为我们已为其分配分区且尚未有正在进行中的提取或待处理的提取数据的任何节点设置提取请求。 @return 发送的获取次数
     */
    public synchronized int sendFetches() {
        // 更新指标以防分配发生变化
        sensors.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = FetchRequest.Builder
                    .forConsumer(this.maxWaitMs, this.minBytes, data.toSend())
                    .isolationLevel(isolationLevel)
                    .setMaxBytes(this.maxBytes)
                    .metadata(data.metadata())
                    .toForget(data.toForget())
                    .rackId(clientRackId);

            if (log.isDebugEnabled()) {
                log.debug("Sending {} {} to broker {}", isolationLevel, data.toString(), fetchTarget);
            }
            RequestFuture<ClientResponse> future = client.send(fetchTarget, request);
            //在添加侦听器之前，我们将节点添加到具有挂起获取请求的节点集，因为未来可能已经在另一个线程上实现（例如，
            // 在由心跳线程处理的断开连接期间），这意味着侦听器将被同步调用。
            this.nodesWithPendingFetchRequests.add(entry.getKey().id());
            future.addListener(new RequestFutureListener<ClientResponse>() {
                @Override
                public void onSuccess(ClientResponse resp) {
                    synchronized (Fetcher.this) {
                        try {
                            FetchResponse response = (FetchResponse) resp.responseBody();
                            FetchSessionHandler handler = sessionHandler(fetchTarget.id());
                            if (handler == null) {
                                log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                                        fetchTarget.id());
                                return;
                            }
                            if (!handler.handleResponse(response)) {
                                return;
                            }

                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                            FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

                            for (Map.Entry<TopicPartition, FetchResponseData.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                FetchRequest.PartitionData requestData = data.sessionPartitions().get(partition);
                                if (requestData == null) {
                                    String message;
                                    if (data.metadata().isFull()) {
                                        message = MessageFormatter.arrayFormat(
                                                "Response for missing full request partition: partition={}; metadata={}",
                                                new Object[]{partition, data.metadata()}).getMessage();
                                    } else {
                                        message = MessageFormatter.arrayFormat(
                                                "Response for missing session request partition: partition={}; metadata={}; toSend={}; toForget={}",
                                                new Object[]{partition, data.metadata(), data.toSend(), data.toForget()}).getMessage();
                                    }

                                    // 接收到缺少会话分区的获取响应
                                    throw new IllegalStateException(message);
                                } else {
                                    long fetchOffset = requestData.fetchOffset;
                                    FetchResponseData.PartitionData partitionData = entry.getValue();

                                    log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                                            isolationLevel, fetchOffset, partition, partitionData);

                                    Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
                                    short responseVersion = resp.requestHeader().apiVersion();

                                    completedFetches.add(new CompletedFetch(partition, partitionData,
                                            metricAggregator, batches, fetchOffset, responseVersion));
                                }
                            }

                            sensors.fetchLatency.record(resp.requestLatencyMs());
                        } finally {
                            nodesWithPendingFetchRequests.remove(fetchTarget.id());
                        }
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    synchronized (Fetcher.this) {
                        try {
                            FetchSessionHandler handler = sessionHandler(fetchTarget.id());
                            if (handler != null) {
                                handler.handleError(e);
                            }
                        } finally {
                            nodesWithPendingFetchRequests.remove(fetchTarget.id());
                        }
                    }
                }
            });

        }
        return fetchRequestMap.size();
    }

    /**
     * 获取集群中所有主题的主题元数据
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer) {
        return getTopicMetadata(MetadataRequest.Builder.allTopics(), timer);
    }

    /**
     * 获取Kafka集群中所有主题的元数据
     *
     * @param request The MetadataRequest to send
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, Timer timer) {
        // 如果没有请求主题，则保存往返行程。
        if (!request.isAllTopics() && request.emptyTopicList())
            return Collections.emptyMap();

        do {
            RequestFuture<ClientResponse> future = sendMetadataRequest(request);
            client.poll(future, timer);

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            if (future.succeeded()) {
                MetadataResponse response = (MetadataResponse) future.value().responseBody();
                Cluster cluster = response.buildCluster();

                Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
                if (!unauthorizedTopics.isEmpty())
                    throw new TopicAuthorizationException(unauthorizedTopics);

                boolean shouldRetry = false;
                Map<String, Errors> errors = response.errors();
                if (!errors.isEmpty()) {
                    // 如果有错误，我们需要检查它们是否致命，或者我们是否应该重试

                    log.debug("Topic metadata fetch included errors: {}", errors);

                    for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                        String topic = errorEntry.getKey();
                        Errors error = errorEntry.getValue();

                        if (error == Errors.INVALID_TOPIC_EXCEPTION)
                            throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                        else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                            // if a requested topic is unknown, we just continue and let it be absent
                            // in the returned map
                            continue;
                        else if (error.exception() instanceof RetriableException)
                            shouldRetry = true;
                        else
                            throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                                    error.exception());
                    }
                }

                if (!shouldRetry) {
                    HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
                    for (String topic : cluster.topics())
                        topicsPartitionInfos.put(topic, cluster.partitionsForTopic(topic));
                    return topicsPartitionInfos;
                }
            }

            timer.sleep(retryBackoffMs);
        } while (timer.notExpired());

        throw new TimeoutException("Timeout expired while fetching topic metadata");
    }

    /**
     * 向Kafka集群中负载最少的节点异步发送元数据请求
     * @return A future that indicates result of sent metadata request
     */
    private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest.Builder request) {
        final Node node = client.leastLoadedNode();
        if (node == null)
            return RequestFuture.noBrokersAvailable();
        else
            return client.send(node, request);
    }

    private Long offsetResetStrategyTimestamp(final TopicPartition partition) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        if (strategy == OffsetResetStrategy.EARLIEST)
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            return ListOffsetsRequest.LATEST_TIMESTAMP;
        else
            return null;
    }

    private OffsetResetStrategy timestampToOffsetResetStrategy(long timestamp) {
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP)
            return OffsetResetStrategy.EARLIEST;
        else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP)
            return OffsetResetStrategy.LATEST;
        else
            return null;
    }

    /**
     * 重置所有分配分区的偏移量。
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException 如果没有定义偏移重置策略，并且一个或多个分区没有等待
     * seekToBeginning() or seekToEnd().
     */
    public void resetOffsetsIfNeeded() {
        // Raise exception from previous offset fetch if there is one
        RuntimeException exception = cachedListOffsetsException.getAndSet(null);
        if (exception != null)
            throw exception;

        Set<TopicPartition> partitions = subscriptions.partitionsNeedingReset(time.milliseconds());
        if (partitions.isEmpty())
            return;

        final Map<TopicPartition, Long> offsetResetTimestamps = new HashMap<>();
        for (final TopicPartition partition : partitions) {
            Long timestamp = offsetResetStrategyTimestamp(partition);
            if (timestamp != null)
                offsetResetTimestamps.put(partition, timestamp);
        }

        resetOffsetsAsync(offsetResetTimestamps);
    }

    /**
     * 验证已检测到领导者更改的所有已分配分区的偏移量。
     */
    public void validateOffsetsIfNeeded() {
        RuntimeException exception = cachedOffsetForLeaderException.getAndSet(null);
        if (exception != null)
            throw exception;

        // 根据当前领导者和纪元验证每个分区如果我们看到新的元数据版本，请检查所有分区
        validatePositionsOnMetadataChange();

        // 收集需要验证的位置，带回退
        Map<TopicPartition, FetchPosition> partitionsToValidate = subscriptions
                .partitionsNeedingValidation(time.milliseconds())
                .stream()
                .filter(tp -> subscriptions.position(tp) != null)
                .collect(Collectors.toMap(Function.identity(), subscriptions::position));

        validateOffsetsAsync(partitionsToValidate);
    }

    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Timer timer) {
        metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keySet()));

        try {
            Map<TopicPartition, ListOffsetData> fetchedOffsets = fetchOffsetsByTimes(timestampsToSearch,
                    timer, true).fetchedOffsets;

            HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
                offsetsByTimes.put(entry.getKey(), null);

            for (Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
                // 'entry.getValue().timestamp' will not be null since we are guaranteed
                // to work with a v1 (or later) ListOffset request
                ListOffsetData offsetData = entry.getValue();
                offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(offsetData.offset, offsetData.timestamp,
                        offsetData.leaderEpoch));
            }

            return offsetsByTimes;
        } finally {
            metadata.clearTransientTopics();
        }
    }

    private ListOffsetResult fetchOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                 Timer timer,
                                                 boolean requireTimestamps) {
        ListOffsetResult result = new ListOffsetResult();
        if (timestampsToSearch.isEmpty())
            return result;

        Map<TopicPartition, Long> remainingToSearch = new HashMap<>(timestampsToSearch);
        do {
            RequestFuture<ListOffsetResult> future = sendListOffsetsRequests(remainingToSearch, requireTimestamps);

            future.addListener(new RequestFutureListener<ListOffsetResult>() {
                @Override
                public void onSuccess(ListOffsetResult value) {
                    synchronized (future) {
                        result.fetchedOffsets.putAll(value.fetchedOffsets);
                        remainingToSearch.keySet().retainAll(value.partitionsToRetry);

                        for (final Map.Entry<TopicPartition, ListOffsetData> entry: value.fetchedOffsets.entrySet()) {
                            final TopicPartition partition = entry.getKey();

                            // 如果感兴趣的分区是订阅的一部分，那么也使用返回的偏移量来更新订阅状态:对于read-committed，
                            // 返回的偏移量将是LSO;对于read-uncommitted，返回的偏移量为HW;
                            if (subscriptions.isAssigned(partition)) {
                                final long offset = entry.getValue().offset;
                                if (isolationLevel == IsolationLevel.READ_COMMITTED) {
                                    log.trace("Updating last stable offset for partition {} to {}", partition, offset);
                                    subscriptions.updateLastStableOffset(partition, offset);
                                } else {
                                    log.trace("Updating high watermark for partition {} to {}", partition, offset);
                                    subscriptions.updateHighWatermark(partition, offset);
                                }
                            }
                        }
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    if (!(e instanceof RetriableException)) {
                        throw future.exception();
                    }
                }
            });

            // 如果timeout设置为0，不要尝试轮询网络客户端，并立即返回空;否则，尝试同步获取结果，
            // 如果不能及时完成则抛出超时异常
            if (timer.timeoutMs() == 0L)
                return result;

            client.poll(future, timer);

            if (!future.isDone()) {
                break;
            } else if (remainingToSearch.isEmpty()) {
                return result;
            } else {
                client.awaitMetadataUpdate(timer);
            }
        } while (timer.notExpired());

        throw new TimeoutException("Failed to get offsets by times in " + timer.elapsedMs() + "ms");
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timer);
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Timer timer) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timer);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Timer timer) {
        metadata.addTransientTopics(topicsForPartitions(partitions));
        try {
            Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
                    .distinct()
                    .collect(Collectors.toMap(Function.identity(), tp -> timestamp));

            ListOffsetResult result = fetchOffsetsByTimes(timestampsToSearch, timer, false);

            return result.fetchedOffsets.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset));
        } finally {
            metadata.clearTransientTopics();
        }
    }

    /**
     *返回获取的记录，清空记录缓冲区并更新消耗的位置。
     *
     * 注意:返回空记录保证消耗的位置没有更新。每个分区获取的记录
     * 如果在fetchResponse中有OffsetOutOfRange错误defaultResetPolicy为NONE如果在fetchResponse中有TopicAuthorization错误。
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> fetched = new HashMap<>();
        Queue<CompletedFetch> pausedCompletedFetches = new ArrayDeque<>();
        int recordsRemaining = maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                if (nextInLineFetch == null || nextInLineFetch.isConsumed) {
                    CompletedFetch records = completedFetches.peek();
                    if (records == null) break;

                    if (records.notInitialized()) {
                        try {
                            nextInLineFetch = initializeCompletedFetch(records);
                        } catch (Exception e) {
                            // 在解析时移除completedFetch，如果
                            // (1)它不包含任何记录，并且
                            // (2)在此异常之前没有实际内容的获取记录。第一个条件确保completedFetch不会被同一个completedFetch卡住
                            // 在诸如TopicAuthorizationException的情况下，第二个条件确保no由于以下记录中的异常可能导致数据丢失。
                            FetchResponseData.PartitionData partition = records.partitionData;
                            if (fetched.isEmpty() && FetchResponse.recordsOrFail(partition).sizeInBytes() == 0) {
                                completedFetches.poll();
                            }
                            throw e;
                        }
                    } else {
                        nextInLineFetch = records;
                    }
                    completedFetches.poll();
                } else if (subscriptions.isPaused(nextInLineFetch.partition)) {
                    // 当分区暂停时，我们将记录添加回completedFetches队列，而不是排水如果分区在那个时候被恢复，它们可以在后续轮询中返回
                    log.debug("Skipping fetching records for assigned partition {} because it is paused", nextInLineFetch.partition);
                    pausedCompletedFetches.add(nextInLineFetch);
                    nextInLineFetch = null;
                } else {
                    List<ConsumerRecord<K, V>> records = fetchRecords(nextInLineFetch, recordsRemaining);

                    if (!records.isEmpty()) {
                        TopicPartition partition = nextInLineFetch.partition;
                        List<ConsumerRecord<K, V>> currentRecords = fetched.get(partition);
                        if (currentRecords == null) {
                            fetched.put(partition, records);
                        } else {
                            // 这种情况通常不会发生，因为每个分区每次只发送一个fetch，但是在一些罕见的情况下(例如分区的leader更改)，它可能会发生。
                            // 我们必须复制到一个新的列表，因为旧的列表可能是不可变的
                            List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
                            newRecords.addAll(currentRecords);
                            newRecords.addAll(records);
                            fetched.put(partition, newRecords);
                        }
                        recordsRemaining -= records.size();
                    }
                }
            }
        } catch (KafkaException e) {
            if (fetched.isEmpty())
                throw e;
        } finally {
            // 将暂停分区的所有轮询的已完成获取添加回已完成获取队列中在下次投票中重新评估
            completedFetches.addAll(pausedCompletedFetches);
        }

        return fetched;
    }

    private List<ConsumerRecord<K, V>> fetchRecords(CompletedFetch completedFetch, int maxRecords) {
        if (!subscriptions.isAssigned(completedFetch.partition)) {
            // 如果在获取的记录返回到消费者的轮询调用之前发生了重新平衡，则可能会发生这种情况
            log.debug("Not returning fetched records for partition {} since it is no longer assigned",
                    completedFetch.partition);
        } else if (!subscriptions.isFetchable(completedFetch.partition)) {
            // 当一个分区在取回的记录返回给使用者分区之前暂停时，就会发生这种情况轮询调用或者是否正在重置偏移量
            log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable",
                    completedFetch.partition);
        } else {
            FetchPosition position = subscriptions.position(completedFetch.partition);
            if (position == null) {
                throw new IllegalStateException("Missing position for fetchable partition " + completedFetch.partition);
            }

            if (completedFetch.nextFetchOffset == position.offset) {
                List<ConsumerRecord<K, V>> partRecords = completedFetch.fetchRecords(maxRecords);

                log.trace("Returning {} fetched records at offset {} for assigned partition {}",
                        partRecords.size(), position, completedFetch.partition);

                if (completedFetch.nextFetchOffset > position.offset) {
                    FetchPosition nextPosition = new FetchPosition(
                            completedFetch.nextFetchOffset,
                            completedFetch.lastEpoch,
                            position.currentLeader);
                    log.trace("Update fetching position to {} for partition {}", nextPosition, completedFetch.partition);
                    subscriptions.position(completedFetch.partition, nextPosition);
                }

                Long partitionLag = subscriptions.partitionLag(completedFetch.partition, isolationLevel);
                if (partitionLag != null)
                    this.sensors.recordPartitionLag(completedFetch.partition, partitionLag);

                Long lead = subscriptions.partitionLead(completedFetch.partition);
                if (lead != null) {
                    this.sensors.recordPartitionLead(completedFetch.partition, lead);
                }

                return partRecords;
            } else {
                // 这些记录不是基于最后消耗位置的下一行记录，忽略它们它们必须来自过时的请求
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        completedFetch.partition, completedFetch.nextFetchOffset, position);
            }
        }

        log.trace("Draining fetched records for partition {}", completedFetch.partition);
        completedFetch.drain();

        return emptyList();
    }

    // Visible for testing
    void resetOffsetIfNeeded(TopicPartition partition, OffsetResetStrategy requestedResetStrategy, ListOffsetData offsetData) {
        FetchPosition position = new FetchPosition(
            offsetData.offset,
            Optional.empty(), // This will ensure we skip validation
            metadata.currentLeader(partition));
        offsetData.leaderEpoch.ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(partition, epoch));
        subscriptions.maybeSeekUnvalidated(partition, position, requestedResetStrategy);
    }

    private void resetOffsetsAsync(Map<TopicPartition, Long> partitionResetTimestamps) {
        Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(partitionResetTimestamps, new HashSet<>());
        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            Node node = entry.getKey();
            final Map<TopicPartition, ListOffsetsPartition> resetTimestamps = entry.getValue();
            subscriptions.setNextAllowedRetry(resetTimestamps.keySet(), time.milliseconds() + requestTimeoutMs);

            RequestFuture<ListOffsetResult> future = sendListOffsetRequest(node, resetTimestamps, false);
            future.addListener(new RequestFutureListener<ListOffsetResult>() {
                @Override
                public void onSuccess(ListOffsetResult result) {
                    if (!result.partitionsToRetry.isEmpty()) {
                        subscriptions.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs);
                        metadata.requestUpdate();
                    }

                    for (Map.Entry<TopicPartition, ListOffsetData> fetchedOffset : result.fetchedOffsets.entrySet()) {
                        TopicPartition partition = fetchedOffset.getKey();
                        ListOffsetData offsetData = fetchedOffset.getValue();
                        ListOffsetsPartition requestedReset = resetTimestamps.get(partition);
                        resetOffsetIfNeeded(partition, timestampToOffsetResetStrategy(requestedReset.timestamp()), offsetData);
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.requestFailed(resetTimestamps.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException) && !cachedListOffsetsException.compareAndSet(null, e))
                        log.error("Discarding error in ListOffsetResponse because another error is pending", e);
                }
            });
        }
    }

    static boolean hasUsableOffsetForLeaderEpochVersion(NodeApiVersions nodeApiVersions) {
        ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        if (apiVersion == null)
            return false;

        return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion());
    }

    /**
     * 对于每个需要验证的分区，发出一个异步请求，以获取该分区的结束偏移量，
     * 该分区的epoch小于或等于该分区最后看到的epoch。
     *
     * Requests are grouped by Node for efficiency.
     */
    private void validateOffsetsAsync(Map<TopicPartition, FetchPosition> partitionsToValidate) {
        final Map<Node, Map<TopicPartition, FetchPosition>> regrouped =
            regroupFetchPositionsByLeader(partitionsToValidate);

        long nextResetTimeMs = time.milliseconds() + requestTimeoutMs;
        regrouped.forEach((node, fetchPositions) -> {
            if (node.isEmpty()) {
                metadata.requestUpdate();
                return;
            }

            NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
            if (nodeApiVersions == null) {
                client.tryConnect(node);
                return;
            }

            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                log.debug("Skipping validation of fetch offsets for partitions {} since the broker does not " +
                              "support the required protocol version (introduced in Kafka 2.3)",
                    fetchPositions.keySet());
                for (TopicPartition partition : fetchPositions.keySet()) {
                    subscriptions.completeValidation(partition);
                }
                return;
            }

            subscriptions.setNextAllowedRetry(fetchPositions.keySet(), nextResetTimeMs);

            RequestFuture<OffsetForEpochResult> future =
                offsetsForLeaderEpochClient.sendAsyncRequest(node, fetchPositions);

            future.addListener(new RequestFutureListener<OffsetForEpochResult>() {
                @Override
                public void onSuccess(OffsetForEpochResult offsetsResult) {
                    List<SubscriptionState.LogTruncation> truncations = new ArrayList<>();
                    if (!offsetsResult.partitionsToRetry().isEmpty()) {
                        subscriptions.setNextAllowedRetry(offsetsResult.partitionsToRetry(), time.milliseconds() + retryBackoffMs);
                        metadata.requestUpdate();
                    }

                    // 对于每个OffsetsForLeader响应，检查结束偏移量是否低于分区的当前偏移量。
                    // 如果是这样，则意味着我们经历了日志截断，需要重新定位该分区的偏移量。
                    // 另外，检查返回的偏移量和epoch是否有效。如果不是，那么我们应该重置它的偏移量(如果配置了重置策略)，
                    // 或者抛出超出范围的异常。
                    offsetsResult.endOffsets().forEach((topicPartition, respEndOffset) -> {
                        FetchPosition requestPosition = fetchPositions.get(topicPartition);
                        Optional<SubscriptionState.LogTruncation> truncationOpt =
                            subscriptions.maybeCompleteValidation(topicPartition, requestPosition, respEndOffset);
                        truncationOpt.ifPresent(truncations::add);
                    });

                    if (!truncations.isEmpty()) {
                        maybeSetOffsetForLeaderException(buildLogTruncationException(truncations));
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    subscriptions.requestFailed(fetchPositions.keySet(), time.milliseconds() + retryBackoffMs);
                    metadata.requestUpdate();

                    if (!(e instanceof RetriableException)) {
                        maybeSetOffsetForLeaderException(e);
                    }
                }
            });
        });
    }

    private LogTruncationException buildLogTruncationException(List<SubscriptionState.LogTruncation> truncations) {
        Map<TopicPartition, OffsetAndMetadata> divergentOffsets = new HashMap<>();
        Map<TopicPartition, Long> truncatedFetchOffsets = new HashMap<>();
        for (SubscriptionState.LogTruncation truncation : truncations) {
            truncation.divergentOffsetOpt.ifPresent(divergentOffset ->
                divergentOffsets.put(truncation.topicPartition, divergentOffset));
            truncatedFetchOffsets.put(truncation.topicPartition, truncation.fetchPosition.offset);
        }
        return new LogTruncationException("Detected truncated partitions: " + truncations,
            truncatedFetchOffsets, divergentOffsets);
    }

    private void maybeSetOffsetForLeaderException(RuntimeException e) {
        if (!cachedOffsetForLeaderException.compareAndSet(null, e)) {
            log.error("Discarding error in OffsetsForLeaderEpoch because another error is pending", e);
        }
    }

    /**
     * 按目标时间搜索指定分区的偏移量。
     *
     * @param timestampsToSearch the mapping between partitions and target time
     * @param requireTimestamps true if we should fail with an UnsupportedVersionException if the broker does
     *                         not support fetching precise timestamps for offsets
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<ListOffsetResult> sendListOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
                                                                    final boolean requireTimestamps) {
        final Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, partitionsToRetry);
        if (timestampsToSearchByNode.isEmpty())
            return RequestFuture.failure(new StaleMetadataException());

        final RequestFuture<ListOffsetResult> listOffsetRequestsFuture = new RequestFuture<>();
        final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets = new HashMap<>();
        final AtomicInteger remainingResponses = new AtomicInteger(timestampsToSearchByNode.size());

        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            RequestFuture<ListOffsetResult> future = sendListOffsetRequest(entry.getKey(), entry.getValue(), requireTimestamps);
            future.addListener(new RequestFutureListener<ListOffsetResult>() {
                @Override
                public void onSuccess(ListOffsetResult partialResult) {
                    synchronized (listOffsetRequestsFuture) {
                        fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
                        partitionsToRetry.addAll(partialResult.partitionsToRetry);

                        if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone()) {
                            ListOffsetResult result = new ListOffsetResult(fetchedTimestampOffsets, partitionsToRetry);
                            listOffsetRequestsFuture.complete(result);
                        }
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    synchronized (listOffsetRequestsFuture) {
                        if (!listOffsetRequestsFuture.isDone())
                            listOffsetRequestsFuture.raise(e);
                    }
                }
            });
        }
        return listOffsetRequestsFuture;
    }

    /**
     * 分组时间戳以按节点搜索' timestampsToSearch '中具有可用领导者的主题分区。从' timestampsToSearch '
     * 中没有其leader可用的主题分区被添加到`partitionsToRetry`
     * @param timestampsToSearch The mapping from partitions ot the target timestamps
     * @param partitionsToRetry A set of topic partitions that will be extended with partitions
     *                          that need metadata update or re-connect to the leader.
     */
    private Map<Node, Map<TopicPartition, ListOffsetsPartition>> groupListOffsetRequests(
            Map<TopicPartition, Long> timestampsToSearch,
            Set<TopicPartition> partitionsToRetry) {
        final Map<TopicPartition, ListOffsetsPartition> partitionDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry: timestampsToSearch.entrySet()) {
            TopicPartition tp  = entry.getKey();
            Long offset = entry.getValue();
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            if (!leaderAndEpoch.leader.isPresent()) {
                log.debug("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                metadata.requestUpdate();
                partitionsToRetry.add(tp);
            } else {
                Node leader = leaderAndEpoch.leader.get();
                if (client.isUnavailable(leader)) {
                    client.maybeThrowAuthFailure(leader);

                    // 连接失败了，我们需要等待回退期才能再次尝试。不需要请求元数据更新，因为断开连接已经这样做了。
                    log.debug("Leader {} for partition {} is unavailable for fetching offset until reconnect backoff expires",
                            leader, tp);
                    partitionsToRetry.add(tp);
                } else {
                    int currentLeaderEpoch = leaderAndEpoch.epoch.orElse(ListOffsetsResponse.UNKNOWN_EPOCH);
                    partitionDataMap.put(tp, new ListOffsetsPartition()
                            .setPartitionIndex(tp.partition())
                            .setTimestamp(offset)
                            .setCurrentLeaderEpoch(currentLeaderEpoch));
                }
            }
        }
        return regroupPartitionMapByNode(partitionDataMap);
    }

    /**
     * 将listOffseRequest发送到分区和目标时间戳的特定代理。
     *
     * @param node The node to send the ListOffsetRequest to.
     * @param timestampsToSearch The mapping from partitions to the target timestamps.
     * @param requireTimestamp  True if we require a timestamp in the response.
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<ListOffsetResult> sendListOffsetRequest(final Node node,
                                                                  final Map<TopicPartition, ListOffsetsPartition> timestampsToSearch,
                                                                  boolean requireTimestamp) {
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(requireTimestamp, isolationLevel, false)
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch));

        log.debug("Sending ListOffsetRequest {} to broker {}", builder, node);
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<ClientResponse, ListOffsetResult>() {
                    @Override
                    public void onSuccess(ClientResponse response, RequestFuture<ListOffsetResult> future) {
                        ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                        log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                        handleListOffsetResponse(lor, future);
                    }
                });
    }

    /**
     * Callback 对于上面的列表偏移量调用的响应。
     * @param listOffsetsResponse 来自服务器的响应。
     * @param future 响应返回时要完成的未来。请注意，任何分区级错误通常都会导致整个未来结果失败。
     *               唯一的例外是UNSUPPORTED_FOR_MESSAGE_FORMAT，它表示代理不支持v1消息格式。
     *               具有此特定错误的分区将被排除在未来的映射之外。注意，每个分区对应的时间戳值可能只有在v0中为空。
     *               在v1和以后的版本中，ListOffset API不会返回空时间戳(必要时返回-1)。
     */
    private void handleListOffsetResponse(ListOffsetsResponse listOffsetsResponse,
                                          RequestFuture<ListOffsetResult> future) {
        Map<TopicPartition, ListOffsetData> fetchedOffsets = new HashMap<>();
        Set<TopicPartition> partitionsToRetry = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();

        for (ListOffsetsTopicResponse topic : listOffsetsResponse.topics()) {
            for (ListOffsetsPartitionResponse partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                Errors error = Errors.forCode(partition.errorCode());
                switch (error) {
                    case NONE:
                        if (!partition.oldStyleOffsets().isEmpty()) {
                            // 处理带有偏移量的v0响应
                            long offset;
                            if (partition.oldStyleOffsets().size() > 1) {
                                future.raise(new IllegalStateException("Unexpected partitionData response of length " +
                                        partition.oldStyleOffsets().size()));
                                return;
                            } else {
                                offset = partition.oldStyleOffsets().get(0);
                            }
                            log.debug("Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                                topicPartition, offset);
                            if (offset != ListOffsetsResponse.UNKNOWN_OFFSET) {
                                ListOffsetData offsetData = new ListOffsetData(offset, null, Optional.empty());
                                fetchedOffsets.put(topicPartition, offsetData);
                            }
                        } else {
                            // 处理v1和之后的响应或v0而不进行偏移
                            log.debug("Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
                                topicPartition, partition.offset(), partition.timestamp());
                            if (partition.offset() != ListOffsetsResponse.UNKNOWN_OFFSET) {
                                Optional<Integer> leaderEpoch = (partition.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                                        ? Optional.empty()
                                        : Optional.of(partition.leaderEpoch());
                                ListOffsetData offsetData = new ListOffsetData(partition.offset(), partition.timestamp(),
                                    leaderEpoch);
                                fetchedOffsets.put(topicPartition, offsetData);
                            }
                        }
                        break;
                    case UNSUPPORTED_FOR_MESSAGE_FORMAT:
                        // 代理端的消息格式在0.10.0之前，这意味着它不支持时间戳。
                        // 我们将这种情况视为无法找到与请求的时间戳对应的偏移量并将其排除在结果之外。
                        log.debug("Cannot search by timestamp for partition {} because the message format version " +
                                      "is before 0.10.0", topicPartition);
                        break;
                    case NOT_LEADER_OR_FOLLOWER:
                    case REPLICA_NOT_AVAILABLE:
                    case KAFKA_STORAGE_ERROR:
                    case OFFSET_NOT_AVAILABLE:
                    case LEADER_NOT_AVAILABLE:
                    case FENCED_LEADER_EPOCH:
                    case UNKNOWN_LEADER_EPOCH:
                        log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                            topicPartition, error);
                        partitionsToRetry.add(topicPartition);
                        break;
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        log.warn("Received unknown topic or partition error in ListOffset request for partition {}", topicPartition);
                        partitionsToRetry.add(topicPartition);
                        break;
                    case TOPIC_AUTHORIZATION_FAILED:
                        unauthorizedTopics.add(topicPartition.topic());
                        break;
                    default:
                        log.warn("Attempt to fetch offsets for partition {} failed due to unexpected exception: {}, retrying.",
                            topicPartition, error.message());
                        partitionsToRetry.add(topicPartition);
                }
            }
        }

        if (!unauthorizedTopics.isEmpty())
            future.raise(new TopicAuthorizationException(unauthorizedTopics));
        else
            future.complete(new ListOffsetResult(fetchedOffsets, partitionsToRetry));
    }

    static class ListOffsetResult {
        private final Map<TopicPartition, ListOffsetData> fetchedOffsets;
        private final Set<TopicPartition> partitionsToRetry;

        ListOffsetResult(Map<TopicPartition, ListOffsetData> fetchedOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.fetchedOffsets = fetchedOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        ListOffsetResult() {
            this.fetchedOffsets = new HashMap<>();
            this.partitionsToRetry = new HashSet<>();
        }
    }

    private List<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> exclude = new HashSet<>();
        if (nextInLineFetch != null && !nextInLineFetch.isConsumed) {
            exclude.add(nextInLineFetch.partition);
        }
        for (CompletedFetch completedFetch : completedFetches) {
            exclude.add(completedFetch.partition);
        }
        return subscriptions.fetchablePartitions(tp -> !exclude.contains(tp));
    }

    /**
     * 确定要从哪个副本读取。
     */
    Node selectReadReplica(TopicPartition partition, Node leaderReplica, long currentTimeMs) {
        Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);
        if (nodeId.isPresent()) {
            Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
            if (node.isPresent()) {
                return node.get();
            } else {
                log.trace("Not fetching from {} for partition {} since it is marked offline or is missing from our metadata," +
                          " using the leader instead.", nodeId, partition);
                subscriptions.clearPreferredReadReplica(partition);
                return leaderReplica;
            }
        } else {
            return leaderReplica;
        }
    }

    /**
     * 如果我们看到了新的元数据（由 { @link metadataUpdateVersion} 跟踪），那么我们应该检查所有分配的位置是否有效。
     */
    private void validatePositionsOnMetadataChange() {
        int newMetadataUpdateVersion = metadata.updateVersion();
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            subscriptions.assignedPartitions().forEach(topicPartition -> {
                ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(topicPartition);
                subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, topicPartition, leaderAndEpoch);
            });
        }
    }

    /**
     * 为我们已为其分配了没有正在运行的现有请求的分区的所有节点创建获取请求。
     */
    private Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();

        validatePositionsOnMetadataChange();

        long currentTimeMs = time.milliseconds();

        for (TopicPartition partition : fetchablePartitions()) {
            FetchPosition position = this.subscriptions.position(partition);
            if (position == null) {
                throw new IllegalStateException("Missing position for fetchable partition " + partition);
            }

            Optional<Node> leaderOpt = position.currentLeader.leader;
            if (!leaderOpt.isPresent()) {
                log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                metadata.requestUpdate();
                continue;
            }

            // 如果设置了首选读副本，则使用首选读副本，否则使用该位置的leader
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);
            if (client.isUnavailable(node)) {
                client.maybeThrowAuthFailure(node);

                // 如果我们在重新连接回退窗口期间尝试发送，那么请求在发送之前就会失败，所以现在跳过发送
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            } else if (this.nodesWithPendingFetchRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous request to {} has not been processed", partition, node);
            } else {
                // 如果存在leader并且没有正在执行的请求，则发出一个新的fetch
                FetchSessionHandler.Builder builder = fetchable.get(node);
                if (builder == null) {
                    int id = node.id();
                    FetchSessionHandler handler = sessionHandler(id);
                    if (handler == null) {
                        handler = new FetchSessionHandler(logContext, id);
                        sessionHandlers.put(id, handler);
                    }
                    builder = handler.newBuilder();
                    fetchable.put(node, builder);
                }

                builder.add(partition, new FetchRequest.PartitionData(position.offset,
                    FetchRequest.INVALID_LOG_START_OFFSET, this.fetchSize,
                    position.currentLeader.epoch, Optional.empty()));

                log.debug("Added {} fetch request for partition {} at position {} to node {}", isolationLevel,
                    partition, position, node);
            }
        }

        Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
            reqs.put(entry.getKey(), entry.getValue().build());
        }
        return reqs;
    }

    private Map<Node, Map<TopicPartition, FetchPosition>> regroupFetchPositionsByLeader(
            Map<TopicPartition, FetchPosition> partitionMap) {
        return partitionMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().currentLeader.leader.isPresent())
                .collect(Collectors.groupingBy(entry -> entry.getValue().currentLeader.leader.get(),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private <T> Map<Node, Map<TopicPartition, T>> regroupPartitionMapByNode(Map<TopicPartition, T> partitionMap) {
        return partitionMap.entrySet()
                .stream()
                .collect(Collectors.groupingBy(entry -> metadata.fetch().leaderFor(entry.getKey()),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * 初始化 CompletedFetch 对象。
     */
    private CompletedFetch initializeCompletedFetch(CompletedFetch nextCompletedFetch) {
        TopicPartition tp = nextCompletedFetch.partition;
        FetchResponseData.PartitionData partition = nextCompletedFetch.partitionData;
        long fetchOffset = nextCompletedFetch.nextFetchOffset;
        CompletedFetch completedFetch = null;
        Errors error = Errors.forCode(partition.errorCode());

        try {
            if (!subscriptions.hasValidPosition(tp)) {
                // 这可能发生在重新平衡时，而取回仍然在进行中
                log.debug("Ignoring fetched records for partition {} since it no longer has valid position", tp);
            } else if (error == Errors.NONE) {
                // 只有当开始偏移量匹配当前消耗位置
                FetchPosition position = subscriptions.position(tp);
                if (position == null || position.offset != fetchOffset) {
                    log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                            "the expected offset {}", tp, fetchOffset, position);
                    return null;
                }

                log.trace("Preparing to read {} bytes of data for partition {} with offset {}",
                        FetchResponse.recordsSize(partition), tp, position);
                Iterator<? extends RecordBatch> batches = FetchResponse.recordsOrFail(partition).batches().iterator();
                completedFetch = nextCompletedFetch;

                if (!batches.hasNext() && FetchResponse.recordsSize(partition) > 0) {
                    if (completedFetch.responseVersion < 3) {
                        // Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                        Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
                        throw new RecordTooLargeException("There are some messages at [Partition=Offset]: " +
                                recordTooLargePartitions + " whose size is larger than the fetch size " + this.fetchSize +
                                " and hence cannot be returned. Please considering upgrading your broker to 0.10.1.0 or " +
                                "newer to avoid this issue. Alternately, increase the fetch size on the client (using " +
                                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG + ")",
                                recordTooLargePartitions);
                    } else {
                        // This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
                        throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
                            fetchOffset + ". Received a non-empty fetch response from the server, but no " +
                            "complete records were found.");
                    }
                }

                if (partition.highWatermark() >= 0) {
                    log.trace("Updating high watermark for partition {} to {}", tp, partition.highWatermark());
                    subscriptions.updateHighWatermark(tp, partition.highWatermark());
                }

                if (partition.logStartOffset() >= 0) {
                    log.trace("Updating log start offset for partition {} to {}", tp, partition.logStartOffset());
                    subscriptions.updateLogStartOffset(tp, partition.logStartOffset());
                }

                if (partition.lastStableOffset() >= 0) {
                    log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset());
                    subscriptions.updateLastStableOffset(tp, partition.lastStableOffset());
                }

                if (FetchResponse.isPreferredReplica(partition)) {
                    subscriptions.updatePreferredReadReplica(completedFetch.partition, partition.preferredReadReplica(), () -> {
                        long expireTimeMs = time.milliseconds() + metadata.metadataExpireMs();
                        log.debug("Updating preferred read replica for partition {} to {}, set to expire at {}",
                                tp, partition.preferredReadReplica(), expireTimeMs);
                        return expireTimeMs;
                    });
                }

                nextCompletedFetch.initialized = true;
            } else if (error == Errors.NOT_LEADER_OR_FOLLOWER ||
                       error == Errors.REPLICA_NOT_AVAILABLE ||
                       error == Errors.KAFKA_STORAGE_ERROR ||
                       error == Errors.FENCED_LEADER_EPOCH ||
                       error == Errors.OFFSET_NOT_AVAILABLE) {
                log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
                this.metadata.requestUpdate();
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
                this.metadata.requestUpdate();
            } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
                Optional<Integer> clearedReplicaId = subscriptions.clearPreferredReadReplica(tp);
                if (!clearedReplicaId.isPresent()) {
                    // 如果没有需要清除的首选副本，我们正在从leader取回，所以正常处理这个错误
                    FetchPosition position = subscriptions.position(tp);
                    if (position == null || fetchOffset != position.offset) {
                        log.debug("Discarding stale fetch response for partition {} since the fetched offset {} " +
                                "does not match the current offset {}", tp, fetchOffset, position);
                    } else {
                        handleOffsetOutOfRange(position, tp);
                    }
                } else {
                    log.debug("Unset the preferred read replica {} for partition {} since we got {} when fetching {}",
                            clearedReplicaId.get(), tp, error, fetchOffset);
                }
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                // 我们记录实际分区，而不仅仅是主题，以帮助解决大型集群中的ACL传播问题
                log.warn("Not authorized to read from partition {}.", tp);
                throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
            } else if (error == Errors.UNKNOWN_LEADER_EPOCH) {
                log.debug("Received unknown leader epoch error in fetch for partition {}", tp);
            } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
                log.warn("Unknown server error while fetching offset {} for topic-partition {}",
                        fetchOffset, tp);
            } else if (error == Errors.CORRUPT_MESSAGE) {
                throw new KafkaException("Encountered corrupt message when fetching offset "
                        + fetchOffset
                        + " for topic-partition "
                        + tp);
            } else {
                throw new IllegalStateException("Unexpected error code "
                        + error.code()
                        + " while fetching at offset "
                        + fetchOffset
                        + " from topic-partition " + tp);
            }
        } finally {
            if (completedFetch == null)
                nextCompletedFetch.metricAggregator.record(tp, 0, 0);

            if (error != Errors.NONE)
                // 如果出现错误，我们将分区移到最后。这样，相同主题的分区更有可能保持在一起(允许更有效的序列化)。
                subscriptions.movePartitionToEnd(tp);
        }

        return completedFetch;
    }

    private void handleOffsetOutOfRange(FetchPosition fetchPosition, TopicPartition topicPartition) {
        String errorMessage = "Fetch position " + fetchPosition + " is out of range for partition " + topicPartition;
        if (subscriptions.hasDefaultOffsetResetPolicy()) {
            log.info("{}, resetting offset", errorMessage);
            subscriptions.requestOffsetReset(topicPartition);
        } else {
            log.info("{}, raising error to the application since no reset policy is configured", errorMessage);
            throw new OffsetOutOfRangeException(errorMessage,
                Collections.singletonMap(topicPartition, fetchPosition.offset));
        }
    }

    /**
     * 解析记录条目，必要时反序列化键/值字段
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition,
                                             RecordBatch batch,
                                             Record record) {
        try {
            long offset = record.offset();
            long timestamp = record.timestamp();
            Optional<Integer> leaderEpoch = maybeLeaderEpoch(batch.partitionLeaderEpoch());
            TimestampType timestampType = batch.timestampType();
            Headers headers = new RecordHeaders(record.headers());
            ByteBuffer keyBytes = record.key();
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), headers, keyByteArray);
            ByteBuffer valueBytes = record.value();
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), headers, valueByteArray);
            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                                        timestamp, timestampType,
                                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                                        key, value, headers, leaderEpoch);
        } catch (RuntimeException e) {
            throw new RecordDeserializationException(partition, record.offset(),
                "Error deserializing key/value for partition " + partition +
                    " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
        }
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * 清除不属于新分配分区一部分的缓冲数据
     *
     * @param assignedPartitions  newly assigned {@link TopicPartition}
     */
    public void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions) {
        Iterator<CompletedFetch> completedFetchesItr = completedFetches.iterator();
        while (completedFetchesItr.hasNext()) {
            CompletedFetch records = completedFetchesItr.next();
            TopicPartition tp = records.partition;
            if (!assignedPartitions.contains(tp)) {
                records.drain();
                completedFetchesItr.remove();
            }
        }

        if (nextInLineFetch != null && !assignedPartitions.contains(nextInLineFetch.partition)) {
            nextInLineFetch.drain();
            nextInLineFetch = null;
        }
    }

    /**
     * 清除不属于新分配主题的缓存数据
     *
     * @param assignedTopics  newly assigned topics
     */
    public void clearBufferedDataForUnassignedTopics(Collection<String> assignedTopics) {
        Set<TopicPartition> currentTopicPartitions = new HashSet<>();
        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (assignedTopics.contains(tp.topic())) {
                currentTopicPartitions.add(tp);
            }
        }
        clearBufferedDataForUnassignedPartitions(currentTopicPartitions);
    }

    protected FetchSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    public static Sensor throttleTimeSensor(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
        Sensor fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg), new Avg());

        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax), new Max());

        return fetchThrottleTimeSensor;
    }

    private class CompletedFetch {
        private final TopicPartition partition;
        private final Iterator<? extends RecordBatch> batches;
        private final Set<Long> abortedProducerIds;
        private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions;
        private final FetchResponseData.PartitionData partitionData;
        private final FetchResponseMetricAggregator metricAggregator;
        private final short responseVersion;

        private int recordsRead;
        private int bytesRead;
        private RecordBatch currentBatch;
        private Record lastRecord;
        private CloseableIterator<Record> records;
        private long nextFetchOffset;
        private Optional<Integer> lastEpoch;
        private boolean isConsumed = false;
        private Exception cachedRecordException = null;
        private boolean corruptLastRecord = false;
        private boolean initialized = false;

        private CompletedFetch(TopicPartition partition,
                               FetchResponseData.PartitionData partitionData,
                               FetchResponseMetricAggregator metricAggregator,
                               Iterator<? extends RecordBatch> batches,
                               Long fetchOffset,
                               short responseVersion) {
            this.partition = partition;
            this.partitionData = partitionData;
            this.metricAggregator = metricAggregator;
            this.batches = batches;
            this.nextFetchOffset = fetchOffset;
            this.responseVersion = responseVersion;
            this.lastEpoch = Optional.empty();
            this.abortedProducerIds = new HashSet<>();
            this.abortedTransactions = abortedTransactions(partitionData);
        }

        private void drain() {
            if (!isConsumed) {
                maybeCloseRecordStream();
                cachedRecordException = null;
                this.isConsumed = true;
                this.metricAggregator.record(partition, bytesRead, recordsRead);

                // 如果我们收到一些字节，我们将分区移动到末尾。这样，同一主题的分区更有可能保持在一起（允许更有效的序列化）。
                if (bytesRead > 0)
                    subscriptions.movePartitionToEnd(partition);
            }
        }

        private void maybeEnsureValid(RecordBatch batch) {
            if (checkCrcs && currentBatch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                try {
                    batch.ensureValid();
                } catch (CorruptRecordException e) {
                    throw new KafkaException("Record batch for partition " + partition + " at offset " +
                            batch.baseOffset() + " is invalid, cause: " + e.getMessage());
                }
            }
        }

        private void maybeEnsureValid(Record record) {
            if (checkCrcs) {
                try {
                    record.ensureValid();
                } catch (CorruptRecordException e) {
                    throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                            + " is invalid, cause: " + e.getMessage());
                }
            }
        }

        private void maybeCloseRecordStream() {
            if (records != null) {
                records.close();
                records = null;
            }
        }

        private Record nextFetchedRecord() {
            while (true) {
                if (records == null || !records.hasNext()) {
                    maybeCloseRecordStream();

                    if (!batches.hasNext()) {
                        // 即使通过压缩删除最后一条记录，消息格式 v2 也会保留批次中的最后一个偏移量。通过使用从批次中的最后一个偏移量计算的
                        // 下一个偏移量，我们确保下一次提取的偏移量将指向下一个批次，从而避免不必要地重新提取同一批次（在最坏的情况下，
                        // 消费者可以获得反复获取同一批次）
                        if (currentBatch != null)
                            nextFetchOffset = currentBatch.nextOffset();
                        drain();
                        return null;
                    }

                    currentBatch = batches.next();
                    lastEpoch = currentBatch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                            Optional.empty() : Optional.of(currentBatch.partitionLeaderEpoch());

                    maybeEnsureValid(currentBatch);

                    // 从中止事务队列中删除所有在当前批次的最后一个偏移量之前开始的中止事务，并将关联的 producerIds 添加到中止的生产者集中
                    if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                        consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                        long producerId = currentBatch.producerId();
                        if (containsAbortMarker(currentBatch)) {
                            abortedProducerIds.remove(producerId);
                        } else if (isBatchAborted(currentBatch)) {
                            log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                          "offsets {} to {}",
                                      partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                            nextFetchOffset = currentBatch.nextOffset();
                            continue;
                        }
                    }

                    records = currentBatch.streamingIterator(decompressionBufferSupplier);
                } else {
                    Record record = records.next();
                    // 跳过任何超出范围的记录
                    if (record.offset() >= nextFetchOffset) {
                        // 我们仅在不应跳过消息时进行验证。
                        maybeEnsureValid(record);

                        // 控制记录不返回给用户
                        if (!currentBatch.isControlBatch()) {
                            return record;
                        } else {
                            // 当我们跳过一个控制批次时，增加下一个获取偏移量。
                            nextFetchOffset = record.offset() + 1;
                        }
                    }
                }
            }
        }

        private List<ConsumerRecord<K, V>> fetchRecords(int maxRecords) {
            // Error when fetching the next record before deserialization.
            if (corruptLastRecord)
                throw new KafkaException("Received exception when fetching the next record from " + partition
                                             + ". If needed, please seek past the record to "
                                             + "continue consumption.", cachedRecordException);

            if (isConsumed)
                return Collections.emptyList();

            List<ConsumerRecord<K, V>> records = new ArrayList<>();
            try {
                for (int i = 0; i < maxRecords; i++) {
                    // 只有在最后一次提取中没有异常时才移动到下一条记录。否则我们应该使用最后一条记录再次进行反序列化。
                    if (cachedRecordException == null) {
                        corruptLastRecord = true;
                        lastRecord = nextFetchedRecord();
                        corruptLastRecord = false;
                    }
                    if (lastRecord == null)
                        break;
                    records.add(parseRecord(partition, currentBatch, lastRecord));
                    recordsRead++;
                    bytesRead += lastRecord.sizeInBytes();
                    nextFetchOffset = lastRecord.offset() + 1;
                    // 在某些情况下，反序列化可能会抛出异常并且重试可能会成功，我们允许用户在这种情况下继续前进
                    cachedRecordException = null;
                }
            } catch (SerializationException se) {
                cachedRecordException = se;
                if (records.isEmpty())
                    throw se;
            } catch (KafkaException e) {
                cachedRecordException = e;
                if (records.isEmpty())
                    throw new KafkaException("Received exception when fetching the next record from " + partition
                                                 + ". If needed, please seek past the record to "
                                                 + "continue consumption.", e);
            }
            return records;
        }

        private void consumeAbortedTransactionsUpTo(long offset) {
            if (abortedTransactions == null)
                return;

            while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
                FetchResponseData.AbortedTransaction abortedTransaction = abortedTransactions.poll();
                abortedProducerIds.add(abortedTransaction.producerId());
            }
        }

        private boolean isBatchAborted(RecordBatch batch) {
            return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
        }

        private PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions(FetchResponseData.PartitionData partition) {
            if (partition.abortedTransactions() == null || partition.abortedTransactions().isEmpty())
                return null;

            PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                    partition.abortedTransactions().size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
            );
            abortedTransactions.addAll(partition.abortedTransactions());
            return abortedTransactions;
        }

        private boolean containsAbortMarker(RecordBatch batch) {
            if (!batch.isControlBatch())
                return false;

            Iterator<Record> batchIterator = batch.iterator();
            if (!batchIterator.hasNext())
                return false;

            Record firstRecord = batchIterator.next();
            return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
        }

        private boolean notInitialized() {
            return !this.initialized;
        }
    }

    /**
     * 由于我们从每个提取响应中惰性地解析每个分区的消息数据，因此需要在解析来自每个分区的消息时聚合提取级指标。
     * 该类用于促进这种增量聚合。
     */
    private static class FetchResponseMetricAggregator {
        private final FetchManagerMetrics sensors;
        private final Set<TopicPartition> unrecordedPartitions;

        private final FetchMetrics fetchMetrics = new FetchMetrics();
        private final Map<String, FetchMetrics> topicFetchMetrics = new HashMap<>();

        private FetchResponseMetricAggregator(FetchManagerMetrics sensors,
                                              Set<TopicPartition> partitions) {
            this.sensors = sensors;
            this.unrecordedPartitions = partitions;
        }

        /**
         * 在解析每个分区之后，我们使用解析的总字节数和记录数更新当前度量总数。在所有分区都报告之后，我们编写度量。
         */
        public void record(TopicPartition partition, int bytes, int records) {
            this.unrecordedPartitions.remove(partition);
            this.fetchMetrics.increment(bytes, records);

            // 收集和汇总每个主题的指标
            String topic = partition.topic();
            FetchMetrics topicFetchMetric = this.topicFetchMetrics.get(topic);
            if (topicFetchMetric == null) {
                topicFetchMetric = new FetchMetrics();
                this.topicFetchMetrics.put(topic, topicFetchMetric);
            }
            topicFetchMetric.increment(bytes, records);

            if (this.unrecordedPartitions.isEmpty()) {
                // 一旦报告了所有预期的分区，就记录指标
                this.sensors.bytesFetched.record(this.fetchMetrics.fetchBytes);
                this.sensors.recordsFetched.record(this.fetchMetrics.fetchRecords);

                // 还要记录每个主题的指标
                for (Map.Entry<String, FetchMetrics> entry: this.topicFetchMetrics.entrySet()) {
                    FetchMetrics metric = entry.getValue();
                    this.sensors.recordTopicFetchMetrics(entry.getKey(), metric.fetchBytes, metric.fetchRecords);
                }
            }
        }

        private static class FetchMetrics {
            private int fetchBytes;
            private int fetchRecords;

            protected void increment(int bytes, int records) {
                this.fetchBytes += bytes;
                this.fetchRecords += records;
            }
        }
    }

    private static class FetchManagerMetrics {
        private final Metrics metrics;
        private FetcherMetricsRegistry metricsRegistry;
        private final Sensor bytesFetched;
        private final Sensor recordsFetched;
        private final Sensor fetchLatency;
        private final Sensor recordsFetchLag;
        private final Sensor recordsFetchLead;

        private int assignmentId = 0;
        private Set<TopicPartition> assignedPartitions = Collections.emptySet();

        private FetchManagerMetrics(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
            this.metrics = metrics;
            this.metricsRegistry = metricsRegistry;

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeAvg), new Avg());
            this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeMax), new Max());
            this.bytesFetched.add(new Meter(metrics.metricInstance(metricsRegistry.bytesConsumedRate),
                    metrics.metricInstance(metricsRegistry.bytesConsumedTotal)));

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg), new Avg());
            this.recordsFetched.add(new Meter(metrics.metricInstance(metricsRegistry.recordsConsumedRate),
                    metrics.metricInstance(metricsRegistry.recordsConsumedTotal)));

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyAvg), new Avg());
            this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyMax), new Max());
            this.fetchLatency.add(new Meter(new WindowedCount(), metrics.metricInstance(metricsRegistry.fetchRequestRate),
                    metrics.metricInstance(metricsRegistry.fetchRequestTotal)));

            this.recordsFetchLag = metrics.sensor("records-lag");
            this.recordsFetchLag.add(metrics.metricInstance(metricsRegistry.recordsLagMax), new Max());

            this.recordsFetchLead = metrics.sensor("records-lead");
            this.recordsFetchLead.add(metrics.metricInstance(metricsRegistry.recordsLeadMin), new Min());
        }

        private void recordTopicFetchMetrics(String topic, int bytes, int records) {
            // 获取的记录字节
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic.replace('.', '_'));

                bytesFetched = this.metrics.sensor(name);
                bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeAvg,
                        metricTags), new Avg());
                bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeMax,
                        metricTags), new Max());
                bytesFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicBytesConsumedRate, metricTags),
                        this.metrics.metricInstance(metricsRegistry.topicBytesConsumedTotal, metricTags)));
            }
            bytesFetched.record(bytes);

            // 记录获取的记录
            name = "topic." + topic + ".records-fetched";
            Sensor recordsFetched = this.metrics.getSensor(name);
            if (recordsFetched == null) {
                Map<String, String> metricTags = new HashMap<>(1);
                metricTags.put("topic", topic.replace('.', '_'));

                recordsFetched = this.metrics.sensor(name);
                recordsFetched.add(this.metrics.metricInstance(metricsRegistry.topicRecordsPerRequestAvg,
                        metricTags), new Avg());
                recordsFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedRate, metricTags),
                        this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedTotal, metricTags)));
            }
            recordsFetched.record(records);
        }

        private void maybeUpdateAssignment(SubscriptionState subscription) {
            int newAssignmentId = subscription.assignmentId();
            if (this.assignmentId != newAssignmentId) {
                Set<TopicPartition> newAssignedPartitions = subscription.assignedPartitions();
                for (TopicPartition tp : this.assignedPartitions) {
                    if (!newAssignedPartitions.contains(tp)) {
                        metrics.removeSensor(partitionLagMetricName(tp));
                        metrics.removeSensor(partitionLeadMetricName(tp));
                        metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp));
                    }
                }

                for (TopicPartition tp : newAssignedPartitions) {
                    if (!this.assignedPartitions.contains(tp)) {
                        MetricName metricName = partitionPreferredReadReplicaMetricName(tp);
                        if (metrics.metric(metricName) == null) {
                            metrics.addMetric(
                                metricName,
                                (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1)
                            );
                        }
                    }
                }

                this.assignedPartitions = newAssignedPartitions;
                this.assignmentId = newAssignmentId;
            }
        }

        private void recordPartitionLead(TopicPartition tp, long lead) {
            this.recordsFetchLead.record(lead);

            String name = partitionLeadMetricName(tp);
            Sensor recordsLead = this.metrics.getSensor(name);
            if (recordsLead == null) {
                Map<String, String> metricTags = topicPartitionTags(tp);

                recordsLead = this.metrics.sensor(name);

                recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLead, metricTags), new Value());
                recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadMin, metricTags), new Min());
                recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadAvg, metricTags), new Avg());
            }
            recordsLead.record(lead);
        }

        private void recordPartitionLag(TopicPartition tp, long lag) {
            this.recordsFetchLag.record(lag);

            String name = partitionLagMetricName(tp);
            Sensor recordsLag = this.metrics.getSensor(name);
            if (recordsLag == null) {
                Map<String, String> metricTags = topicPartitionTags(tp);
                recordsLag = this.metrics.sensor(name);

                recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLag, metricTags), new Value());
                recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagMax, metricTags), new Max());
                recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagAvg, metricTags), new Avg());
            }
            recordsLag.record(lag);
        }

        private static String partitionLagMetricName(TopicPartition tp) {
            return tp + ".records-lag";
        }

        private static String partitionLeadMetricName(TopicPartition tp) {
            return tp + ".records-lead";
        }

        private MetricName partitionPreferredReadReplicaMetricName(TopicPartition tp) {
            Map<String, String> metricTags = topicPartitionTags(tp);
            return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
        }

        private Map<String, String> topicPartitionTags(TopicPartition tp) {
            Map<String, String> metricTags = new HashMap<>(2);
            metricTags.put("topic", tp.topic().replace('.', '_'));
            metricTags.put("partition", String.valueOf(tp.partition()));
            return metricTags;
        }
    }

    @Override
    public void close() {
        if (nextInLineFetch != null)
            nextInLineFetch.drain();
        decompressionBufferSupplier.close();
    }

    private Set<String> topicsForPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
    }

}
