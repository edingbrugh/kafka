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

package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry;
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;


public class KafkaConsumer<K, V> implements Consumer<K, V> {

    private static final String CLIENT_ID_METRIC_TAG = "client-id";
    private static final long NO_CURRENT_THREAD = -1L;
    private static final String JMX_PREFIX = "kafka.consumer";
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    // Visible for testing
    final Metrics metrics;
    final KafkaConsumerMetrics kafkaConsumerMetrics;

    private Logger log;
    private final String clientId;
    private final Optional<String> groupId;
    private final ConsumerCoordinator coordinator;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Fetcher<K, V> fetcher;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;

    private final Time time;
    private final ConsumerNetworkClient client;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final int defaultApiTimeoutMs;
    private volatile boolean closed = false;
    private List<ConsumerPartitionAssignor> assignors;

    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    private final AtomicInteger refcount = new AtomicInteger(0);

    private boolean cachedSubscriptionHashAllFetchPositions;


    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }


    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }


    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(Utils.propsToMap(properties), keyDeserializer, valueDeserializer);
    }


    public KafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    @SuppressWarnings("unchecked")
    KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);

            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);

            LogContext logContext;

            // If group.instance.id is set, we will append it to the log context.
            if (groupRebalanceConfig.groupInstanceId.isPresent()) {
                logContext = new LogContext("[Consumer instanceId=" + groupRebalanceConfig.groupInstanceId.get() +
                        ", clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
            } else {
                logContext = new LogContext("[Consumer clientId=" + clientId + ", groupId=" + groupId.orElse("null") + "] ");
            }

            this.log = logContext.logger(getClass());
            boolean enableAutoCommit = config.maybeOverrideEnableAutoCommit();
            groupId.ifPresent(groupIdStr -> {
                if (groupIdStr.isEmpty()) {
                    log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");
                }
            });

            log.debug("Initializing the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = Time.SYSTEM;
            this.metrics = buildMetrics(config, time, clientId);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            List<ConsumerInterceptor<K, V>> interceptorList = (List) config.getConfiguredInstances(
                    ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ConsumerInterceptor.class,
                    Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.keyDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), true);
            } else {
                config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
                this.valueDeserializer.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)), false);
            } else {
                config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
                this.valueDeserializer = valueDeserializer;
            }
            OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
            this.subscriptions = new SubscriptionState(logContext, offsetResetStrategy);
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keyDeserializer,
                    valueDeserializer, metrics.reporters(), interceptorList);
            this.metadata = new ConsumerMetadata(retryBackoffMs,
                    config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG),
                    !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG),
                    config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                    subscriptions, logContext, clusterResourceListeners);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG));
            this.metadata.bootstrap(addresses);
            String metricGrpPrefix = "consumer";

            FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry(Collections.singleton(CLIENT_ID_METRIC_TAG), metricGrpPrefix);
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext);
            this.isolationLevel = IsolationLevel.valueOf(
                    config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
            Sensor throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry);
            int heartbeatIntervalMs = config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

            ApiVersions apiVersions = new ApiVersions();
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder, logContext),
                    this.metadata,
                    clientId,
                    100, // a fixed large enough value will suffice for max in-flight requests
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time,
                    true,
                    apiVersions,
                    throttleTimeSensor,
                    logContext);
            this.client = new ConsumerNetworkClient(
                    logContext,
                    netClient,
                    metadata,
                    time,
                    retryBackoffMs,
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    heartbeatIntervalMs); //Will avoid blocking an extended period of time to prevent heartbeat thread starvation

            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // no coordinator will be constructed for the default (null) group id
            this.coordinator = !groupId.isPresent() ? null :
                    new ConsumerCoordinator(groupRebalanceConfig,
                            logContext,
                            this.client,
                            assignors,
                            this.metadata,
                            this.subscriptions,
                            metrics,
                            metricGrpPrefix,
                            this.time,
                            enableAutoCommit,
                            config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                            this.interceptors,
                            config.getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
            this.fetcher = new Fetcher<>(
                    logContext,
                    this.client,
                    config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                    config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                    config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                    config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG),
                    config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
                    this.keyDeserializer,
                    this.valueDeserializer,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricsRegistry,
                    this.time,
                    this.retryBackoffMs,
                    this.requestTimeoutMs,
                    isolationLevel,
                    apiVersions);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, metricGrpPrefix);

            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(0, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    // visible for testing
    KafkaConsumer(LogContext logContext,
                  String clientId,
                  ConsumerCoordinator coordinator,
                  Deserializer<K> keyDeserializer,
                  Deserializer<V> valueDeserializer,
                  Fetcher<K, V> fetcher,
                  ConsumerInterceptors<K, V> interceptors,
                  Time time,
                  ConsumerNetworkClient client,
                  Metrics metrics,
                  SubscriptionState subscriptions,
                  ConsumerMetadata metadata,
                  long retryBackoffMs,
                  long requestTimeoutMs,
                  int defaultApiTimeoutMs,
                  List<ConsumerPartitionAssignor> assignors,
                  String groupId) {
        this.log = logContext.logger(getClass());
        this.clientId = clientId;
        this.coordinator = coordinator;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.fetcher = fetcher;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = Objects.requireNonNull(interceptors);
        this.time = time;
        this.client = client;
        this.metrics = metrics;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.assignors = assignors;
        this.groupId = Optional.ofNullable(groupId);
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
    }

    private static Metrics buildMetrics(ConsumerConfig config, Time time, String clientId) {
        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricsTags);
        List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class, Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId));
        JmxReporter jmxReporter = new JmxReporter();
        jmxReporter.configure(config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId)));
        reporters.add(jmxReporter);
        MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
        return new Metrics(metricConfig, reporters, time, metricsContext);
    }


    public Set<TopicPartition> assignment() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
        } finally {
            release();
        }
    }


    public Set<String> subscription() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(new HashSet<>(this.subscriptions.subscription()));
        } finally {
            release();
        }
    }


    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            if (topics == null) {
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            }
            if (topics.isEmpty()) {
                // 将订阅空主题列表视为与取消订阅相同
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    if (Utils.isBlank(topic)) {
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                    }
                }

                throwIfNoAssignorsConfigured();
                fetcher.clearBufferedDataForUnassignedTopics(topics);
                log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));
                if (this.subscriptions.subscribe(new HashSet<>(topics), listener)) {
                    metadata.requestUpdateForNewTopics();
                }
            }
        } finally {
            release();
        }
    }


    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }


    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        maybeThrowInvalidGroupIdException();
        if (pattern == null || pattern.toString().equals("")) {
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));
        }

        acquireAndEnsureOpen();
        try {
            throwIfNoAssignorsConfigured();
            log.info("Subscribed to pattern: '{}'", pattern);
            this.subscriptions.subscribe(pattern, listener);
            this.coordinator.updatePatternSubscription(metadata.fetch());
            this.metadata.requestUpdateForNewTopics();
        } finally {
            release();
        }
    }


    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)} or {@link #subscribe(Pattern)}.
     * This also clears any partitions directly assigned through {@link #assign(Collection)}.
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. rebalance callback errors)
     */
    public void unsubscribe() {
        acquireAndEnsureOpen();
        try {
            fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
            if (this.coordinator != null) {
                this.coordinator.onLeavePrepare();
                this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
            }
            this.subscriptions.unsubscribe();
            log.info("Unsubscribed all topics or patterns and assigned partitions");
        } finally {
            release();
        }
    }


    @Override
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {
                this.unsubscribe();
            } else {
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (Utils.isBlank(topic)) {
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                    }
                }
                fetcher.clearBufferedDataForUnassignedPartitions(partitions);

                // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                if (coordinator != null) {
                    this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());
                }

                log.info("Subscribed to partition(s): {}", Utils.join(partitions, ", "));
                if (this.subscriptions.assignFromUser(new HashSet<>(partitions))) {
                    metadata.requestUpdateForNewTopics();
                }
            }
        } finally {
            release();
        }
    }


    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(time.timer(timeoutMs), false);
    }


    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return poll(time.timer(timeout), true);
    }

    /**
     * @throws KafkaException 如果重新平衡回调抛出异常
     */
    private ConsumerRecords<K, V> poll(final Timer timer, final boolean includeMetadataInTimeout) {
        acquireAndEnsureOpen();
        try {
            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            do {
                client.maybeTriggerWakeup();

                if (includeMetadataInTimeout) {
                    // 尝试更新分配元数据，但不需要阻塞加入组的计时器
                    updateAssignmentMetadataIfNeeded(timer, false);
                } else {
                    while (!updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE), true)) {
                        log.warn("Still waiting for metadata");
                    }
                }

                final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollForFetches(timer);
                if (!records.isEmpty()) {

                    if (fetcher.sendFetches() > 0 || client.hasPendingRequests()) {
                        client.transmitSends();
                    }

                    //  Map<TopicPartition, List<ConsumerRecord<K, V>>> ---> Map<TopicPartition, List<ConsumerRecord<K, V>>>
                    return this.interceptors.onConsume(new ConsumerRecords<>(records));
                }
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            release();
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer, final boolean waitForJoinGroup) {
        if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) {
            return false;
        }

        return updateFetchPositions(timer);
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollForFetches(Timer timer) {
        long pollTimeout = coordinator == null ? timer.remainingMs() :
                Math.min(coordinator.timeToNextPoll(timer.currentTimeMs()), timer.remainingMs());

        //如果数据已经可用，请立即返回
        final Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
        if (!records.isEmpty()) {
            return records;
        }

        // 发送任何新的取回(不会重新发送挂起的取回)
        fetcher.sendFetches();

        // 如果我们错过了一些位置，我们不想在poll中阻塞因为偏移量查找可能在失败后退出
        // 注意:使用cachedSubscriptionHashAllFetchPositions意味着我们必须调用updateassignmentmetadataif在此方法之前需要。
        if (!cachedSubscriptionHashAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);
        client.poll(pollTimer, () -> {
            // 由于获取操作可能由后台线程完成，所以我们需要这个轮询条件确保我们在poll()中没有不必要的阻塞
            return !fetcher.hasAvailableFetches();
        });
        timer.update(pollTimer.currentTimeMs());

        return fetcher.fetchedRecords();
    }


    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public void commitSync(Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }


    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            if (!coordinator.commitOffsetsSync(new HashMap<>(offsets), time.timer(timeout))) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before successfully " +
                        "committing offsets " + offsets);
            }
        } finally {
            release();
        }
    }


    @Override
    public void commitAsync() {
        commitAsync(null);
    }


    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }


    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            log.debug("Committing offsets: {}", offsets);
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            coordinator.commitOffsetsAsync(new HashMap<>(offsets), callback);
        } finally {
            release();
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        acquireAndEnsureOpen();
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offset,
                    Optional.empty(), // This will ensure we skip validation
                    this.metadata.currentLeader(partition));
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }


    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        acquireAndEnsureOpen();
        try {
            if (offsetAndMetadata.leaderEpoch().isPresent()) {
                log.info("Seeking to offset {} for partition {} with epoch {}",
                        offset, partition, offsetAndMetadata.leaderEpoch().get());
            } else {
                log.info("Seeking to offset {} for partition {}", offset, partition);
            }
            Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    offsetAndMetadata.offset(),
                    offsetAndMetadata.leaderEpoch(),
                    currentLeaderAndEpoch);
            this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
            this.subscriptions.seekUnvalidated(partition, newPosition);
        } finally {
            release();
        }
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException    if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Partitions collection cannot be null");
        }

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.EARLIEST);
        } finally {
            release();
        }
    }


    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Partitions collection cannot be null");
        }

        acquireAndEnsureOpen();
        try {
            Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.LATEST);
        } finally {
            release();
        }
    }


    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!this.subscriptions.isAssigned(partition)) {
                throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");
            }

            Timer timer = time.timer(timeout);
            do {
                SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
                if (position != null) {
                    return position.offset;
                }

                updateFetchPositions(timer);
                client.poll(timer);
            } while (timer.notExpired());

            throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                    "for partition " + partition + " could be determined");
        } finally {
            release();
        }
    }


    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
        return committed(Collections.singleton(partition), timeout).get(partition);
    }


    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            Map<TopicPartition, OffsetAndMetadata> offsets = coordinator.fetchCommittedOffsets(partitions, time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                        "committed offset for partitions " + partitions + " could be determined. Try tuning default.api.timeout.ms " +
                        "larger to relax the threshold.");
            } else {
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            release();
        }
    }


    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }


    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty()) {
                return parts;
            }

            Timer timer = time.timer(timeout);
            Map<String, List<PartitionInfo>> topicMetadata = fetcher.getTopicMetadata(
                    new MetadataRequest.Builder(Collections.singletonList(topic), metadata.allowAutoTopicCreation()), timer);
            return topicMetadata.getOrDefault(topic, Collections.emptyList());
        } finally {
            release();
        }
    }


    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return fetcher.getAllTopicMetadata(time.timer(timeout));
        } finally {
            release();
        }
    }


    @Override
    public void pause(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Pausing partitions {}", partitions);
            for (TopicPartition partition : partitions) {
                subscriptions.pause(partition);
            }
        } finally {
            release();
        }
    }


    @Override
    public void resume(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            log.debug("Resuming partitions {}", partitions);
            for (TopicPartition partition : partitions) {
                subscriptions.resume(partition);
            }
        } finally {
            release();
        }
    }


    @Override
    public Set<TopicPartition> paused() {
        acquireAndEnsureOpen();
        try {
            return Collections.unmodifiableSet(subscriptions.pausedPartitions());
        } finally {
            release();
        }
    }


    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
                // we explicitly exclude the earliest and latest offset here so the timestamp in the returned
                // OffsetAndTimestamp is always positive.
                if (entry.getValue() < 0) {
                    throw new IllegalArgumentException("The target time for partition " + entry.getKey() + " is " +
                            entry.getValue() + ". The target time cannot be negative.");
                }
            }
            return fetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout));
        } finally {
            release();
        }
    }


    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return fetcher.beginningOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }


    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }


    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            return fetcher.endOffsets(partitions, time.timer(timeout));
        } finally {
            release();
        }
    }


    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        acquireAndEnsureOpen();
        try {
            final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

            if (lag == null) {
                if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                        !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                    log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                    subscriptions.requestPartitionEndOffset(topicPartition);
                    fetcher.endOffsets(Collections.singleton(topicPartition), time.timer(0L));
                }

                return OptionalLong.empty();
            }

            return OptionalLong.of(lag);
        } finally {
            release();
        }
    }


    @Override
    public ConsumerGroupMetadata groupMetadata() {
        acquireAndEnsureOpen();
        try {
            maybeThrowInvalidGroupIdException();
            return coordinator.groupMetadata();
        } finally {
            release();
        }
    }


    @Override
    public void enforceRebalance() {
        acquireAndEnsureOpen();
        try {
            if (coordinator == null) {
                throw new IllegalStateException("Tried to force a rebalance but consumer does not have a group.");
            }
            coordinator.requestRejoin("rebalance enforced by user");
        } finally {
            release();
        }
    }


    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }


    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }
        acquire();
        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout.toMillis(), false);
            }
        } finally {
            closed = true;
            release();
        }
    }


    @Override
    public void wakeup() {
        this.client.wakeup();
    }

    private ClusterResourceListeners configureClusterResourceListeners(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList : candidateLists) {
            clusterResourceListeners.maybeAddAll(candidateList);
        }

        clusterResourceListeners.maybeAdd(keyDeserializer);
        clusterResourceListeners.maybeAdd(valueDeserializer);
        return clusterResourceListeners;
    }

    private void close(long timeoutMs, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        try {
            if (coordinator != null) {
                coordinator.close(time.timer(Math.min(timeoutMs, requestTimeoutMs)));
            }
        } catch (Throwable t) {
            firstException.compareAndSet(null, t);
            log.error("Failed to close coordinator", t);
        }
        Utils.closeQuietly(fetcher, "fetcher", firstException);
        Utils.closeQuietly(interceptors, "consumer interceptors", firstException);
        Utils.closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        Utils.closeQuietly(metrics, "consumer metrics", firstException);
        Utils.closeQuietly(client, "consumer network client", firstException);
        Utils.closeQuietly(keyDeserializer, "consumer key deserializer", firstException);
        Utils.closeQuietly(valueDeserializer, "consumer value deserializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }


    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        fetcher.validateOffsetsIfNeeded();

        cachedSubscriptionHashAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHashAllFetchPositions) {
            return true;
        }

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        if (coordinator != null && !coordinator.refreshCommittedOffsetsIfNeeded(timer)) {
            return false;
        }

        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise an exception.
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to lookup and update the positions of any
        // partitions which are awaiting reset.
        fetcher.resetOffsetsIfNeeded();

        return true;
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multi-threaded usage is not
     * supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)) {
            throw new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access");
        }
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty()) {
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
        }
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null) {
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
        }
    }

    // Functions below are for testing only
    String getClientId() {
        return clientId;
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return updateAssignmentMetadataIfNeeded(timer, true);
    }
}
