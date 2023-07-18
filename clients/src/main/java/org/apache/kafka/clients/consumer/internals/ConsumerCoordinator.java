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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS;
import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME;

/**
 * 该类管理与消费者协调器的协调过程。
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    private final GroupRebalanceConfig rebalanceConfig;
    private final Logger log;
    private final List<ConsumerPartitionAssignor> assignors;
    private final ConsumerMetadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final boolean autoCommitEnabled;
    private final int autoCommitIntervalMs;
    private final ConsumerInterceptors<?, ?> interceptors;
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;

    private boolean isLeader = false;
    private Set<String> joinedSubscription;
    private MetadataSnapshot metadataSnapshot;
    private MetadataSnapshot assignmentSnapshot;
    private Timer nextAutoCommitTimer;
    private AtomicBoolean asyncCommitFenced;
    private ConsumerGroupMetadata groupMetadata;
    private final boolean throwOnFetchStableOffsetsUnsupported;

    // hold onto request&future for committed offset requests to enable async calls.
    private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;

    private static class PendingCommittedOffsetRequest {
        private final Set<TopicPartition> requestedPartitions;
        private final Generation requestedGeneration;
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions,
                                              final Generation generationAtRequestTime,
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.response = Objects.requireNonNull(response);
            this.requestedGeneration = generationAtRequestTime;
        }

        private boolean sameRequest(final Set<TopicPartition> currentRequest, final Generation currentGeneration) {
            return Objects.equals(requestedGeneration, currentGeneration) && requestedPartitions.equals(currentRequest);
        }
    }

    private final RebalanceProtocol protocol;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               List<ConsumerPartitionAssignor> assignors,
                               ConsumerMetadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean throwOnFetchStableOffsetsUnsupported) {
        super(rebalanceConfig,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time);
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch(), metadata.updateVersion());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.pendingAsyncCommits = new AtomicInteger();
        this.asyncCommitFenced = new AtomicBoolean(false);
        this.groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID, JoinGroupRequest.UNKNOWN_MEMBER_ID, rebalanceConfig.groupInstanceId);
        this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;

        if (autoCommitEnabled)
            this.nextAutoCommitTimer = time.timer(autoCommitIntervalMs);

        // 选择的rebalance协议如下:
        // 1.单击“下一步”。只考虑所有赋值器都支持的协议。如果没有跨所有赋值器支持的公共协议，则抛出异常。
        // 2. 如果通常支持多个协议，则选择id最高的协议(即id号表示该协议的高级程度)。
        // 我们知道列表中至少有一个赋值者，不需要再检查NPE
        if (!assignors.isEmpty()) {
            List<RebalanceProtocol> supportedProtocols = new ArrayList<>(assignors.get(0).supportedProtocols());

            for (ConsumerPartitionAssignor assignor : assignors) {
                supportedProtocols.retainAll(assignor.supportedProtocols());
            }

            if (supportedProtocols.isEmpty()) {
                throw new IllegalArgumentException("Specified assignors " +
                    assignors.stream().map(ConsumerPartitionAssignor::name).collect(Collectors.toSet()) +
                    " do not have commonly supported rebalance protocol");
            }

            Collections.sort(supportedProtocols);

            protocol = supportedProtocols.get(supportedProtocols.size() - 1);
        } else {
            protocol = null;
        }

        this.metadata.requestUpdate();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        log.debug("Joining group with current subscription: {}", subscriptions.subscription());
        this.joinedSubscription = subscriptions.subscription();
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocolSet = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();

        List<String> topics = new ArrayList<>(joinedSubscription);
        for (ConsumerPartitionAssignor assignor : assignors) {
            Subscription subscription = new Subscription(topics,
                                                         assignor.subscriptionUserData(joinedSubscription),
                                                         subscriptions.assignedPartitionsList());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

            protocolSet.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(assignor.name())
                    .setMetadata(Utils.toArray(metadata)));
        }
        return protocolSet;
    }

    public void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = cluster.topics().stream()
                .filter(subscriptions::matchesSubscribedPattern)
                .collect(Collectors.toSet());
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics();
    }

    private ConsumerPartitionAssignor lookupAssignor(String name) {
        for (ConsumerPartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    private void maybeUpdateJoinedSubscription(Set<TopicPartition> assignedPartitions) {
        if (subscriptions.hasPatternSubscription()) {
            // 检查分配是否包含一些原始订阅中没有的主题，如果是，我们将遵循领导者的决定并将这些主题添加到订阅中，
            // 只要它们仍然符合订阅模式

            Set<String> addedTopics = new HashSet<>();
            // 这是一个副本，因为它交给下面的听众
            for (TopicPartition tp : assignedPartitions) {
                if (!joinedSubscription.contains(tp.topic()))
                    addedTopics.add(tp.topic());
            }

            if (!addedTopics.isEmpty()) {
                Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
                Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
                newSubscription.addAll(addedTopics);
                newJoinedSubscription.addAll(addedTopics);

                if (this.subscriptions.subscribeFromPattern(newSubscription))
                    metadata.requestUpdateForNewTopics();
                this.joinedSubscription = newJoinedSubscription;
            }
        }
    }

    private Exception invokeOnAssignment(final ConsumerPartitionAssignor assignor, final Assignment assignment) {
        log.info("Notifying assignor about the new {}", assignment);

        try {
            assignor.onAssignment(assignment, groupMetadata);
        } catch (Exception e) {
            return e;
        }

        return null;
    }

    private Exception invokePartitionsAssigned(final Set<TopicPartition> assignedPartitions) {
        log.info("Adding newly assigned partitions: {}", Utils.join(assignedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsAssigned(assignedPartitions);
            sensors.assignCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                listener.getClass().getName(), assignedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsRevoked(final Set<TopicPartition> revokedPartitions) {
        log.info("Revoke previously assigned partitions {}", Utils.join(revokedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsRevoked(revokedPartitions);
            sensors.revokeCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                listener.getClass().getName(), revokedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsLost(final Set<TopicPartition> lostPartitions) {
        log.info("Lost previously assigned partitions {}", Utils.join(lostPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsLost(lostPartitions);
            sensors.loseCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
                listener.getClass().getName(), lostPartitions, e);
            return e;
        }

        return null;
    }

    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        log.debug("Executing onJoinComplete with generation {} and memberId {}", generation, memberId);

        // 只有leader负责监控元数据变化(即分区变化)
        if (!isLeader)
            assignmentSnapshot = null;

        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // 让赋值者有机会根据收到的赋值更新内部状态
        groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);

        Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        // 至少应该对简短版本进行编码吗
        if (assignmentBuffer.remaining() < 2)
            throw new IllegalStateException("There are insufficient bytes available to read assignment from the sync-group response (" +
                "actual byte size " + assignmentBuffer.remaining() + ") , this is not expected; " +
                "it is possible that the leader's assign function is buggy and did not return any assignment for this member, " +
                "or because static member is configured and the protocol is buggy hence did not get the assignment for this member");

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        Set<TopicPartition> assignedPartitions = new HashSet<>(assignment.partitions());

        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            final String reason = String.format("received assignment %s does not match the current subscription %s; " +
                    "it is likely that the subscription has changed since we joined the group, will re-join with current subscription",
                    assignment.partitions(), subscriptions.prettyString());
            requestRejoin(reason);

            return;
        }

        final AtomicReference<Exception> firstException = new AtomicReference<>(null);
        Set<TopicPartition> addedPartitions = new HashSet<>(assignedPartitions);
        addedPartitions.removeAll(ownedPartitions);

        if (protocol == RebalanceProtocol.COOPERATIVE) {
            Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);

            log.info("Updating assignment with\n" +
                    "\tAssigned partitions:                       {}\n" +
                    "\tCurrent owned partitions:                  {}\n" +
                    "\tAdded partitions (assigned - owned):       {}\n" +
                    "\tRevoked partitions (owned - assigned):     {}\n",
                assignedPartitions,
                ownedPartitions,
                addedPartitions,
                revokedPartitions
            );

            if (!revokedPartitions.isEmpty()) {
                // 撤销以前拥有但不再分配的分区;注意，我们应该只在触发revoke回调后才更改赋值(或更新赋值者的状态)
                firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions));

                // 如果撤销了任何分区，则需要随后重新加入组
                final String reason = String.format("need to revoke partitions %s as indicated " +
                        "by the current assignment and re-join", revokedPartitions);
                requestRejoin(reason);
            }
        }

        // leader可能已经分配了与订阅模式匹配的分区，但是没有显式请求，因此我们在这里更新已加入的订阅。
        maybeUpdateJoinedSubscription(assignedPartitions);

        // 捕获这里的任何异常，以确保我们可以完成用户回调。
        firstException.compareAndSet(null, invokeOnAssignment(assignor, assignment));

        // 从现在开始重新安排自动提交
        if (autoCommitEnabled)
            this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);

        subscriptions.assignFromSubscribed(assignedPartitions);

        // 添加以前不拥有但现在已分配的分区
        firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

        if (firstException.get() != null) {
            if (firstException.get() instanceof KafkaException) {
                throw (KafkaException) firstException.get();
            } else {
                throw new KafkaException("User rebalance callback throws an error", firstException.get());
            }
        }
    }

    void maybeUpdateSubscriptionMetadata() {
        int version = metadata.updateVersion();
        if (version > metadataSnapshot.version) {
            Cluster cluster = metadata.fetch();

            if (subscriptions.hasPatternSubscription())
                updatePatternSubscription(cluster);

            // 更新当前快照，它将用于检查需要重新平衡的订阅更改(例如新分区)。
            metadataSnapshot = new MetadataSnapshot(subscriptions, cluster, version);
        }
    }

    /**
     * 轮询协调器事件。这确保了协调器是已知的，并且消费者已经加入了组(如果它正在使用组管理)。
     * 如果启用了定期偏移量提交，则还可以处理这些提交。
     * <p>
     * 如果超时过期或不需要等待重新连接，则提前返回
     *
     * @param timer Timer bounding how long this method can block
     * @param waitForJoinGroup Boolean flag indicating if we should wait until re-join group completes
     * @throws KafkaException if the rebalance callback throws an exception
     * @return true iff the operation succeeded
     */
    public boolean poll(Timer timer, boolean waitForJoinGroup) {
        maybeUpdateSubscriptionMetadata();

        invokeCompletedOffsetCommitCallbacks();

        if (subscriptions.hasAutoAssignedPartitions()) {
            if (protocol == null) {
                throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG +
                    " to empty while trying to subscribe for group protocol to auto assign partitions");
            }
            // 始终更新心跳最后一次轮询时间，这样即使(例如)找不到协调器，心跳线程也不会由于应用程序不活动而主动离开组。
            pollHeartbeat(timer.currentTimeMs());
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            if (rejoinNeededOrPending()) {
                // 由于初始元数据获取和初始再平衡之间存在竞争条件，我们需要确保元数据在初始连接之前是新鲜的。
                // 这确保了我们在加入之前至少针对集群的主题匹配了一次模式。
                if (subscriptions.hasPatternSubscription()) {
                    // 对于使用基于模式订阅的消费者组，在创建主题后，任何在元数据刷新后发现主题的消费者都可以触发整个消费者组的再平衡。
                    // 如果消费者在非常不同的时间刷新元数据，则可以在创建一个主题之后触发多个重新平衡。只要刷新回退时间过了，
                    // 我们就可以要求消费者在重新加入组之前刷新元数据，从而显著减少由单个主题创建引起的重新平衡次数。
                    if (this.metadata.timeToAllowUpdate(timer.currentTimeMs()) == 0) {
                        this.metadata.requestUpdate();
                    }

                    if (!client.ensureFreshMetadata(timer)) {
                        return false;
                    }

                    maybeUpdateSubscriptionMetadata();
                }

                // 如果不等待join group，我们将使用0的计时器
                if (!ensureActiveGroup(waitForJoinGroup ? timer : time.timer(0L))) {
                    // 因为我们可以在被调用者中使用不同的计时器，我们仍然需要在调用后更新原始计时器的当前时间
                    timer.update(time.milliseconds());

                    return false;
                }
            }
        } else {
            // 对于手动分配的分区，如果没有现成的节点，则等待元数据。如果到所有节点的连接失败，则在尝试发送获取请求时触发的唤醒将导致轮询立即返回，
            // 从而导致轮询的紧密循环。如果没有唤醒，没有通道的poll()将阻塞超时，延迟重新连接。
            // awaitmetadatupdate()通过配置回退启动新连接，并避免繁忙循环。当使用组管理时，由于协调器未知，
            // 此场景已经执行元数据等待，因此不需要此检查。
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
                client.awaitMetadataUpdate(timer);
            }
        }

        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        return true;
    }

    /**
     * 下一个所需调用的时间 {@link ConsumerNetworkClient#poll(Timer)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        return Math.min(nextAutoCommitTimer.remainingMs(), timeToNextHeartbeat(now));
    }

    private void updateGroupSubscription(Set<String> topics) {
        // 领导者将开始关注小组感兴趣的任何主题的变化，这确保最终会看到所有元数据的变化
        if (this.subscriptions.groupSubscribe(topics))
            metadata.requestUpdateForNewTopics();

        // 更新元数据(如果需要)并跟踪用于分配的元数据，以便我们可以在重新平衡完成后检查是否有任何更改
        if (!client.ensureFreshMetadata(time.timer(Long.MAX_VALUE)))
            throw new TimeoutException();

        maybeUpdateSubscriptionMetadata();
    }

    private boolean isAssignFromSubscribedTopicsAssignor(String name) {
        return ASSIGN_FROM_SUBSCRIBED_ASSIGNORS.contains(name);
    }

    /**
     * 用户自定义分配器可能已经创建了一些不在订阅列表中的主题，并将其分区分配给成员;在这种情况下，
     * 我们希望使用新添加的主题更新领导者自己的元数据，以便在从元数据刷新更新这些主题时不会触发后续的再平衡。
     * 我们跳过对产品内分配的检查，因为这不会发生在产品内分配中。
     *
     * TODO: 这是一个hack，并不是我们想要长期支持的东西，除非我们将regex推入协议中。
     *  我们可能需要修改ConsumerPartitionAssignor API来更好地支持这种情况。
     *
     * @param assignorName          the selected assignor name
     * @param assignments           the assignments after assignor assigned
     * @param allSubscribedTopics   all consumers' subscribed topics
     */
    private void maybeUpdateGroupSubscription(String assignorName,
                                              Map<String, Assignment> assignments,
                                              Set<String> allSubscribedTopics) {
        if (!isAssignFromSubscribedTopicsAssignor(assignorName)) {
            Set<String> assignedTopics = new HashSet<>();
            for (Assignment assigned : assignments.values()) {
                for (TopicPartition tp : assigned.partitions())
                    assignedTopics.add(tp.topic());
            }

            if (!assignedTopics.containsAll(allSubscribedTopics)) {
                Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
                notAssignedTopics.removeAll(assignedTopics);
                log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
            }

            if (!allSubscribedTopics.containsAll(assignedTopics)) {
                Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
                newlyAddedTopics.removeAll(allSubscribedTopics);
                log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

                allSubscribedTopics.addAll(newlyAddedTopics);
                updateGroupSubscription(allSubscribedTopics);
            }
        }
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        List<JoinGroupResponseData.JoinGroupResponseMember> allSubscriptions) {
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        String assignorName = assignor.name();

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();

        // 收集所有拥有的分区
        Map<String, List<TopicPartition>> ownedPartitions = new HashMap<>();

        for (JoinGroupResponseData.JoinGroupResponseMember memberSubscription : allSubscriptions) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(memberSubscription.metadata()));
            subscription.setGroupInstanceId(Optional.ofNullable(memberSubscription.groupInstanceId()));
            subscriptions.put(memberSubscription.memberId(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
            ownedPartitions.put(memberSubscription.memberId(), subscription.ownedPartitions());
        }

        // 领导者将开始关注小组感兴趣的任何主题的变化，这确保最终会看到所有元数据的变化
        updateGroupSubscription(allSubscribedTopics);

        isLeader = true;

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignorName, subscriptions);

        Map<String, Assignment> assignments = assignor.assign(metadata.fetch(), new GroupSubscription(subscriptions)).groupAssignment();

        // 跳过内置协作sticky赋值器的验证，因为我们已经考虑了ownedPartition在赋值器内部的“生成”
        if (protocol == RebalanceProtocol.COOPERATIVE && !assignorName.equals(COOPERATIVE_STICKY_ASSIGNOR_NAME)) {
            validateCooperativeAssignment(ownedPartitions, assignments);
        }

        maybeUpdateGroupSubscription(assignorName, assignments, allSubscribedTopics);

        assignmentSnapshot = metadataSnapshot;

        log.info("Finished assignment for group at generation {}: {}", generation().generationId, assignments);

        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    /**
     * 仅供COOPERATIVE rebalance协议使用。验证分配器返回的分配，这样就不会将所有的分区直接重新分配给不同的消费者:
     * 如果分配器想要重新分配一个已拥有的分区，它必须首先将其从当前所有者的新分配中删除，这样它就不会分配给任何成员，
     * 然后在下一次重新平衡中，它可以最终将那些不属于任何人的分区重新分配给消费者。
     *
     */
    private void validateCooperativeAssignment(final Map<String, List<TopicPartition>> ownedPartitions,
                                               final Map<String, Assignment> assignments) {
        Set<TopicPartition> totalRevokedPartitions = new HashSet<>();
        Set<TopicPartition> totalAddedPartitions = new HashSet<>();
        for (final Map.Entry<String, Assignment> entry : assignments.entrySet()) {
            final Assignment assignment = entry.getValue();
            final Set<TopicPartition> addedPartitions = new HashSet<>(assignment.partitions());
            addedPartitions.removeAll(ownedPartitions.get(entry.getKey()));
            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions.get(entry.getKey()));
            revokedPartitions.removeAll(assignment.partitions());

            totalAddedPartitions.addAll(addedPartitions);
            totalRevokedPartitions.addAll(revokedPartitions);
        }

        // 如果在被撤销的分区和添加的分区之间存在重叠，这意味着一些分区立即被重新分配给另一个成员，而某些成员仍然声称它
        totalAddedPartitions.retainAll(totalRevokedPartitions);
        if (!totalAddedPartitions.isEmpty()) {
            log.error("With the COOPERATIVE protocol, owned partitions cannot be " +
                "reassigned to other members; however the assignor has reassigned partitions {} which are still owned " +
                "by some members", totalAddedPartitions);

            throw new IllegalStateException("Assignor supporting the COOPERATIVE protocol violates its requirements");
        }
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        log.debug("Executing onJoinPrepare with generation {} and memberId {}", generation, memberId);
        // 如果启用了自动提交，则在重新平衡之前提交偏移量
        maybeAutoCommitOffsetsSync(time.timer(rebalanceConfig.rebalanceTimeoutMs));

        // 生成成员id可以在获得错误或心跳超时时由心跳线程重置;在这种情况下，任何先前拥有的分区都将丢失，
        // 我们应该触发回调并清理分配;否则，我们可以根据协议正常进行并撤销分区，在这种情况下，
        // 我们应该只在revoke回调触发后更改分配，以便用户仍然可以访问以前拥有的分区以提交偏移量等。
        Exception exception = null;
        final Set<TopicPartition> revokedPartitions;
        if (generation == Generation.NO_GENERATION.generationId &&
            memberId.equals(Generation.NO_GENERATION.memberId)) {
            revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());

            if (!revokedPartitions.isEmpty()) {
                log.info("Giving away all assigned partitions as lost since generation has been reset," +
                    "indicating that consumer is no longer part of the group");
                exception = invokePartitionsLost(revokedPartitions);

                subscriptions.assignFromSubscribed(Collections.emptySet());
            }
        } else {
            switch (protocol) {
                case EAGER:
                    // 撤销所有分区
                    revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    exception = invokePartitionsRevoked(revokedPartitions);

                    subscriptions.assignFromSubscribed(Collections.emptySet());

                    break;

                case COOPERATIVE:
                    // 只撤销那些不再属于订阅的分区。
                    Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    revokedPartitions = ownedPartitions.stream()
                        .filter(tp -> !subscriptions.subscription().contains(tp.topic()))
                        .collect(Collectors.toSet());

                    if (!revokedPartitions.isEmpty()) {
                        exception = invokePartitionsRevoked(revokedPartitions);

                        ownedPartitions.removeAll(revokedPartitions);
                        subscriptions.assignFromSubscribed(ownedPartitions);
                    }

                    break;
            }
        }

        isLeader = false;
        subscriptions.resetGroupSubscription();

        if (exception != null) {
            throw new KafkaException("User rebalance callback throws an error", exception);
        }
    }

    @Override
    public void onLeavePrepare() {
        // 保存当前的Generation并使用它来获取memberId，因为hb线程可以随时更改它
        final Generation currentGeneration = generation();
        final String memberId = currentGeneration.memberId;

        log.debug("Executing onLeavePrepare with generation {} and memberId {}", currentGeneration, memberId);

        // 我们应该在离开组之前重置赋值并触发回调
        Set<TopicPartition> droppedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        if (subscriptions.hasAutoAssignedPartitions() && !droppedPartitions.isEmpty()) {
            final Exception e;
            if (generation() == Generation.NO_GENERATION || rebalanceInProgress()) {
                e = invokePartitionsLost(droppedPartitions);
            } else {
                e = invokePartitionsRevoked(droppedPartitions);
            }

            subscriptions.assignFromSubscribed(Collections.emptySet());

            if (e != null) {
                throw new KafkaException("User rebalance callback throws an error", e);
            }
        }
    }

    /**
     * @throws KafkaException if the callback throws exception
     */
    @Override
    public boolean rejoinNeededOrPending() {
        if (!subscriptions.hasAutoAssignedPartitions())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed;
        // also for those owned-but-no-longer-existed partitions we should drop them as lost
        if (assignmentSnapshot != null && !assignmentSnapshot.matches(metadataSnapshot)) {
            final String reason = String.format("cached metadata has changed from %s at the beginning of the rebalance to %s",
                assignmentSnapshot, metadataSnapshot);
            requestRejoin(reason);
            return true;
        }

        // we need to join if our subscription has changed since the last join
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) {
            final String reason = String.format("subscription has changed from %s at the beginning of the rebalance to %s",
                joinedSubscription, subscriptions.subscription());
            requestRejoin(reason);
            return true;
        }

        return super.rejoinNeededOrPending();
    }

    /**
     * 刷新所提供分区的提交偏移量。
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    public boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(initializingPartitions, timer);
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata != null) {
                // 如果需要，首先更新epoch
                entry.getValue().leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));

                // 有可能在收到响应时不再分配分区，因此如果是这种情况，我们需要忽略查找
                if (this.subscriptions.isAssigned(tp)) {
                    final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);
                    final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                            offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                            leaderAndEpoch);

                    this.subscriptions.seekUnvalidated(tp, position);

                    log.info("Setting offset for partition {} to the committed offset {}", tp, position);
                } else {
                    log.info("Ignoring the returned {} since its partition {} is no longer assigned",
                        offsetAndMetadata, tp);
                }
            }
        }
        return true;
    }

    /**
     * 从一组分区的协调器中获取当前提交的偏移量。
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset or null if the operation timed out
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                        final Timer timer) {
        if (partitions.isEmpty()) return Collections.emptyMap();

        final Generation generationForOffsetRequest = generationIfStable();
        if (pendingCommittedOffsetRequest != null &&
            !pendingCommittedOffsetRequest.sameRequest(partitions, generationForOffsetRequest)) {
            // 如果我们在等另一个请求，那就清空它。
            pendingCommittedOffsetRequest = null;
        }

        do {
            if (!ensureCoordinatorReady(timer)) return null;

            // 联系协调器获取承诺的补偿量
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            } else {
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generationForOffsetRequest, future);
            }
            client.poll(future, timer);

            if (future.isDone()) {
                pendingCommittedOffsetRequest = null;

                if (future.succeeded()) {
                    return future.value();
                } else if (!future.isRetriable()) {
                    throw future.exception();
                } else {
                    timer.sleep(rebalanceConfig.retryBackoffMs);
                }
            } else {
                return null;
            }
        } while (timer.notExpired());
        return null;
    }

    /**
     * 返回消费者组元数据。
     *
     * @return the current consumer group metadata
     */
    public ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public void close(final Timer timer) {
        // 我们不需要重新启用唤醒，因为我们已经关闭了
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync(timer);
            while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
                ensureCoordinatorReady(timer);
                client.poll(timer);
                invokeCompletedOffsetCommitCallbacks();
            }
        } finally {
            super.close(timer);
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        if (asyncCommitFenced.get()) {
            throw new FencedInstanceIdException("Get fenced exception for group.instance.id "
                + rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
                + ", current member.id is " + memberId());
        }
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            completion.invoke();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        if (!coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // 我们不知道当前的协调器，所以试着找到它，然后发送提交或失败(我们不希望递归重试，这会导致偏移提交无序到达)。
            // 注意，可能有多个偏移提交链接到同一个协调器查找请求。这很好，因为侦听器将按照添加它们的顺序调用。
            // 还要注意，AbstractCoordinator防止多个并发协调器查找请求。
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }

        // 确保提交有机会被传输(不阻塞其完成)。注意，提交被协调器视为心跳，因此不需要通过延迟的任务执行显式地允许心跳。
        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException) {
                    commitException = new RetriableCommitFailedException(e);
                }
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
                if (commitException instanceof FencedInstanceIdException) {
                    asyncCommitFenced.set(true);
                }
            }
        });
    }

    /**
     * 同步提交偏移量。此方法将重试，直到提交成功完成或遇到不可恢复的错误。
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions. See the exception for more details
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     * @throws FencedInstanceIdException if a static member gets fenced
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty())
            return true;

        do {
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, timer);

            // 当同步提交开始时，我们可能已经进行了动态偏移提交。如果是这样，请确保在返回之前调用相应的回调，
            // 以保持应用偏移量提交的顺序。
            invokeCompletedOffsetCommitCallbacks();

            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
            }

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            timer.sleep(rebalanceConfig.retryBackoffMs);
        } while (timer.notExpired());

        return false;
    }

    public void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            nextAutoCommitTimer.update(now);
            if (nextAutoCommitTimer.isExpired()) {
                nextAutoCommitTimer.reset(autoCommitIntervalMs);
                doAutoCommitOffsetsAsync();
            }
        }
    }

    private void doAutoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        commitOffsetsAsync(allConsumedOffsets, (offsets, exception) -> {
            if (exception != null) {
                if (exception instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", offsets,
                        exception);
                    nextAutoCommitTimer.updateAndReset(rebalanceConfig.retryBackoffMs);
                } else {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
                }
            } else {
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        });
    }

    private void maybeAutoCommitOffsetsSync(Timer timer) {
        if (autoCommitEnabled) {
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
                if (!commitOffsetsSync(allConsumedOffsets, timer))
                    log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
                // 重新抛出唤醒，因为它们是由用户触发的
                throw e;
            } catch (Exception e) {
                // 与异步自动提交失败一致，我们不传播异常
                log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
            }
        }
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    /**
     * 提交指定主题和分区列表的偏移量。这是一个非阻塞调用，它返回一个请求未来，在同步提交的情况下可以轮询，
     * 在异步提交的情况下可以忽略。
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // 创建偏移量提交请求
        Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }

            OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                            new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                    .setName(topicPartition.topic())
                    );

            topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setCommittedOffset(offsetAndMetadata.offset())
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata())
            );
            requestTopicDataMap.put(topicPartition.topic(), topic);
        }

        final Generation generation;
        if (subscriptions.hasAutoAssignedPartitions()) {
            generation = generationIfStable();
            // 如果生成为null，则我们不是活动组的一部分(我们希望是)。我们唯一能做的就是提交失败，让用户在poll()中重新加入组。
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");

                if (rebalanceInProgress()) {
                    // 如果客户端知道它已经在重新平衡，我们可以使用RebalanceInProgressException而不是
                    // CommitFailedException来指示这不是一个致命错误
                    return RequestFuture.failure(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                        "consumer is undergoing a rebalance for auto partition assignment. You can try completing the rebalance " +
                        "by calling poll() and then retry the operation."));
                } else {
                    return RequestFuture.failure(new CommitFailedException("Offset commit cannot be completed since the " +
                        "consumer is not part of an active group for auto partition assignment; it is likely that the consumer " +
                        "was kicked out of the group."));
                }
            }
        } else {
            generation = Generation.NO_GENERATION;
        }

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                new OffsetCommitRequestData()
                        .setGroupId(this.rebalanceConfig.groupId)
                        .setGenerationId(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(rebalanceConfig.groupInstanceId.orElse(null))
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets, generation));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets, Generation generation) {
            super(generation);
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitSensor.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);

                    long offset = offsetAndMetadata.offset();

                    Errors error = Errors.forCode(partition.errorCode());
                    if (error == Errors.NONE) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                    } else {
                        if (error.exception() instanceof RetriableException) {
                            log.warn("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        } else {
                            log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        }

                        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                            future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                            return;
                        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                            unauthorizedTopics.add(tp.topic());
                        } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                                || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                            // raise the error to the user
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS
                                || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                            // just retry
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                                || error == Errors.NOT_COORDINATOR
                                || error == Errors.REQUEST_TIMED_OUT) {
                            markCoordinatorUnknown(error);
                            future.raise(error);
                            return;
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.info("OffsetCommit failed with {} due to group instance id {} fenced", sentGeneration, rebalanceConfig.groupInstanceId);

                            // 如果这一代人发生了变化，或者我们没有处于再平衡阶段，那就不要抛出致命错误，而是在进行再平衡
                            if (generationUnchanged()) {
                                future.raise(error);
                            } else {
                                KafkaException exception;
                                synchronized (ConsumerCoordinator.this) {
                                    if (ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                        exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                            "consumer member's old generation is fenced by its group instance id, it is possible that " +
                                            "this consumer has already participated another rebalance and got a new generation");
                                    } else {
                                        exception = new CommitFailedException();
                                    }
                                }
                                future.raise(exception);
                            }
                            return;
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            /* 消费者不应该尝试在join-group和sync-group之间提交偏移量，因此在代理端，
                            不希望在CompletingRebalance阶段看到提交偏移量请求;如果发生这种情况，
                            那么broker将返回此错误，以表明我们仍处于再平衡的中间。在这种情况下，
                            我们将抛出RebalanceInProgressException，请求重新加入，
                            但不重置代。如果调用者决定重试，他们可以继续并调用poll来完成重新平衡，然后再次尝试提交。
                             */
                            requestRejoin("offset commit failed since group is already rebalancing");
                            future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                "consumer group is executing a rebalance at the moment. You can try completing the rebalance " +
                                "by calling poll() and then retry commit again"));
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            log.info("OffsetCommit failed with {}: {}", sentGeneration, error.message());

                            // 只需要重置代和重新加入组，如果代没有改变或我们没有在重新平衡;否则只会引发rebalance-in-progress错误
                            KafkaException exception;
                            synchronized (ConsumerCoordinator.this) {
                                if (!generationUnchanged() && ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                    exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                        "consumer member's generation is already stale, meaning it has already participated another rebalance and " +
                                        "got a new generation. You can try completing the rebalance by calling poll() and then retry commit again");
                                } else {
                                    resetGenerationOnResponseError(ApiKeys.OFFSET_COMMIT, error);
                                    exception = new CommitFailedException();
                                }
                            }
                            future.raise(exception);
                            return;
                        } else {
                            future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                            return;
                        }
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * 获取一组分区的已提交偏移量。这是一个非阻塞呼叫。可以轮询返回的未来以获得从代理返回的实际偏移量。
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder =
            new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId, true, new ArrayList<>(partitions), throwOnFetchStableOffsetsUnsupported);

        // 发送带有回调的请求
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        private OffsetFetchResponseHandler() {
            super(Generation.NO_GENERATION);
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            Errors responseError = response.groupLevelError(rebalanceConfig.groupId);
            if (responseError != Errors.NONE) {
                log.debug("Offset fetch failed: {}", responseError.message());

                if (responseError == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    future.raise(responseError);
                } else if (responseError == Errors.NOT_COORDINATOR) {
                    markCoordinatorUnknown(responseError);
                    future.raise(responseError);
                } else if (responseError == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + responseError.message()));
                }
                return;
            }

            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData =
                response.partitionDataMap(rebalanceConfig.groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(responseData.size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : responseData.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +
                            tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // 用偏移量记录位置(-1表示没有提交的偏移量要获取);如果没有提交偏移量，则记录为空
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            if (unauthorizedTopics != null) {
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // just retry
                log.info("The following partitions still have unstable offsets " +
                             "which are not cleared on the broker side: {}" +
                             ", this could be either " +
                             "transactional offsets waiting for completion, or " +
                             "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                future.raise(new UnstableOffsetCommitException("There are unstable offsets for the requested topic partitions"));
            } else {
                future.complete(offsets);
            }
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitSensor;
        private final Sensor revokeCallbackSensor;
        private final Sensor assignCallbackSensor;
        private final Sensor loseCallbackSensor;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitSensor = metrics.sensor("commit-latency");
            this.commitSensor.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitSensor.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitSensor.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            this.revokeCallbackSensor = metrics.sensor("partition-revoked-latency");
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-revoked rebalance listener callback"), new Avg());
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-revoked rebalance listener callback"), new Max());

            this.assignCallbackSensor = metrics.sensor("partition-assigned-latency");
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-assigned rebalance listener callback"), new Avg());
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-assigned rebalance listener callback"), new Max());

            this.loseCallbackSensor = metrics.sensor("partition-lost-latency");
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-lost rebalance listener callback"), new Avg());
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-lost rebalance listener callback"), new Max());

            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final int version;
        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster, int version) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.metadataTopics()) {
                Integer numPartitions = cluster.partitionCountForTopic(topic);
                if (numPartitions != null)
                    partitionsPerTopic.put(topic, numPartitions);
            }
            this.partitionsPerTopic = partitionsPerTopic;
            this.version = version;
        }

        boolean matches(MetadataSnapshot other) {
            return version == other.version || partitionsPerTopic.equals(other.partitionsPerTopic);
        }

        @Override
        public String toString() {
            return "(version" + version + ": " + partitionsPerTopic + ")";
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

    /* test-only classes below */
    RebalanceProtocol getProtocol() {
        return protocol;
    }

    boolean poll(Timer timer) {
        return poll(timer, true);
    }
}
