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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.internals.Fetcher.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

/**
 * 用于为使用者跟踪主题、分区和偏移量的类。分区被直接“分配”给 {@link #assignFromUser(Set)} (手工作业)
 * 或与 {@link #assignFromSubscribed(Collection)} (订阅自动分配).
 *
 * 一旦分配了分区，分区就不会被认为是“可获取的”，直到将其初始位置设置为 {@link #seekValidated(TopicPartition, FetchPosition)}.
 * 可读取分区跟踪一个读取位置(用于设置下一次读取的偏移量)和一个已消耗位置(已返回给用户的最后一个偏移量)。您可以通过
 * {@link #pause(TopicPartition)} 不影响获取消耗的补偿。分区将保持不可取状态，直到 {@link #resume(TopicPartition)} 使用。
 * 还可以使用独立查询暂停状态 {@link #isPaused(TopicPartition)}.
 *
 * 请注意，当分区分配由用户直接更改或通过组rebalance更改时，
 * 暂停状态和fetchconsuming位置都不会保留。
 * 线程安全:这个类是线程安全的

 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private final Logger log;

    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    }

    /* the type of subscription */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    private Set<String> subscription;

    /* 该组已订阅的主题列表。这可能包括一些不属于组leader“订阅”的主题，因为它负责检测需要组再平衡的元数据更改。
     */
    private Set<String> groupSubscription;

    /* 当前分配的分区，请注意分区的顺序很重要(有关详细信息，请参阅FetchBuilder) */
    private final PartitionStates<TopicPartitionState> assignment;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    /* User-provided listener to be invoked when assignment changes */
    private ConsumerRebalanceListener rebalanceListener;

    private int assignmentId = 0;

    @Override
    public synchronized String toString() {
        return "SubscriptionState{" +
            "type=" + subscriptionType +
            ", subscribedPattern=" + subscribedPattern +
            ", subscription=" + String.join(",", subscription) +
            ", groupSubscription=" + String.join(",", groupSubscription) +
            ", defaultResetStrategy=" + defaultResetStrategy +
            ", assignment=" + assignment.partitionStateValues() + " (id=" + assignmentId + ")}";
    }

    public synchronized String prettyString() {
        switch (subscriptionType) {
            case NONE:
                return "None";
            case AUTO_TOPICS:
                return "Subscribe(" + String.join(",", subscription) + ")";
            case AUTO_PATTERN:
                return "Subscribe(" + subscribedPattern + ")";
            case USER_ASSIGNED:
                return "Assign(" + assignedPartitions() + " , id=" + assignmentId + ")";
            default:
                throw new IllegalStateException("Unrecognized subscription type: " + subscriptionType);
        }
    }

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * 单调递增的id，每次赋值改变后递增。这可以用于检查何时赋值发生了更改。
     *
     * @return The current assignment Id
     */
    synchronized int assignmentId() {
        return assignmentId;
    }

    /**
     * 如果订阅类型尚未设置(即当它为NONE时)，该方法将设置订阅类型，
     * 或者在设置订阅类型时验证订阅类型是否等于给定类型(即当它不是NONE时)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public synchronized boolean subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);
        return changeSubscription(topics);
    }

    public synchronized void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        registerRebalanceListener(listener);
        setSubscriptionType(SubscriptionType.AUTO_PATTERN);
        this.subscribedPattern = pattern;
    }

    public synchronized boolean subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);

        return changeSubscription(topics);
    }

    private boolean changeSubscription(Set<String> topicsToSubscribe) {
        if (subscription.equals(topicsToSubscribe))
            return false;

        subscription = topicsToSubscribe;
        return true;
    }

    /**
     * 设置当前组订阅。这是由组负责人使用的，以确保它接收到组感兴趣的所有主题的元数据更新。
     *
     * @param topics All topics from the group subscription
     * @return 如果组订阅包含不属于本地订阅的主题，则为True
     */
    synchronized boolean groupSubscribe(Collection<String> topics) {
        if (!hasAutoAssignedPartitions())
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        groupSubscription = new HashSet<>(topics);
        return !subscription.containsAll(groupSubscription);
    }

    /**
     * 将组的订阅重置为仅包含该消费者订阅的主题。
     */
    synchronized void resetGroupSubscription() {
        groupSubscription = Collections.emptySet();
    }

    /**
     * 更改分配给用户提供的指定分区，注意这与 {@link #assignFromSubscribed(Collection)}
     * 其输入分区由订阅的主题提供。
     */
    public synchronized boolean assignFromUser(Set<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (this.assignment.partitionSet().equals(partitions))
            return false;

        assignmentId++;

        // 更新订阅的主题
        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TopicPartitionState state = assignment.stateValue(partition);
            if (state == null)
                state = new TopicPartitionState();
            partitionToState.put(partition, state);

            manualSubscribedTopics.add(partition.topic());
        }

        this.assignment.set(partitionToState);
        return changeSubscription(manualSubscribedTopics);
    }

    /**
     * @return 如果分配匹配订阅，则为True，否则为false
     */
    public synchronized boolean checkAssignmentMatchedSubscription(Collection<TopicPartition> assignments) {
        for (TopicPartition topicPartition : assignments) {
            if (this.subscribedPattern != null) {
                if (!this.subscribedPattern.matcher(topicPartition.topic()).matches()) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                        topicPartition,
                        this.subscribedPattern);

                    return false;
                }
            } else {
                if (!this.subscription.contains(topicPartition.topic())) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 将分配值更改为从协调器返回的指定分区，注意这与 {@link #assignFromUser(Set)} 它直接从用户输入设置赋值。
     */
    public synchronized void assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.hasAutoAssignedPartitions())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments) {
            TopicPartitionState state = this.assignment.stateValue(tp);
            if (state == null)
                state = new TopicPartitionState();
            assignedPartitionStates.put(tp, state);
        }

        assignmentId++;
        this.assignment.set(assignedPartitionStates);
    }

    private void registerRebalanceListener(ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        this.rebalanceListener = listener;
    }

    /**
     * 检查是否正在使用模式订阅。
     *
     */
    synchronized boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public synchronized void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.groupSubscription = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
        this.assignmentId++;
    }

    /**
     * 检查主题是否与订阅的模式匹配。
     *
     * @return true if pattern subscription is in use and the topic matches the subscribed pattern, false otherwise
     */
    synchronized boolean matchesSubscribedPattern(String topic) {
        Pattern pattern = this.subscribedPattern;
        if (hasPatternSubscription() && pattern != null)
            return pattern.matcher(topic).matches();
        return false;
    }

    public synchronized Set<String> subscription() {
        if (hasAutoAssignedPartitions())
            return this.subscription;
        return Collections.emptySet();
    }

    public synchronized Set<TopicPartition> pausedPartitions() {
        return collectPartitions(TopicPartitionState::isPaused);
    }

    /**
     * 获取需要元数据的订阅主题。对于leader，这将包括所有组成员订阅的联合。对于追随者来说，它只是会员的订阅。
     * 这在查询主题元数据以检测需要重新平衡的元数据更改时使用。leader获取组中所有主题的元数据，
     * 以便执行分区分配(这至少需要分配所有主题的分区计数)。
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    synchronized Set<String> metadataTopics() {
        if (groupSubscription.isEmpty())
            return subscription;
        else if (groupSubscription.containsAll(subscription))
            return groupSubscription;
        else {
            // 当订阅发生更改时，“groupSubscription”可能已经过期，请确保返回新的订阅主题。
            Set<String> topics = new HashSet<>(groupSubscription);
            topics.addAll(subscription);
            return topics;
        }
    }

    synchronized boolean needsMetadata(String topic) {
        return subscription.contains(topic) || groupSubscription.contains(topic);
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    private TopicPartitionState assignedStateOrNull(TopicPartition tp) {
        return this.assignment.stateValue(tp);
    }

    public synchronized void seekValidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekValidated(position);
    }

    public void seek(TopicPartition tp, long offset) {
        seekValidated(tp, new FetchPosition(offset));
    }

    public void seekUnvalidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekUnvalidated(position);
    }

    synchronized void maybeSeekUnvalidated(TopicPartition tp, FetchPosition position, OffsetResetStrategy requestedResetStrategy) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
        } else if (!state.awaitingReset()) {
            log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
        } else if (requestedResetStrategy != state.resetStrategy) {
            log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
        } else {
            log.info("Resetting offset for partition {} to position {}.", tp, position);
            state.seekUnvalidated(position);
        }
    }

    /**
     * @return 当前分配分区的可修改副本
     */
    public synchronized Set<TopicPartition> assignedPartitions() {
        return new HashSet<>(this.assignment.partitionSet());
    }

    /**
     * @return 作为列表的当前分配分区的可修改副本
     */
    public synchronized List<TopicPartition> assignedPartitionsList() {
        return new ArrayList<>(this.assignment.partitionSet());
    }

    /**
     * 以线程安全的方式提供分配的分区数。
     * @return the number of assigned partitions.
     */
    synchronized int numAssignedPartitions() {
        return this.assignment.size();
    }

    public synchronized List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        // 由于这是在获取的热路径中，所以我们使用java.util.stream API来代替它
        List<TopicPartition> result = new ArrayList<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            // Cheap check is first to avoid evaluating the predicate if possible
            if (topicPartitionState.isFetchable() && isAvailable.test(topicPartition)) {
                result.add(topicPartition);
            }
        });
        return result;
    }

    public synchronized boolean hasAutoAssignedPartitions() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public synchronized void position(TopicPartition tp, FetchPosition position) {
        assignedState(tp).position(position);
    }

    /**
     * 如果已知此分区的leader支持OffsetsForLeaderEpoch API的可用版本，则输入偏移验证状态。如果领导节点不支持API，只需完成偏移验证。
     *
     * @param apiVersions supported API versions
     * @param tp topic partition to validate
     * @param leaderAndEpoch leader epoch of the topic partition
     * @return true if we enter the offset validation state
     */
    public synchronized boolean maybeValidatePositionForCurrentLeader(ApiVersions apiVersions,
                                                                      TopicPartition tp,
                                                                      Metadata.LeaderAndEpoch leaderAndEpoch) {
        if (leaderAndEpoch.leader.isPresent()) {
            NodeApiVersions nodeApiVersions = apiVersions.get(leaderAndEpoch.leader.get().idString());
            if (nodeApiVersions == null || hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
            } else {
                // 如果代理不支持更新版本的OffsetsForLeaderEpoch，我们跳过验证
                assignedState(tp).updatePositionLeaderNoValidation(leaderAndEpoch);
                return false;
            }
        } else {
            return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
        }
    }

    /**
     * 尝试使用从OffsetForLeaderEpoch请求返回的结束偏移量完成验证。
     * @return Log truncation details if detected and no reset policy is defined.
     */
    public synchronized Optional<LogTruncation> maybeCompleteValidation(TopicPartition tp,
                                                                        FetchPosition requestPosition,
                                                                        EpochEndOffset epochEndOffset) {
        TopicPartitionState state = assignedStateOrNull(tp);
        if (state == null) {
            log.debug("Skipping completed validation for partition {} which is not currently assigned.", tp);
        } else if (!state.awaitingValidation()) {
            log.debug("Skipping completed validation for partition {} which is no longer expecting validation.", tp);
        } else {
            SubscriptionState.FetchPosition currentPosition = state.position;
            if (!currentPosition.equals(requestPosition)) {
                log.debug("Skipping completed validation for partition {} since the current position {} " +
                          "no longer matches the position {} when the request was sent",
                          tp, currentPosition, requestPosition);
            } else if (epochEndOffset.endOffset() == UNDEFINED_EPOCH_OFFSET ||
                        epochEndOffset.leaderEpoch() == UNDEFINED_EPOCH) {
                if (hasDefaultOffsetResetPolicy()) {
                    log.info("Truncation detected for partition {} at offset {}, resetting offset",
                             tp, currentPosition);
                    requestOffsetReset(tp);
                } else {
                    log.warn("Truncation detected for partition {} at offset {}, but no reset policy is set",
                             tp, currentPosition);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.empty()));
                }
            } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                if (hasDefaultOffsetResetPolicy()) {
                    SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                    log.info("Truncation detected for partition {} at offset {}, resetting offset to " +
                             "the first offset known to diverge {}", tp, currentPosition, newPosition);
                    state.seekValidated(newPosition);
                } else {
                    OffsetAndMetadata divergentOffset = new OffsetAndMetadata(epochEndOffset.endOffset(),
                        Optional.of(epochEndOffset.leaderEpoch()), null);
                    log.warn("Truncation detected for partition {} at offset {} (the end offset from the " +
                             "broker is {}), but no reset policy is set", tp, currentPosition, divergentOffset);
                    return Optional.of(new LogTruncation(tp, requestPosition, Optional.of(divergentOffset)));
                }
            } else {
                state.completeValidation();
            }
        }

        return Optional.empty();
    }

    public synchronized boolean awaitingValidation(TopicPartition tp) {
        return assignedState(tp).awaitingValidation();
    }

    public synchronized void completeValidation(TopicPartition tp) {
        assignedState(tp).completeValidation();
    }

    public synchronized FetchPosition validPosition(TopicPartition tp) {
        return assignedState(tp).validPosition();
    }

    public synchronized FetchPosition position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public synchronized Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (topicPartitionState.position == null) {
            return null;
        } else if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
        } else {
            return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
        }
    }

    public synchronized Long partitionEndOffset(TopicPartition tp, IsolationLevel isolationLevel) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return topicPartitionState.lastStableOffset;
        } else {
            return topicPartitionState.highWatermark;
        }
    }

    public synchronized void requestPartitionEndOffset(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        topicPartitionState.requestEndOffset();
    }

    public synchronized boolean partitionEndOffsetRequested(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.endOffsetRequested();
    }

    synchronized Long partitionLead(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
    }

    synchronized void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark(highWatermark);
    }

    synchronized void updateLogStartOffset(TopicPartition tp, long logStartOffset) {
        assignedState(tp).logStartOffset(logStartOffset);
    }

    synchronized void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        assignedState(tp).lastStableOffset(lastStableOffset);
    }

    /**
     * 设置带有租约超时的首选读副本。在此时间之后，副本将不再有效
     * {@link #preferredReadReplica(TopicPartition, long)} 将返回空结果。
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     */
    public synchronized void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, LongSupplier timeMs) {
        assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
    }

    /**
     * 获取首选读副本
     *
     * @param tp The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not expired.
     */
    public synchronized Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
        if (topicPartitionState == null) {
            return Optional.empty();
        } else {
            return topicPartitionState.preferredReadReplica(timeMs);
        }
    }

    /**
     * 取消优选读副本的设置。这将导致取回器返回到leader进行取回。
     *
     * @param tp The topic partition
     * @return 如果设置了首选读副本，则为True，否则为false。
     */
    public synchronized Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        return assignedState(tp).clearPreferredReadReplica();
    }

    public synchronized Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        assignment.forEach((topicPartition, partitionState) -> {
            if (partitionState.hasValidPosition())
                allConsumed.put(topicPartition, new OffsetAndMetadata(partitionState.position.offset,
                        partitionState.position.offsetEpoch, ""));
        });
        return allConsumed;
    }

    public synchronized void requestOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).reset(offsetResetStrategy);
    }

    public synchronized void requestOffsetReset(Collection<TopicPartition> partitions, OffsetResetStrategy offsetResetStrategy) {
        partitions.forEach(tp -> {
            log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp);
            assignedState(tp).reset(offsetResetStrategy);
        });
    }

    public void requestOffsetReset(TopicPartition partition) {
        requestOffsetReset(partition, defaultResetStrategy);
    }

    synchronized void setNextAllowedRetry(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        for (TopicPartition partition : partitions) {
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
        }
    }

    boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public synchronized boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public synchronized OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy();
    }

    public synchronized boolean hasAllFetchPositions() {
        // 由于这是在获取的热路径中，所以我们使用java.util.stream API来代替它
        Iterator<TopicPartitionState> it = assignment.stateIterator();
        while (it.hasNext()) {
            if (!it.next().hasValidPosition()) {
                return false;
            }
        }
        return true;
    }

    public synchronized Set<TopicPartition> initializingPartitions() {
        return collectPartitions(state -> state.fetchState.equals(FetchStates.INITIALIZING));
    }

    private Set<TopicPartition> collectPartitions(Predicate<TopicPartitionState> filter) {
        Set<TopicPartition> result = new HashSet<>();
        assignment.forEach((topicPartition, topicPartitionState) -> {
            if (filter.test(topicPartitionState)) {
                result.add(topicPartition);
            }
        });
        return result;
    }


    public synchronized void resetInitializingPositions() {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        assignment.forEach((tp, partitionState) -> {
            if (partitionState.fetchState.equals(FetchStates.INITIALIZING)) {
                if (defaultResetStrategy == OffsetResetStrategy.NONE)
                    partitionsWithNoOffsets.add(tp);
                else
                    requestOffsetReset(tp);
            }
        });

        if (!partitionsWithNoOffsets.isEmpty())
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
    }

    public synchronized Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs));
    }

    public synchronized Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs));
    }

    public synchronized boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public synchronized boolean isPaused(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isPaused();
    }

    synchronized boolean isFetchable(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.isFetchable();
    }

    public synchronized boolean hasValidPosition(TopicPartition tp) {
        TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
        return assignedOrNull != null && assignedOrNull.hasValidPosition();
    }

    public synchronized void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public synchronized void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    synchronized void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions) {
            // 当请求失败时，分配可能不再包含此分区，在这种情况下，我们将忽略。
            final TopicPartitionState state = assignedStateOrNull(partition);
            if (state != null)
                state.requestFailed(nextRetryTimeMs);
        }
    }

    synchronized void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public synchronized ConsumerRebalanceListener rebalanceListener() {
        return rebalanceListener;
    }

    private static class TopicPartitionState {

        private FetchState fetchState;
        private FetchPosition position; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextRetryTimeMs;
        private Integer preferredReadReplica;
        private Long preferredReadReplicaExpireTimeMs;
        private boolean endOffsetRequested;
        
        TopicPartitionState() {
            this.paused = false;
            this.endOffsetRequested = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
        }

        public boolean endOffsetRequested() {
            return endOffsetRequested;
        }

        public void requestEndOffset() {
            endOffsetRequested = true;
        }

        private void transitionState(FetchState newState, Runnable runIfTransitioned) {
            FetchState nextState = this.fetchState.transitionTo(newState);
            if (nextState.equals(newState)) {
                this.fetchState = nextState;
                runIfTransitioned.run();
                if (this.position == null && nextState.requiresPosition()) {
                    throw new IllegalStateException("Transitioned subscription state to " + nextState + ", but position is null");
                } else if (!nextState.requiresPosition()) {
                    this.position = null;
                }
            }
        }

        private Optional<Integer> preferredReadReplica(long timeMs) {
            if (preferredReadReplicaExpireTimeMs != null && timeMs > preferredReadReplicaExpireTimeMs) {
                preferredReadReplica = null;
                return Optional.empty();
            } else {
                return Optional.ofNullable(preferredReadReplica);
            }
        }

        private void updatePreferredReadReplica(int preferredReadReplica, LongSupplier timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.getAsLong();
            }
        }

        private Optional<Integer> clearPreferredReadReplica() {
            if (preferredReadReplica != null) {
                int removedReplicaId = this.preferredReadReplica;
                this.preferredReadReplica = null;
                this.preferredReadReplicaExpireTimeMs = null;
                return Optional.of(removedReplicaId);
            } else {
                return Optional.empty();
            }
        }

        private void reset(OffsetResetStrategy strategy) {
            transitionState(FetchStates.AWAIT_RESET, () -> {
                this.resetStrategy = strategy;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * 检查该职位是否存在，是否需要验证。如果是，则进入AWAIT_VALIDATION状态。此方法还将使用当前的leader和epoch更新位置。
         *
         * @param currentLeaderAndEpoch leader and epoch to compare the offset with
         * @return true if the position is now awaiting validation
         */
        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }

            if (!currentLeaderAndEpoch.leader.isPresent()) {
                return false;
            }

            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
            return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * 对于旧版本的API，我们不能执行偏移量验证，所以我们直接转换到抓取
         */
        private void updatePositionLeaderNoValidation(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (position != null) {
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                    this.nextRetryTimeMs = null;
                });
            }
        }

        private void validatePosition(FetchPosition position) {
            if (position.offsetEpoch.isPresent() && position.currentLeader.epoch.isPresent()) {
                transitionState(FetchStates.AWAIT_VALIDATION, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            } else {
                // If we have no epoch information for the current position, then we can skip validation
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * 清除等待验证状态并输入抓取。
         */
        private void completeValidation() {
            if (hasPosition()) {
                transitionState(FetchStates.FETCHING, () -> this.nextRetryTimeMs = null);
            }
        }

        private boolean awaitingValidation() {
            return fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        private boolean awaitingReset() {
            return fetchState.equals(FetchStates.AWAIT_RESET);
        }

        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private void requestFailed(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private boolean hasValidPosition() {
            return fetchState.hasValidPosition();
        }

        private boolean hasPosition() {
            return position != null;
        }

        private boolean isPaused() {
            return paused;
        }

        private void seekValidated(FetchPosition position) {
            transitionState(FetchStates.FETCHING, () -> {
                this.position = position;
                this.resetStrategy = null;
                this.nextRetryTimeMs = null;
            });
        }

        private void seekUnvalidated(FetchPosition fetchPosition) {
            seekValidated(fetchPosition);
            validatePosition(fetchPosition);
        }

        private void position(FetchPosition position) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = position;
        }

        private FetchPosition validPosition() {
            if (hasValidPosition()) {
                return position;
            } else {
                return null;
            }
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
            this.endOffsetRequested = false;
        }

        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
            this.endOffsetRequested = false;
        }

        private OffsetResetStrategy resetStrategy() {
            return resetStrategy;
        }
    }

    /**
     * 分区的获取状态。该类用于确定有效的状态转换并公开当前获取状态的一些行为。实际状态变量存储在 {@link TopicPartitionState}.
     */
    interface FetchState {
        default FetchState transitionTo(FetchState newState) {
            if (validTransitions().contains(newState)) {
                return newState;
            } else {
                return this;
            }
        }

        /**
         * Return the valid states which this state can transition to
         */
        Collection<FetchState> validTransitions();

        /**
         * Test if this state requires a position to be set
         */
        boolean requiresPosition();

        /**
         * Test if this state is considered to have a valid position which can be used for fetching
         */
        boolean hasValidPosition();
    }

    /**
     * 所有可能获取状态的枚举。返回的值对状态转换进行编码
     * {@link FetchState#validTransitions}.
     */
    enum FetchStates implements FetchState {
        INITIALIZING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        FETCHING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return true;
            }
        },

        AWAIT_RESET() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        AWAIT_VALIDATION() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        }
    }

    /**
     * 表示分区订阅的位置。这包括来自FetchResponse的批处理中最后一条记录的偏移量和epoch。它还包括使用批处理时的leader纪元。
     */
    public static class FetchPosition {
        public final long offset;
        final Optional<Integer> offsetEpoch;
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        }

        public FetchPosition(long offset, Optional<Integer> offsetEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.offsetEpoch = Objects.requireNonNull(offsetEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchPosition that = (FetchPosition) o;
            return offset == that.offset &&
                    offsetEpoch.equals(that.offsetEpoch) &&
                    currentLeader.equals(that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, offsetEpoch, currentLeader);
        }

        @Override
        public String toString() {
            return "FetchPosition{" +
                    "offset=" + offset +
                    ", offsetEpoch=" + offsetEpoch +
                    ", currentLeader=" + currentLeader +
                    '}';
        }
    }

    public static class LogTruncation {
        public final TopicPartition topicPartition;
        public final FetchPosition fetchPosition;
        public final Optional<OffsetAndMetadata> divergentOffsetOpt;

        public LogTruncation(TopicPartition topicPartition,
                             FetchPosition fetchPosition,
                             Optional<OffsetAndMetadata> divergentOffsetOpt) {
            this.topicPartition = topicPartition;
            this.fetchPosition = fetchPosition;
            this.divergentOffsetOpt = divergentOffsetOpt;
        }

        @Override
        public String toString() {
            StringBuilder bldr = new StringBuilder()
                .append("(partition=")
                .append(topicPartition)
                .append(", fetchOffset=")
                .append(fetchPosition.offset)
                .append(", fetchEpoch=")
                .append(fetchPosition.offsetEpoch);

            if (divergentOffsetOpt.isPresent()) {
                OffsetAndMetadata divergentOffset = divergentOffsetOpt.get();
                bldr.append(", divergentOffset=")
                    .append(divergentOffset.offset())
                    .append(", divergentEpoch=")
                    .append(divergentOffset.leaderEpoch());
            } else {
                bldr.append(", divergentOffset=unknown")
                    .append(", divergentEpoch=unknown");
            }

            return bldr.append(")").toString();

        }
    }
}
