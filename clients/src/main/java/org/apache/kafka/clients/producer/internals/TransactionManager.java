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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 维护事务状态的类。也保持必要的状态，以确保幂等生产。
 */
public class TransactionManager {
    private static final int NO_INFLIGHT_REQUEST_CORRELATION_ID = -1;
    private static final int NO_LAST_ACKED_SEQUENCE_NUMBER = -1;

    private final Logger log;
    private final String transactionalId;
    private final int transactionTimeoutMs;
    private final ApiVersions apiVersions;

    private static class TopicPartitionBookkeeper {

        private final Map<TopicPartition, TopicPartitionEntry> topicPartitions = new HashMap<>();

        private TopicPartitionEntry getPartition(TopicPartition topicPartition) {
            TopicPartitionEntry ent = topicPartitions.get(topicPartition);
            if (ent == null)
                throw new IllegalStateException("Trying to get the sequence number for " + topicPartition +
                        ", but the sequence number was never set for this partition.");
            return ent;
        }

        private TopicPartitionEntry getOrCreatePartition(TopicPartition topicPartition) {
            TopicPartitionEntry ent = topicPartitions.get(topicPartition);
            if (ent == null) {
                ent = new TopicPartitionEntry();
                topicPartitions.put(topicPartition, ent);
            }
            return ent;
        }

        private void addPartition(TopicPartition topicPartition) {
            this.topicPartitions.putIfAbsent(topicPartition, new TopicPartitionEntry());
        }

        private boolean contains(TopicPartition topicPartition) {
            return topicPartitions.containsKey(topicPartition);
        }

        private void reset() {
            topicPartitions.clear();
        }

        private OptionalLong lastAckedOffset(TopicPartition topicPartition) {
            TopicPartitionEntry entry = topicPartitions.get(topicPartition);
            if (entry != null && entry.lastAckedOffset != ProduceResponse.INVALID_OFFSET)
                return OptionalLong.of(entry.lastAckedOffset);
            else
                return OptionalLong.empty();
        }

        private OptionalInt lastAckedSequence(TopicPartition topicPartition) {
            TopicPartitionEntry entry = topicPartitions.get(topicPartition);
            if (entry != null && entry.lastAckedSequence != NO_LAST_ACKED_SEQUENCE_NUMBER)
                return OptionalInt.of(entry.lastAckedSequence);
            else
                return OptionalInt.empty();
        }

        private void startSequencesAtBeginning(TopicPartition topicPartition, ProducerIdAndEpoch newProducerIdAndEpoch) {
            final PrimitiveRef.IntRef sequence = PrimitiveRef.ofInt(0);
            TopicPartitionEntry topicPartitionEntry = getPartition(topicPartition);
            topicPartitionEntry.resetSequenceNumbers(inFlightBatch -> {
                inFlightBatch.resetProducerState(newProducerIdAndEpoch, sequence.value, inFlightBatch.isTransactional());
                sequence.value += inFlightBatch.recordCount;
            });
            topicPartitionEntry.producerIdAndEpoch = newProducerIdAndEpoch;
            topicPartitionEntry.nextSequence = sequence.value;
            topicPartitionEntry.lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
        }
    }

    private static class TopicPartitionEntry {

        // The producer id/epoch being used for a given partition.
        private ProducerIdAndEpoch producerIdAndEpoch;

        // The base sequence of the next batch bound for a given partition.
        private int nextSequence;

        // The sequence number of the last record of the last ack'd batch from the given partition. When there are no
        // in flight requests for a partition, the lastAckedSequence(topicPartition) == nextSequence(topicPartition) - 1.
        private int lastAckedSequence;

        // Keep track of the in flight batches bound for a partition, ordered by sequence. This helps us to ensure that
        // we continue to order batches by the sequence numbers even when the responses come back out of order during
        // leader failover. We add a batch to the queue when it is drained, and remove it when the batch completes
        // (either successfully or through a fatal failure).
        private SortedSet<ProducerBatch> inflightBatchesBySequence;

        // We keep track of the last acknowledged offset on a per partition basis in order to disambiguate UnknownProducer
        // responses which are due to the retention period elapsing, and those which are due to actual lost data.
        private long lastAckedOffset;

        private static final Comparator<ProducerBatch> PRODUCER_BATCH_COMPARATOR = (b1, b2) -> {
            if (b1.baseSequence() < b2.baseSequence()) return -1;
            else if (b1.baseSequence() > b2.baseSequence()) return 1;
            else return Integer.compare(b1.hashCode(), b2.hashCode());
        };

        TopicPartitionEntry() {
            this.producerIdAndEpoch = ProducerIdAndEpoch.NONE;
            this.nextSequence = 0;
            this.lastAckedSequence = NO_LAST_ACKED_SEQUENCE_NUMBER;
            this.lastAckedOffset = ProduceResponse.INVALID_OFFSET;
            this.inflightBatchesBySequence = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
        }

        void resetSequenceNumbers(Consumer<ProducerBatch> resetSequence) {
            TreeSet<ProducerBatch> newInflights = new TreeSet<>(PRODUCER_BATCH_COMPARATOR);
            for (ProducerBatch inflightBatch : inflightBatchesBySequence) {
                resetSequence.accept(inflightBatch);
                newInflights.add(inflightBatch);
            }
            inflightBatchesBySequence = newInflights;
        }
    }

    private final TopicPartitionBookkeeper topicPartitionBookkeeper;

    private final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits;

    // If a batch bound for a partition expired locally after being sent at least once, the partition has is considered
    // to have an unresolved state. We keep track fo such partitions here, and cannot assign any more sequence numbers
    // for this partition until the unresolved state gets cleared. This may happen if other inflight batches returned
    // successfully (indicating that the expired batch actually made it to the broker). If we don't get any successful
    // responses for the partition once the inflight request count falls to zero, we reset the producer id and
    // consequently clear this data structure as well.
    // The value of the map is the sequence number of the batch following the expired one, computed by adding its
    // record count to its sequence number. This is used to tell if a subsequent batch is the one immediately following
    // the expired one.
    private final Map<TopicPartition, Integer> partitionsWithUnresolvedSequences;

    // The partitions that have received an error that triggers an epoch bump. When the epoch is bumped, these
    // partitions will have the sequences of their in-flight batches rewritten
    private final Set<TopicPartition> partitionsToRewriteSequences;

    private final PriorityQueue<TxnRequestHandler> pendingRequests;
    private final Set<TopicPartition> newPartitionsInTransaction;
    private final Set<TopicPartition> pendingPartitionsInTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private TransactionalRequestResult pendingResult;

    // This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
    // For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
    // error for the first AddPartitionsRequest in a transaction.
    private final long retryBackoffMs;

    // The retryBackoff is overridden to the following value if the first AddPartitions receives a
    // CONCURRENT_TRANSACTIONS error.
    private static final long ADD_PARTITIONS_RETRY_BACKOFF_MS = 20L;

    private int inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    private Node transactionCoordinator;
    private Node consumerGroupCoordinator;
    private boolean coordinatorSupportsBumpingEpoch;

    private volatile State currentState = State.UNINITIALIZED;
    private volatile RuntimeException lastError = null;
    private volatile ProducerIdAndEpoch producerIdAndEpoch;
    private volatile boolean transactionStarted = false;
    private volatile boolean epochBumpRequired = false;

    private enum State {
        UNINITIALIZED,
        INITIALIZING,
        READY,
        IN_TRANSACTION,
        COMMITTING_TRANSACTION,
        ABORTING_TRANSACTION,
        ABORTABLE_ERROR,
        FATAL_ERROR;

        private boolean isTransitionValid(State source, State target) {
            switch (target) {
                case UNINITIALIZED:
                    return source == READY;
                case INITIALIZING:
                    return source == UNINITIALIZED || source == ABORTING_TRANSACTION;
                case READY:
                    return source == INITIALIZING || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
                case IN_TRANSACTION:
                    return source == READY;
                case COMMITTING_TRANSACTION:
                    return source == IN_TRANSACTION;
                case ABORTING_TRANSACTION:
                    return source == IN_TRANSACTION || source == ABORTABLE_ERROR;
                case ABORTABLE_ERROR:
                    return source == IN_TRANSACTION || source == COMMITTING_TRANSACTION || source == ABORTABLE_ERROR;
                case FATAL_ERROR:
                default:
                    // We can transition to FATAL_ERROR unconditionally.
                    // FATAL_ERROR is never a valid starting state for any transition. So the only option is to close the
                    // producer or do purely non transactional requests.
                    return true;
            }
        }
    }

    // We use the priority to determine the order in which requests need to be sent out. For instance, if we have
    // a pending FindCoordinator request, that must always go first. Next, If we need a producer id, that must go second.
    // The endTxn request must always go last, unless we are bumping the epoch (a special case of InitProducerId) as
    // part of ending the transaction.
    private enum Priority {
        FIND_COORDINATOR(0),
        INIT_PRODUCER_ID(1),
        ADD_PARTITIONS_OR_OFFSETS(2),
        END_TXN(3),
        EPOCH_BUMP(4);

        final int priority;

        Priority(int priority) {
            this.priority = priority;
        }
    }

    public TransactionManager(final LogContext logContext,
                              final String transactionalId,
                              final int transactionTimeoutMs,
                              final long retryBackoffMs,
                              final ApiVersions apiVersions) {
        this.producerIdAndEpoch = ProducerIdAndEpoch.NONE;
        this.transactionalId = transactionalId;
        this.log = logContext.logger(TransactionManager.class);
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsInTransaction = new HashSet<>();
        this.pendingPartitionsInTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        this.pendingRequests = new PriorityQueue<>(10, Comparator.comparingInt(o -> o.priority().priority));
        this.pendingTxnOffsetCommits = new HashMap<>();
        this.partitionsWithUnresolvedSequences = new HashMap<>();
        this.partitionsToRewriteSequences = new HashSet<>();
        this.retryBackoffMs = retryBackoffMs;
        this.topicPartitionBookkeeper = new TopicPartitionBookkeeper();
        this.apiVersions = apiVersions;
    }

    public synchronized TransactionalRequestResult initializeTransactions() {
        return initializeTransactions(ProducerIdAndEpoch.NONE);
    }

    synchronized TransactionalRequestResult initializeTransactions(ProducerIdAndEpoch producerIdAndEpoch) {
        boolean isEpochBump = producerIdAndEpoch != ProducerIdAndEpoch.NONE;
        return handleCachedTransactionRequestResult(() -> {
            // 如果这是一个epoch bump，我们将转换状态作为处理EndTxnRequest的一部分
            if (!isEpochBump) {
                transitionTo(State.INITIALIZING);
                log.info("Invoking InitProducerId for the first time in order to acquire a producer ID");
            } else {
                log.info("Invoking InitProducerId with current producer ID and epoch {} in order to bump the epoch", producerIdAndEpoch);
            }
            InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                    .setTransactionalId(transactionalId)
                    .setTransactionTimeoutMs(transactionTimeoutMs)
                    .setProducerId(producerIdAndEpoch.producerId)
                    .setProducerEpoch(producerIdAndEpoch.epoch);
            InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData),
                    isEpochBump);
            enqueueRequest(handler);
            return handler.result;
        }, State.INITIALIZING);
    }

    public synchronized void beginTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.IN_TRANSACTION);
    }

    public synchronized TransactionalRequestResult beginCommit() {
        return handleCachedTransactionRequestResult(() -> {
            maybeFailWithError();
            transitionTo(State.COMMITTING_TRANSACTION);
            return beginCompletingTransaction(TransactionResult.COMMIT);
        }, State.COMMITTING_TRANSACTION);
    }

    public synchronized TransactionalRequestResult beginAbort() {
        return handleCachedTransactionRequestResult(() -> {
            if (currentState != State.ABORTABLE_ERROR)
                maybeFailWithError();
            transitionTo(State.ABORTING_TRANSACTION);

            // 我们正在中止事务，因此不需要添加新的分区
            newPartitionsInTransaction.clear();
            return beginCompletingTransaction(TransactionResult.ABORT);
        }, State.ABORTING_TRANSACTION);
    }

    private TransactionalRequestResult beginCompletingTransaction(TransactionResult transactionResult) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        // 如果错误是INVALID_PRODUCER_ID_MAPPING错误，服务器将不接受EndTxnRequest，因此直接跳到InitProducerId。否则，我们必须首先中止事务，
        // 因为如果我们直接调用InitProducerId，生产者将被隔离。
        if (!(lastError instanceof InvalidPidMappingException)) {
            EndTxnRequest.Builder builder = new EndTxnRequest.Builder(
                    new EndTxnRequestData()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerIdAndEpoch.producerId)
                            .setProducerEpoch(producerIdAndEpoch.epoch)
                            .setCommitted(transactionResult.id));

            EndTxnHandler handler = new EndTxnHandler(builder);
            enqueueRequest(handler);
            if (!epochBumpRequired) {
                return handler.result;
            }
        }

        return initializeTransactions(this.producerIdAndEpoch);
    }

    public synchronized TransactionalRequestResult sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                            final ConsumerGroupMetadata groupMetadata) {
        ensureTransactional();
        maybeFailWithError();
        if (currentState != State.IN_TRANSACTION)
            throw new KafkaException("Cannot send offsets to transaction either because the producer is not in an " +
                    "active transaction");

        log.debug("Begin adding offsets {} for consumer group {} to transaction", offsets, groupMetadata);
        AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(
            new AddOffsetsToTxnRequestData()
                .setTransactionalId(transactionalId)
                .setProducerId(producerIdAndEpoch.producerId)
                .setProducerEpoch(producerIdAndEpoch.epoch)
                .setGroupId(groupMetadata.groupId())
        );
        AddOffsetsToTxnHandler handler = new AddOffsetsToTxnHandler(builder, offsets, groupMetadata);

        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized void maybeAddPartitionToTransaction(TopicPartition topicPartition) {
        if (isPartitionAdded(topicPartition) || isPartitionPendingAdd(topicPartition))
            return;

        log.debug("Begin adding new partition {} to transaction", topicPartition);
        topicPartitionBookkeeper.addPartition(topicPartition);
        newPartitionsInTransaction.add(topicPartition);
    }

    RuntimeException lastError() {
        return lastError;
    }

    public synchronized void failIfNotReadyForSend() {
        if (hasError())
            throw new KafkaException("Cannot perform send because at least one previous transactional or " +
                    "idempotent request has failed with errors.", lastError);

        if (isTransactional()) {
            if (!hasProducerId())
                throw new IllegalStateException("Cannot perform a 'send' before completing a call to initTransactions " +
                        "when transactions are enabled.");

            if (currentState != State.IN_TRANSACTION)
                throw new IllegalStateException("Cannot call send in state " + currentState);
        }
    }

    synchronized boolean isSendToPartitionAllowed(TopicPartition tp) {
        if (hasFatalError())
            return false;
        return !isTransactional() || partitionsInTransaction.contains(tp);
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean hasProducerId() {
        return producerIdAndEpoch.isValid();
    }

    public boolean isTransactional() {
        return transactionalId != null;
    }

    synchronized boolean hasPartitionsToAdd() {
        return !newPartitionsInTransaction.isEmpty() || !pendingPartitionsInTransaction.isEmpty();
    }

    synchronized boolean isCompleting() {
        return currentState == State.COMMITTING_TRANSACTION || currentState == State.ABORTING_TRANSACTION;
    }

    synchronized boolean hasError() {
        return currentState == State.ABORTABLE_ERROR || currentState == State.FATAL_ERROR;
    }

    synchronized boolean isAborting() {
        return currentState == State.ABORTING_TRANSACTION;
    }

    synchronized void transitionToAbortableError(RuntimeException exception) {
        if (currentState == State.ABORTING_TRANSACTION) {
            log.debug("Skipping transition to abortable error state since the transaction is already being " +
                    "aborted. Underlying exception: ", exception);
            return;
        }

        log.info("Transiting to abortable error state due to {}", exception.toString());
        transitionTo(State.ABORTABLE_ERROR, exception);
    }

    synchronized void transitionToFatalError(RuntimeException exception) {
        log.info("Transiting to fatal error state due to {}", exception.toString());
        transitionTo(State.FATAL_ERROR, exception);

        if (pendingResult != null) {
            pendingResult.fail(exception);
        }
    }

    // visible for testing
    synchronized boolean isPartitionAdded(TopicPartition partition) {
        return partitionsInTransaction.contains(partition);
    }

    // visible for testing
    synchronized boolean isPartitionPendingAdd(TopicPartition partition) {
        return newPartitionsInTransaction.contains(partition) || pendingPartitionsInTransaction.contains(partition);
    }

    /**
     * 在不阻塞的情况下获取当前生产者id和epoch。调用者必须使用{@link ProducerIdAndEpoch#isValid()}来验证结果是否有效。
     *
     * @return the current ProducerIdAndEpoch.
     */
    ProducerIdAndEpoch producerIdAndEpoch() {
        return producerIdAndEpoch;
    }

    synchronized public void maybeUpdateProducerIdAndEpoch(TopicPartition topicPartition) {
        if (hasStaleProducerIdAndEpoch(topicPartition) && !hasInflightBatches(topicPartition)) {
            // 如果批处理在不同的ID或epoch上(由于epoch碰撞)，并且所有正在运行的批处理都已完成，则重置分区序列，以便下一个批处理(具有新epoch)从0开始
            topicPartitionBookkeeper.startSequencesAtBeginning(topicPartition, this.producerIdAndEpoch);
            log.debug("ProducerId of partition {} set to {} with epoch {}. Reinitialize sequence at beginning.",
                      topicPartition, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        }
    }

    /**
     * 自动设置生产者id和epoch。
     */
    private void setProducerIdAndEpoch(ProducerIdAndEpoch producerIdAndEpoch) {
        log.info("ProducerId set to {} with epoch {}", producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        this.producerIdAndEpoch = producerIdAndEpoch;
    }

    /**
     * 这个方法重置生产者ID和epoch，并将状态设置为UNINITIALIZED，这将触发一个新的InitProducerId请求。
     * 此方法仅在生产者epoch耗尽时调用;我们将碰撞epoch。
     */
    private void resetIdempotentProducerId() {
        if (isTransactional())
            throw new IllegalStateException("Cannot reset producer state for a transactional producer. " +
                    "You must either abort the ongoing transaction or reinitialize the transactional producer instead");
        log.debug("Resetting idempotent producer ID. ID and epoch before reset are {}", this.producerIdAndEpoch);
        setProducerIdAndEpoch(ProducerIdAndEpoch.NONE);
        transitionTo(State.UNINITIALIZED);
    }

    private void resetSequenceForPartition(TopicPartition topicPartition) {
        topicPartitionBookkeeper.topicPartitions.remove(topicPartition);
        this.partitionsWithUnresolvedSequences.remove(topicPartition);
    }

    private void resetSequenceNumbers() {
        topicPartitionBookkeeper.reset();
        this.partitionsWithUnresolvedSequences.clear();
    }

    synchronized void requestEpochBumpForPartition(TopicPartition tp) {
        epochBumpRequired = true;
        this.partitionsToRewriteSequences.add(tp);
    }

    private void bumpIdempotentProducerEpoch() {
        if (this.producerIdAndEpoch.epoch == Short.MAX_VALUE) {
            resetIdempotentProducerId();
        } else {
            setProducerIdAndEpoch(new ProducerIdAndEpoch(this.producerIdAndEpoch.producerId, (short) (this.producerIdAndEpoch.epoch + 1)));
            log.debug("Incremented producer epoch, current producer ID and epoch are now {}", this.producerIdAndEpoch);
        }

        // 当epoch发生碰撞时，重写触发epoch碰撞的分区的所有动态序列
        for (TopicPartition topicPartition : this.partitionsToRewriteSequences) {
            this.topicPartitionBookkeeper.startSequencesAtBeginning(topicPartition, this.producerIdAndEpoch);
            this.partitionsWithUnresolvedSequences.remove(topicPartition);
        }
        this.partitionsToRewriteSequences.clear();

        epochBumpRequired = false;
    }

    synchronized void bumpIdempotentEpochAndResetIdIfNeeded() {
        if (!isTransactional()) {
            if (epochBumpRequired) {
                bumpIdempotentProducerEpoch();
            }
            if (currentState != State.INITIALIZING && !hasProducerId()) {
                transitionTo(State.INITIALIZING);
                InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                        .setTransactionalId(null)
                        .setTransactionTimeoutMs(Integer.MAX_VALUE);
                InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData), false);
                enqueueRequest(handler);
            }
        }
    }

    /**
     * 返回下一个要写入给定TopicPartition的序列号。
     */
    synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        return topicPartitionBookkeeper.getOrCreatePartition(topicPartition).nextSequence;
    }

    /**
     * 返回给定TopicPartition的当前生产者id/epoch。
     */
    synchronized ProducerIdAndEpoch producerIdAndEpoch(TopicPartition topicPartition) {
        return topicPartitionBookkeeper.getOrCreatePartition(topicPartition).producerIdAndEpoch;
    }

    synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        Integer currentSequence = sequenceNumber(topicPartition);

        currentSequence = DefaultRecordBatch.incrementSequence(currentSequence, increment);
        topicPartitionBookkeeper.getPartition(topicPartition).nextSequence = currentSequence;
    }

    synchronized void addInFlightBatch(ProducerBatch batch) {
        if (!batch.hasSequence())
            throw new IllegalStateException("Can't track batch for partition " + batch.topicPartition + " when sequence is not set.");
        topicPartitionBookkeeper.getPartition(batch.topicPartition).inflightBatchesBySequence.add(batch);
    }

    /**
     * 返回给定分区的第一个飞行序列。这是具有最低序号的飞行批次的基本序列
     * @return 如果事务管理器正在跟踪此分区的迁移请求，则为最低迁移序列。如果这个分区没有被跟踪的请求，这个方法将返回RecordBatch.NO_SEQUENCE。
     */
    synchronized int firstInFlightSequence(TopicPartition topicPartition) {
        if (!hasInflightBatches(topicPartition))
            return RecordBatch.NO_SEQUENCE;

        SortedSet<ProducerBatch> inflightBatches = topicPartitionBookkeeper.getPartition(topicPartition).inflightBatchesBySequence;
        if (inflightBatches.isEmpty())
            return RecordBatch.NO_SEQUENCE;
        else
            return inflightBatches.first().baseSequence();
    }

    synchronized ProducerBatch nextBatchBySequence(TopicPartition topicPartition) {
        SortedSet<ProducerBatch> queue = topicPartitionBookkeeper.getPartition(topicPartition).inflightBatchesBySequence;
        return queue.isEmpty() ? null : queue.first();
    }

    synchronized void removeInFlightBatch(ProducerBatch batch) {
        if (hasInflightBatches(batch.topicPartition)) {
            topicPartitionBookkeeper.getPartition(batch.topicPartition).inflightBatchesBySequence.remove(batch);
        }
    }

    private int maybeUpdateLastAckedSequence(TopicPartition topicPartition, int sequence) {
        int lastAckedSequence = lastAckedSequence(topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER);
        if (sequence > lastAckedSequence) {
            topicPartitionBookkeeper.getPartition(topicPartition).lastAckedSequence = sequence;
            return sequence;
        }

        return lastAckedSequence;
    }

    synchronized OptionalInt lastAckedSequence(TopicPartition topicPartition) {
        return topicPartitionBookkeeper.lastAckedSequence(topicPartition);
    }

    synchronized OptionalLong lastAckedOffset(TopicPartition topicPartition) {
        return topicPartitionBookkeeper.lastAckedOffset(topicPartition);
    }

    private void updateLastAckedOffset(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        if (response.baseOffset == ProduceResponse.INVALID_OFFSET)
            return;
        long lastOffset = response.baseOffset + batch.recordCount - 1;
        OptionalLong lastAckedOffset = lastAckedOffset(batch.topicPartition);
        // 当请求重新排队并为此获得有效响应时，TransactionManager可能会被重置。只有当生产者是幂等的(不是事务性的)时才会发生这种情况，
        // 在这种情况下，不会有关于它的跟踪账簿条目，所以我们必须插入一个。
        if (!lastAckedOffset.isPresent() && !isTransactional()) {
            topicPartitionBookkeeper.addPartition(batch.topicPartition);
        }
        if (lastOffset > lastAckedOffset.orElse(ProduceResponse.INVALID_OFFSET)) {
            topicPartitionBookkeeper.getPartition(batch.topicPartition).lastAckedOffset = lastOffset;
        } else {
            log.trace("Partition {} keeps lastOffset at {}", batch.topicPartition, lastOffset);
        }
    }

    public synchronized void handleCompletedBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        int lastAckedSequence = maybeUpdateLastAckedSequence(batch.topicPartition, batch.lastSequence());
        log.debug("ProducerId: {}; Set last ack'd sequence number for topic-partition {} to {}",
                batch.producerId(),
                batch.topicPartition,
                lastAckedSequence);

        updateLastAckedOffset(response, batch);
        removeInFlightBatch(batch);
    }

    private void maybeTransitionToErrorState(RuntimeException exception) {
        if (exception instanceof ClusterAuthorizationException
                || exception instanceof TransactionalIdAuthorizationException
                || exception instanceof ProducerFencedException
                || exception instanceof UnsupportedVersionException) {
            transitionToFatalError(exception);
        } else if (isTransactional()) {
            if (canBumpEpoch() && !isCompleting()) {
                epochBumpRequired = true;
            }
            transitionToAbortableError(exception);
        }
    }

    synchronized void handleFailedBatch(ProducerBatch batch, RuntimeException exception, boolean adjustSequenceNumbers) {
        maybeTransitionToErrorState(exception);
        removeInFlightBatch(batch);

        if (hasFatalError()) {
            log.debug("Ignoring batch {} with producer id {}, epoch {}, and sequence number {} " +
                            "since the producer is already in fatal error state", batch, batch.producerId(),
                    batch.producerEpoch(), batch.baseSequence(), exception);
            return;
        }

        if (exception instanceof OutOfOrderSequenceException && !isTransactional()) {
            log.error("The broker returned {} for topic-partition {} with producerId {}, epoch {}, and sequence number {}",
                    exception, batch.topicPartition, batch.producerId(), batch.producerEpoch(), batch.baseSequence());

            // 如果我们使用OutOfOrderSequenceException失败，我们在日志中有一个空白。碰撞这个分区的epoch，这将把序列号重置为0，并允许我们继续
            requestEpochBumpForPartition(batch.topicPartition);
        } else if (exception instanceof UnknownProducerIdException) {
            // 如果我们获得分区的UnknownProducerId，则代理没有该生产者的状态。因此，它将接受序列号为0的写操作。我们在这里重置分区的序列号，
            // 以便生产者在终止事务后可以继续。对该分区的所有飞行请求也将失败，并出现UnknownProducerId错误，因此序列将保持为0。
            // 请注意，如果代理支持碰撞epoch，我们稍后将在调用InitProducerId后重置所有序列号
            resetSequenceForPartition(batch.topicPartition);
        } else {
            if (adjustSequenceNumbers) {
                if (!isTransactional()) {
                    requestEpochBumpForPartition(batch.topicPartition);
                } else {
                    adjustSequencesDueToFailedBatch(batch);
                }
            }
        }
    }

    // 如果批处理失败，则必须调整绑定到该分区的未来批处理的序列号，以便它们不会使用OutOfOrderSequenceException失败。
    // 这个方法必须只在我们知道被质疑的批处理已经被代理明确地失败时才调用。它已经收到一个确认的致命状态码，如“消息太大”或类似的东西。
    private void adjustSequencesDueToFailedBatch(ProducerBatch batch) {
        if (!topicPartitionBookkeeper.contains(batch.topicPartition))
            // 没有跟踪此分区的序列号。如果生产者id由于之前的OutOfOrderSequenceException而刚刚重置，则可能发生这种情况。
            return;
        log.debug("producerId: {}, send to partition {} failed fatally. Reducing future sequence numbers by {}",
                batch.producerId(), batch.topicPartition, batch.recordCount);
        int currentSequence = sequenceNumber(batch.topicPartition);
        currentSequence -= batch.recordCount;
        if (currentSequence < 0)
            throw new IllegalStateException("Sequence number for partition " + batch.topicPartition + " is going to become negative: " + currentSequence);

        setNextSequence(batch.topicPartition, currentSequence);

        topicPartitionBookkeeper.getPartition(batch.topicPartition).resetSequenceNumbers(inFlightBatch -> {
            if (inFlightBatch.baseSequence() < batch.baseSequence())
                return;

            int newSequence = inFlightBatch.baseSequence() - batch.recordCount;
            if (newSequence < 0)
                throw new IllegalStateException("Sequence number for batch with sequence " + inFlightBatch.baseSequence()
                        + " for partition " + batch.topicPartition + " is going to become negative: " + newSequence);

            log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}", inFlightBatch.baseSequence(), batch.topicPartition, newSequence);
            inFlightBatch.resetProducerState(new ProducerIdAndEpoch(inFlightBatch.producerId(), inFlightBatch.producerEpoch()), newSequence, inFlightBatch.isTransactional());
        });
    }

    synchronized boolean hasInflightBatches(TopicPartition topicPartition) {
        return !topicPartitionBookkeeper.getOrCreatePartition(topicPartition).inflightBatchesBySequence.isEmpty();
    }

    synchronized boolean hasStaleProducerIdAndEpoch(TopicPartition topicPartition) {
        return !producerIdAndEpoch.equals(topicPartitionBookkeeper.getOrCreatePartition(topicPartition).producerIdAndEpoch);
    }

    synchronized boolean hasUnresolvedSequences() {
        return !partitionsWithUnresolvedSequences.isEmpty();
    }

    synchronized boolean hasUnresolvedSequence(TopicPartition topicPartition) {
        return partitionsWithUnresolvedSequences.containsKey(topicPartition);
    }

    synchronized void markSequenceUnresolved(ProducerBatch batch) {
        int nextSequence = batch.lastSequence() + 1;
        partitionsWithUnresolvedSequences.compute(batch.topicPartition,
            (k, v) -> v == null ? nextSequence : Math.max(v, nextSequence));
        log.debug("Marking partition {} unresolved with next sequence number {}", batch.topicPartition,
                partitionsWithUnresolvedSequences.get(batch.topicPartition));
    }

    // 尝试解析未解析的序列。如果所有正在运行的请求都完成了，并且有些分区仍然无法解析，那么如果可能的话，要么碰撞epoch，要么转换为致命错误
    synchronized void maybeResolveSequences() {
        for (Iterator<TopicPartition> iter = partitionsWithUnresolvedSequences.keySet().iterator(); iter.hasNext(); ) {
            TopicPartition topicPartition = iter.next();
            if (!hasInflightBatches(topicPartition)) {
                // 分区已完全清空。此时，最后一个返回的序列应该比下一个用于分区的序列少一个。如果是，则分区已完全解析。
                // 如果没有，我们应该在必要时重置序列号。
                if (isNextSequence(topicPartition, sequenceNumber(topicPartition))) {
                    // 当一个批次过期，但后续批次成功时，就会发生这种情况。
                    iter.remove();
                } else {
                    // 如果所有在航批次最终在生产者中过期，我们将进入该分支。
                    if (isTransactional()) {
                        // 对于事务性生产者，如果可能的话，我们会撞击epoch，否则我们会转换到致命错误
                        String unackedMessagesErr = "The client hasn't received acknowledgment for some previously " +
                                "sent messages and can no longer retry them. ";
                        if (canBumpEpoch()) {
                            epochBumpRequired = true;
                            KafkaException exception = new KafkaException(unackedMessagesErr + "It is safe to abort " +
                                    "the transaction and continue.");
                            transitionToAbortableError(exception);
                        } else {
                            KafkaException exception = new KafkaException(unackedMessagesErr + "It isn't safe to continue.");
                            transitionToFatalError(exception);
                        }
                    } else {
                        // For the idempotent producer, bump the epoch
                        log.info("No inflight batches remaining for {}, last ack'd sequence for partition is {}, next sequence is {}. " +
                                        "Going to bump epoch and reset sequence numbers.", topicPartition,
                                lastAckedSequence(topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER), sequenceNumber(topicPartition));
                        requestEpochBumpForPartition(topicPartition);
                    }

                    iter.remove();
                }
            }
        }
    }

    private boolean isNextSequence(TopicPartition topicPartition, int sequence) {
        return sequence - lastAckedSequence(topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER) == 1;
    }

    private void setNextSequence(TopicPartition topicPartition, int sequence) {
        topicPartitionBookkeeper.getPartition(topicPartition).nextSequence = sequence;
    }

    private boolean isNextSequenceForUnresolvedPartition(TopicPartition topicPartition, int sequence) {
        return this.hasUnresolvedSequence(topicPartition) &&
                sequence == this.partitionsWithUnresolvedSequences.get(topicPartition);
    }

    synchronized TxnRequestHandler nextRequest(boolean hasIncompleteBatches) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        TxnRequestHandler nextRequestHandler = pendingRequests.peek();
        if (nextRequestHandler == null)
            return null;

        // Do not send the EndTxn until all batches have been flushed
        if (nextRequestHandler.isEndTxn() && hasIncompleteBatches)
            return null;

        pendingRequests.poll();
        if (maybeTerminateRequestWithError(nextRequestHandler)) {
            log.trace("Not sending transactional request {} because we are in an error state",
                    nextRequestHandler.requestBuilder());
            return null;
        }

        if (nextRequestHandler.isEndTxn() && !transactionStarted) {
            nextRequestHandler.result.done();
            if (currentState != State.FATAL_ERROR) {
                log.debug("Not sending EndTxn for completed transaction since no partitions " +
                        "or offsets were successfully added");
                completeTransaction();
            }
            nextRequestHandler = pendingRequests.poll();
        }

        if (nextRequestHandler != null)
            log.trace("Request {} dequeued for sending", nextRequestHandler.requestBuilder());

        return nextRequestHandler;
    }

    synchronized void retry(TxnRequestHandler request) {
        request.setRetry();
        enqueueRequest(request);
    }

    synchronized void authenticationFailed(AuthenticationException e) {
        for (TxnRequestHandler request : pendingRequests)
            request.fatalError(e);
    }

    synchronized void close() {
        KafkaException shutdownException = new KafkaException("The producer closed forcefully");
        pendingRequests.forEach(handler ->
                handler.fatalError(shutdownException));
        if (pendingResult != null) {
            pendingResult.fail(shutdownException);
        }
    }

    Node coordinator(FindCoordinatorRequest.CoordinatorType type) {
        switch (type) {
            case GROUP:
                return consumerGroupCoordinator;
            case TRANSACTION:
                return transactionCoordinator;
            default:
                throw new IllegalStateException("Received an invalid coordinator type: " + type);
        }
    }

    void lookupCoordinator(TxnRequestHandler request) {
        lookupCoordinator(request.coordinatorType(), request.coordinatorKey());
    }

    void setInFlightCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    private void clearInFlightCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    boolean hasInFlightRequest() {
        return inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    // visible for testing.
    boolean hasFatalError() {
        return currentState == State.FATAL_ERROR;
    }

    // visible for testing.
    boolean hasAbortableError() {
        return currentState == State.ABORTABLE_ERROR;
    }

    // visible for testing
    synchronized boolean transactionContainsPartition(TopicPartition topicPartition) {
        return partitionsInTransaction.contains(topicPartition);
    }

    // visible for testing
    synchronized boolean hasPendingOffsetCommits() {
        return !pendingTxnOffsetCommits.isEmpty();
    }

    synchronized boolean hasPendingRequests() {
        return !pendingRequests.isEmpty();
    }

    // visible for testing
    synchronized boolean hasOngoingTransaction() {
        // transactions are considered ongoing once started until completion or a fatal error
        return currentState == State.IN_TRANSACTION || isCompleting() || hasAbortableError();
    }

    synchronized boolean canRetry(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        Errors error = response.error;

        // UNKNOWN_PRODUCER_ID意味着我们已经丢失了代理上的生产者状态。根据日志开始偏移量的不同，我们可能需要重试这些操作，
        // 如下面的每种情况所述。如果这些都不适用，那么对于幂等生产者，我们将在本地碰撞epoch并从序列0重置飞行中批次的序列号，
        // 然后重试失败的批次，现在应该成功了。对于事务性生产者，允许批处理失败。在处理失败的批处理时，我们将转换到一个可中止的错误，
        // 并设置一个标志，表明我们需要撞击epoch(如果代理支持)。
        if (error == Errors.UNKNOWN_PRODUCER_ID) {
            if (response.logStartOffset == -1) {
                // 我们不知道这个响应的log开始偏移量。我们应该重试请求，直到我们得到它。UNKNOWN_PRODUCER_ID错误代码与
                // 新的producerResponse一起添加，其中包括logStartOffset。所以'-1'哨兵不是为了向后兼容。相反，
                // 代理可能在返回响应时不知道logStartOffset，因为从最初引发错误到构造响应时，分区可能已经离开了代理。
                // 在这些情况下，我们应该只是重试请求:一旦事情解决，我们保证最终获得logStartOffset。
                return true;
            }

            if (batch.sequenceHasBeenReset()) {
                // 当第一个飞行批次由于截断情况而失败时，所有其他飞行批次的序列将从头开始重新启动。然而，当这些响应从代理返回时，
                // 它们也会带来一个UNKNOWN_PRODUCER_ID错误。在这种情况下，我们不应该将序列号重置到开头。
                return true;
            } else if (lastAckedOffset(batch.topicPartition).orElse(NO_LAST_ACKED_SEQUENCE_NUMBER) < response.logStartOffset) {
                // 日志的头部已被删除，可能是由于保留时间的流逝。在这种情况下，我们期望失去生产者状态。对于事务生成器，
                // 将所有飞行批的序列重新设置为从头开始并重试它们，以便不需要中止事务。对于幂等产生器，碰撞epoch以避免重用(sequence, epoch)对
                if (isTransactional()) {
                    topicPartitionBookkeeper.startSequencesAtBeginning(batch.topicPartition, this.producerIdAndEpoch);
                } else {
                    requestEpochBumpForPartition(batch.topicPartition);
                }
                return true;
            }

            if (!isTransactional()) {
                // 对于幂等生产者，总是重试UNKNOWN_PRODUCER_ID错误。如果批处理具有当前的生产者ID和epoch，则请求epoch的bump。否则，请重试该产品。
                requestEpochBumpForPartition(batch.topicPartition);
                return true;
            }
        } else if (error == Errors.OUT_OF_ORDER_SEQUENCE_NUMBER) {
            if (!hasUnresolvedSequence(batch.topicPartition) &&
                    (batch.sequenceHasBeenReset() || !isNextSequence(batch.topicPartition, batch.baseSequence()))) {
                // 如果批处理不是下一个批处理，我们应该重试OutOfOrderSequenceException。它的基序列不是lastAckedSequence + 1。
                return true;
            } else if (!isTransactional()) {
                // 对于幂等生产者，重试所有OUT_OF_ORDER_SEQUENCE_NUMBER错误。如果没有未解决的序列，或者这批序列紧跟着一个未解决的序列，
                // 我们知道序列中实际上有一个间隙，我们碰撞历元。否则，重试而不碰撞，并等待查看序列是否解析
                if (!hasUnresolvedSequence(batch.topicPartition) ||
                        isNextSequenceForUnresolvedPartition(batch.topicPartition, batch.baseSequence())) {
                    requestEpochBumpForPartition(batch.topicPartition);
                }
                return true;
            }
        }

        // 如果上述两种情况都不成立，如果异常是可检索的，则重试
        return error.exception() instanceof RetriableException;
    }

    // visible for testing
    synchronized boolean isReady() {
        return isTransactional() && currentState == State.READY;
    }

    void handleCoordinatorReady() {
        NodeApiVersions nodeApiVersions = transactionCoordinator != null ?
                apiVersions.get(transactionCoordinator.idString()) :
                null;
        ApiVersion initProducerIdVersion = nodeApiVersions != null ?
                nodeApiVersions.apiVersion(ApiKeys.INIT_PRODUCER_ID) :
                null;
        this.coordinatorSupportsBumpingEpoch = initProducerIdVersion != null &&
                initProducerIdVersion.maxVersion() >= 3;
    }

    private void transitionTo(State target) {
        transitionTo(target, null);
    }

    private void transitionTo(State target, RuntimeException error) {
        if (!currentState.isTransitionValid(currentState, target)) {
            String idString = transactionalId == null ?  "" : "TransactionalId " + transactionalId + ": ";
            throw new KafkaException(idString + "Invalid transition attempted from state "
                    + currentState.name() + " to state " + target.name());
        }

        if (target == State.FATAL_ERROR || target == State.ABORTABLE_ERROR) {
            if (error == null)
                throw new IllegalArgumentException("Cannot transition to " + target + " with a null exception");
            lastError = error;
        } else {
            lastError = null;
        }

        if (lastError != null)
            log.debug("Transition from state {} to error state {}", currentState, target, lastError);
        else
            log.debug("Transition from state {} to {}", currentState, target);

        currentState = target;
    }

    private void ensureTransactional() {
        if (!isTransactional())
            throw new IllegalStateException("Transactional method invoked on a non-transactional producer.");
    }

    private void maybeFailWithError() {
        if (hasError()) {
            // 对于ProducerFencedException，不要将其包装为KafkaException，而是创建一个没有调用跟踪的新实例，因为它不是由于当前调用而抛出的
            if (lastError instanceof ProducerFencedException) {
                throw new ProducerFencedException("The producer has been rejected from the broker because " +
                    "it tried to use an old epoch with the transactionalId");
            } else if (lastError instanceof InvalidProducerEpochException) {
                throw new InvalidProducerEpochException("Producer attempted to produce with an old epoch " + producerIdAndEpoch);
            } else {
                throw new KafkaException("Cannot execute transactional method because we are in an error state", lastError);
            }
        }
    }

    private boolean maybeTerminateRequestWithError(TxnRequestHandler requestHandler) {
        if (hasError()) {
            if (hasAbortableError() && requestHandler instanceof FindCoordinatorHandler)
                // No harm letting the FindCoordinator request go through if we're expecting to abort
                return false;

            requestHandler.fail(lastError);
            return true;
        }
        return false;
    }

    private void enqueueRequest(TxnRequestHandler requestHandler) {
        log.debug("Enqueuing transactional request {}", requestHandler.requestBuilder());
        pendingRequests.add(requestHandler);
    }

    private void lookupCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
        switch (type) {
            case GROUP:
                consumerGroupCoordinator = null;
                break;
            case TRANSACTION:
                transactionCoordinator = null;
                break;
            default:
                throw new IllegalStateException("Invalid coordinator type: " + type);
        }

        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(type.id())
                .setKey(coordinatorKey);
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(data);
        enqueueRequest(new FindCoordinatorHandler(builder));
    }

    private TxnRequestHandler addPartitionsToTransactionHandler() {
        pendingPartitionsInTransaction.addAll(newPartitionsInTransaction);
        newPartitionsInTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder =
            new AddPartitionsToTxnRequest.Builder(transactionalId,
                producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch,
                new ArrayList<>(pendingPartitionsInTransaction));
        return new AddPartitionsToTxnHandler(builder);
    }

    private TxnOffsetCommitHandler txnOffsetCommitHandler(TransactionalRequestResult result,
                                                          Map<TopicPartition, OffsetAndMetadata> offsets,
                                                          ConsumerGroupMetadata groupMetadata) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            CommittedOffset committedOffset = new CommittedOffset(offsetAndMetadata.offset(),
                    offsetAndMetadata.metadata(), offsetAndMetadata.leaderEpoch());
            pendingTxnOffsetCommits.put(entry.getKey(), committedOffset);
        }

        final TxnOffsetCommitRequest.Builder builder =
            new TxnOffsetCommitRequest.Builder(transactionalId,
                groupMetadata.groupId(),
                producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch,
                pendingTxnOffsetCommits,
                groupMetadata.memberId(),
                groupMetadata.generationId(),
                groupMetadata.groupInstanceId()
            );
        return new TxnOffsetCommitHandler(result, builder);
    }

    private TransactionalRequestResult handleCachedTransactionRequestResult(
            Supplier<TransactionalRequestResult> transactionalRequestResultSupplier,
            State targetState) {
        ensureTransactional();

        if (pendingResult != null && currentState == targetState) {
            TransactionalRequestResult result = pendingResult;
            if (result.isCompleted())
                pendingResult = null;
            return result;
        }

        pendingResult = transactionalRequestResultSupplier.get();
        return pendingResult;
    }

    // package-private for testing
    boolean canBumpEpoch() {
        if (!isTransactional()) {
            return true;
        }

        return coordinatorSupportsBumpingEpoch;
    }

    private void completeTransaction() {
        if (epochBumpRequired) {
            transitionTo(State.INITIALIZING);
        } else {
            transitionTo(State.READY);
        }
        lastError = null;
        epochBumpRequired = false;
        transactionStarted = false;
        newPartitionsInTransaction.clear();
        pendingPartitionsInTransaction.clear();
        partitionsInTransaction.clear();
    }

    abstract class TxnRequestHandler implements RequestCompletionHandler {
        protected final TransactionalRequestResult result;
        private boolean isRetry = false;

        TxnRequestHandler(TransactionalRequestResult result) {
            this.result = result;
        }

        TxnRequestHandler(String operation) {
            this(new TransactionalRequestResult(operation));
        }

        void fatalError(RuntimeException e) {
            result.fail(e);
            transitionToFatalError(e);
        }

        void abortableError(RuntimeException e) {
            result.fail(e);
            transitionToAbortableError(e);
        }

        void abortableErrorIfPossible(RuntimeException e) {
            if (canBumpEpoch()) {
                epochBumpRequired = true;
                abortableError(e);
            } else {
                fatalError(e);
            }
        }

        void fail(RuntimeException e) {
            result.fail(e);
        }

        void reenqueue() {
            synchronized (TransactionManager.this) {
                this.isRetry = true;
                enqueueRequest(this);
            }
        }

        long retryBackoffMs() {
            return retryBackoffMs;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId) {
                fatalError(new RuntimeException("Detected more than one in-flight transactional request."));
            } else {
                clearInFlightCorrelationId();
                if (response.wasDisconnected()) {
                    log.debug("Disconnected from {}. Will retry.", response.destination());
                    if (this.needsCoordinator())
                        lookupCoordinator(this.coordinatorType(), this.coordinatorKey());
                    reenqueue();
                } else if (response.versionMismatch() != null) {
                    fatalError(response.versionMismatch());
                } else if (response.hasResponse()) {
                    log.trace("Received transactional response {} for request {}", response.responseBody(),
                            requestBuilder());
                    synchronized (TransactionManager.this) {
                        handleResponse(response.responseBody());
                    }
                } else {
                    fatalError(new KafkaException("Could not execute transactional request for unknown reasons"));
                }
            }
        }

        boolean needsCoordinator() {
            return coordinatorType() != null;
        }

        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
        }

        String coordinatorKey() {
            return transactionalId;
        }

        void setRetry() {
            this.isRetry = true;
        }

        boolean isRetry() {
            return isRetry;
        }

        boolean isEndTxn() {
            return false;
        }

        abstract AbstractRequest.Builder<?> requestBuilder();

        abstract void handleResponse(AbstractResponse responseBody);

        abstract Priority priority();
    }

    private class InitProducerIdHandler extends TxnRequestHandler {
        private final InitProducerIdRequest.Builder builder;
        private final boolean isEpochBump;

        private InitProducerIdHandler(InitProducerIdRequest.Builder builder, boolean isEpochBump) {
            super("InitProducerId");
            this.builder = builder;
            this.isEpochBump = isEpochBump;
        }

        @Override
        InitProducerIdRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return this.isEpochBump ? Priority.EPOCH_BUMP : Priority.INIT_PRODUCER_ID;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            if (isTransactional()) {
                return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
            } else {
                return null;
            }
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            InitProducerIdResponse initProducerIdResponse = (InitProducerIdResponse) response;
            Errors error = initProducerIdResponse.error();

            if (error == Errors.NONE) {
                ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(initProducerIdResponse.data().producerId(),
                        initProducerIdResponse.data().producerEpoch());
                setProducerIdAndEpoch(producerIdAndEpoch);
                transitionTo(State.READY);
                lastError = null;
                if (this.isEpochBump) {
                    resetSequenceNumbers();
                }
                result.done();
            } else if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                    error == Errors.CLUSTER_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                // just treat it the same as PRODUCE_FENCED.
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else {
                fatalError(new KafkaException("Unexpected error in InitProducerIdResponse; " + error.message()));
            }
        }
    }

    private class AddPartitionsToTxnHandler extends TxnRequestHandler {
        private final AddPartitionsToTxnRequest.Builder builder;
        private long retryBackoffMs;

        private AddPartitionsToTxnHandler(AddPartitionsToTxnRequest.Builder builder) {
            super("AddPartitionsToTxn");
            this.builder = builder;
            this.retryBackoffMs = TransactionManager.this.retryBackoffMs;
        }

        @Override
        AddPartitionsToTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;
            Map<TopicPartition, Errors> errors = addPartitionsToTxnResponse.errors();
            boolean hasPartitionErrors = false;
            Set<String> unauthorizedTopics = new HashSet<>();
            retryBackoffMs = TransactionManager.this.retryBackoffMs;

            for (Map.Entry<TopicPartition, Errors> topicPartitionErrorEntry : errors.entrySet()) {
                TopicPartition topicPartition = topicPartitionErrorEntry.getKey();
                Errors error = topicPartitionErrorEntry.getValue();

                if (error == Errors.NONE) {
                    continue;
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                    reenqueue();
                    return;
                } else if (error == Errors.CONCURRENT_TRANSACTIONS) {
                    maybeOverrideRetryBackoffMs();
                    reenqueue();
                    return;
                } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    reenqueue();
                    return;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                    // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                    // just treat it the same as PRODUCE_FENCED.
                    fatalError(Errors.PRODUCER_FENCED.exception());
                    return;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.INVALID_TXN_STATE) {
                    fatalError(new KafkaException(error.exception()));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(topicPartition.topic());
                } else if (error == Errors.OPERATION_NOT_ATTEMPTED) {
                    log.debug("Did not attempt to add partition {} to transaction because other partitions in the " +
                            "batch had errors.", topicPartition);
                    hasPartitionErrors = true;
                } else if (error == Errors.UNKNOWN_PRODUCER_ID || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                    abortableErrorIfPossible(error.exception());
                    return;
                } else {
                    log.error("Could not add partition {} due to unexpected error {}", topicPartition, error);
                    hasPartitionErrors = true;
                }
            }

            Set<TopicPartition> partitions = errors.keySet();

            // 无论结果如何，都要从挂起集中删除分区。我们使用挂起集中存在的分区来知道何时发送批处理是不安全的。
            // 但是，如果分区添加失败并且我们进入错误状态，我们希望批处理无论如何都会被终止。在这种情况下，
            // 我们必须能够继续为已成功添加的分区发送正在重试的批。
            pendingPartitionsInTransaction.removeAll(partitions);

            if (!unauthorizedTopics.isEmpty()) {
                abortableError(new TopicAuthorizationException(unauthorizedTopics));
            } else if (hasPartitionErrors) {
                abortableError(new KafkaException("Could not add partitions to transaction due to errors: " + errors));
            } else {
                log.debug("Successfully added partitions {} to transaction", partitions);
                partitionsInTransaction.addAll(partitions);
                transactionStarted = true;
                result.done();
            }
        }

        @Override
        public long retryBackoffMs() {
            return Math.min(TransactionManager.this.retryBackoffMs, this.retryBackoffMs);
        }

        private void maybeOverrideRetryBackoffMs() {
            // 我们只希望在重试第一个AddPartition(由于CONCURRENT_TRANSACTIONS错误而出错)时减少回退，因为这意味着前一个事务仍在完成，
            // 我们不想在尝试启动新事务之前等待太长时间。这只是一个临时的解决方案，长期的解决方案正在进行中
            // https://issues.apache.org/jira/browse/KAFKA-5482
            if (partitionsInTransaction.isEmpty())
                this.retryBackoffMs = ADD_PARTITIONS_RETRY_BACKOFF_MS;
        }
    }

    private class FindCoordinatorHandler extends TxnRequestHandler {
        private final FindCoordinatorRequest.Builder builder;

        private FindCoordinatorHandler(FindCoordinatorRequest.Builder builder) {
            super("FindCoordinator");
            this.builder = builder;
        }

        @Override
        FindCoordinatorRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.FIND_COORDINATOR;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return null;
        }

        @Override
        String coordinatorKey() {
            return null;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            CoordinatorType coordinatorType = CoordinatorType.forId(builder.data().keyType());

            List<Coordinator> coordinators = ((FindCoordinatorResponse) response).coordinators();
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                fatalError(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            Coordinator coordinatorData = coordinators.get(0);
            // 对于没有批处理的旧版本，请从请求数据中获取键，因为它不包含在响应中
            String key = coordinatorData.key() == null ? builder.data().key() : coordinatorData.key();
            Errors error = Errors.forCode(coordinatorData.errorCode());
            if (error == Errors.NONE) {
                Node node = new Node(coordinatorData.nodeId(), coordinatorData.host(), coordinatorData.port());
                switch (coordinatorType) {
                    case GROUP:
                        consumerGroupCoordinator = node;
                        break;
                    case TRANSACTION:
                        transactionCoordinator = node;

                }
                result.done();
                log.info("Discovered {} coordinator {}", coordinatorType.toString().toLowerCase(Locale.ROOT), node);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(GroupAuthorizationException.forGroupId(key));
            } else {
                fatalError(new KafkaException(String.format("Could not find a coordinator with type %s with key %s due to " +
                        "unexpected error: %s", coordinatorType, key,
                        coordinatorData.errorMessage())));
            }
        }
    }

    private class EndTxnHandler extends TxnRequestHandler {
        private final EndTxnRequest.Builder builder;

        private EndTxnHandler(EndTxnRequest.Builder builder) {
            super("EndTxn(" + builder.data.committed() + ")");
            this.builder = builder;
        }

        @Override
        EndTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.END_TXN;
        }

        @Override
        boolean isEndTxn() {
            return true;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) response;
            Errors error = endTxnResponse.error();

            if (error == Errors.NONE) {
                completeTransaction();
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // 我们仍然可以从旧版本的事务协调器接收INVALID_PRODUCER_EPOCH，只需将其视为与PRODUCE_FENCED相同。
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.INVALID_TXN_STATE) {
                fatalError(error.exception());
            } else if (error == Errors.UNKNOWN_PRODUCER_ID || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                abortableErrorIfPossible(error.exception());
            } else {
                fatalError(new KafkaException("Unhandled error in EndTxnResponse: " + error.message()));
            }
        }
    }

    private class AddOffsetsToTxnHandler extends TxnRequestHandler {
        private final AddOffsetsToTxnRequest.Builder builder;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final ConsumerGroupMetadata groupMetadata;

        private AddOffsetsToTxnHandler(AddOffsetsToTxnRequest.Builder builder,
                                       Map<TopicPartition, OffsetAndMetadata> offsets,
                                       ConsumerGroupMetadata groupMetadata) {
            super("AddOffsetsToTxn");
            this.builder = builder;
            this.offsets = offsets;
            this.groupMetadata = groupMetadata;
        }

        @Override
        AddOffsetsToTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            AddOffsetsToTxnResponse addOffsetsToTxnResponse = (AddOffsetsToTxnResponse) response;
            Errors error = Errors.forCode(addOffsetsToTxnResponse.data().errorCode());

            if (error == Errors.NONE) {
                log.debug("Successfully added partition for consumer group {} to transaction", builder.data.groupId());

                // note the result is not completed until the TxnOffsetCommit returns
                pendingRequests.add(txnOffsetCommitHandler(result, offsets, groupMetadata));

                transactionStarted = true;
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.UNKNOWN_PRODUCER_ID || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                abortableErrorIfPossible(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // 我们仍然可以从旧版本的事务协调器接收INVALID_PRODUCER_EPOCH，只需将其视为与PRODUCE_FENCED相同。
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(GroupAuthorizationException.forGroupId(builder.data.groupId()));
            } else {
                fatalError(new KafkaException("Unexpected error in AddOffsetsToTxnResponse: " + error.message()));
            }
        }
    }

    private class TxnOffsetCommitHandler extends TxnRequestHandler {
        private final TxnOffsetCommitRequest.Builder builder;

        private TxnOffsetCommitHandler(TransactionalRequestResult result,
                                       TxnOffsetCommitRequest.Builder builder) {
            super(result);
            this.builder = builder;
        }

        @Override
        TxnOffsetCommitRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.GROUP;
        }

        @Override
        String coordinatorKey() {
            return builder.data.groupId();
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) response;
            boolean coordinatorReloaded = false;
            Map<TopicPartition, Errors> errors = txnOffsetCommitResponse.errors();

            log.debug("Received TxnOffsetCommit response for consumer group {}: {}", builder.data.groupId(),
                    errors);

            for (Map.Entry<TopicPartition, Errors> entry : errors.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    pendingTxnOffsetCommits.remove(topicPartition);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR
                        || error == Errors.REQUEST_TIMED_OUT) {
                    if (!coordinatorReloaded) {
                        coordinatorReloaded = true;
                        lookupCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, builder.data.groupId());
                    }
                } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION
                        || error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // 如果主题未知或正在加载协调器，请使用当前协调器重试
                    continue;
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    abortableError(GroupAuthorizationException.forGroupId(builder.data.groupId()));
                    break;
                } else if (error == Errors.FENCED_INSTANCE_ID) {
                    abortableError(error.exception());
                    break;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    abortableError(new CommitFailedException("Transaction offset Commit failed " +
                        "due to consumer group metadata mismatch: " + error.exception().getMessage()));
                    break;
                } else if (isFatalException(error)) {
                    fatalError(error.exception());
                    break;
                } else {
                    fatalError(new KafkaException("Unexpected error in TxnOffsetCommitResponse: " + error.message()));
                    break;
                }
            }

            if (result.isCompleted()) {
                pendingTxnOffsetCommits.clear();
            } else if (pendingTxnOffsetCommits.isEmpty()) {
                result.done();
            } else {
                // 重试提交失败的可检索错误
                reenqueue();
            }
        }
    }

    private boolean isFatalException(Errors error) {
        return error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                   || error == Errors.INVALID_PRODUCER_EPOCH
                   || error == Errors.PRODUCER_FENCED
                   || error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT;
    }
}
