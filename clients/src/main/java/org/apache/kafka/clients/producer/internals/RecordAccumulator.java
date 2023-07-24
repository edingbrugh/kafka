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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * 这个类充当一个队列，将记录累积到 {@link MemoryRecords}
 * 要发送到服务器的实例。
 * <p>
 * 累加器使用有限的内存，当内存耗尽时，追加调用将阻塞，除非显式禁用此行为。
 */
public final class RecordAccumulator {

    private final Logger log;
    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;
    private final int lingerMs;
    private final long retryBackoffMs;
    private final int deliveryTimeoutMs;
    private final BufferPool free;
    private final Time time;
    private final ApiVersions apiVersions;
    private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;
    private final IncompleteBatches incomplete;
    // 以下变量仅由发送线程访问，因此我们不需要保护它们。
    private final Set<TopicPartition> muted;
    private int drainIndex;
    private final TransactionManager transactionManager;
    // 批处理过期的最早时间(绝对时间)。
    private long nextBatchExpiryTimeMs = Long.MAX_VALUE;

    public RecordAccumulator(LogContext logContext,
                             int batchSize,
                             CompressionType compression,
                             int lingerMs,
                             long retryBackoffMs,
                             int deliveryTimeoutMs,
                             Metrics metrics,
                             String metricGrpName,
                             Time time,
                             ApiVersions apiVersions,
                             TransactionManager transactionManager,
                             BufferPool bufferPool) {
        this.log = logContext.logger(RecordAccumulator.class);
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.batches = new CopyOnWriteMap<>();
        this.free = bufferPool;
        this.incomplete = new IncompleteBatches();
        this.muted = new HashSet<>();
        this.time = time;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);
    }

    /**
     * 向累加器添加一条记录，返回附加结果
     * <p>
     * 追加结果将包含未来的元数据，并标记追加的批处理是已满还是创建了新批处理
     * <p>
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock,
                                     boolean abortOnNewBatch,
                                     long nowMs) throws InterruptedException {
        // 我们跟踪附加线程的数量，以确保我们不会在abortIncompleteBatches()中错过批次。
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // 检查我们是否有正在进行的批次
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);
            synchronized (dq) {
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                if (appendResult != null)
                    return appendResult;
            }

            // 我们没有正在进行的记录批尝试分配一个新批
            if (abortOnNewBatch) {
                // 返回将导致另一个追加调用的结果。
                return new RecordAppendResult(null, false, false, true);
            }

            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {} with remaining timeout {}ms", size, tp.topic(), tp.partition(), maxTimeToBlock);
            buffer = free.allocate(size, maxTimeToBlock);

            // 如果上面的缓冲区分配阻塞，则更新当前时间。
            nowMs = time.milliseconds();
            synchronized (dq) {
                // 需要在获取dequeue锁后检查生产者是否再次关闭。
                if (closed)
                    throw new KafkaException("Producer closed while send in progress");

                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq, nowMs);
                if (appendResult != null) {
                    // 有人给我们找了一批，把我们等的那个还给我们!希望这不会经常发生……
                    return appendResult;
                }

                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, nowMs);
                FutureRecordMetadata future = Objects.requireNonNull(batch.tryAppend(timestamp, key, value, headers,
                        callback, nowMs));

                dq.addLast(batch);
                incomplete.add(batch);

                // 不要在finally块中释放这个缓冲区，因为它正在记录批处理中使用
                buffer = null;
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true, false);
            }
        } finally {
            if (buffer != null)
                free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    private MemoryRecordsBuilder recordsBuilder(ByteBuffer buffer, byte maxUsableMagic) {
        if (transactionManager != null && maxUsableMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedVersionException("Attempting to use idempotence with a broker which does not " +
                "support the required message format (v2). The broker must be version 0.11 or later.");
        }
        return MemoryRecords.builder(buffer, maxUsableMagic, compression, TimestampType.CREATE_TIME, 0L);
    }

    /**
     *  尝试追加到一个ProducerBatch。如果已满，则返回null并创建一个新的批处理。我们还关闭记录追加的批处理，以释放压缩缓冲区等资源。
     *  这批货将被完全封闭。记录批处理头将在以下情况之一(以先出现的为准)被写入和内存记录:在发送之前，如果过期，或者当生产者关闭时。
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque, long nowMs) {
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, nowMs);
            if (future == null)
                last.closeForRecordAppends();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false, false);
        }
        return null;
    }

    private boolean isMuted(TopicPartition tp) {
        return muted.contains(tp);
    }

    public void resetNextBatchExpiryTime() {
        nextBatchExpiryTimeMs = Long.MAX_VALUE;
    }

    public void maybeUpdateNextBatchExpiryTime(ProducerBatch batch) {
        if (batch.createdMs + deliveryTimeoutMs  > 0) {
            // 非否定检查是为了防止由于设置了较大的deliveryTimeoutMs值而导致的潜在溢出
            nextBatchExpiryTimeMs = Math.min(nextBatchExpiryTimeMs, batch.createdMs + deliveryTimeoutMs);
        } else {
            log.warn("Skipping next batch expiry time update due to addition overflow: "
                + "batch.createMs={}, deliveryTimeoutMs={}", batch.createdMs, deliveryTimeoutMs);
        }
    }

    /**
     * 获取在累加器中放置时间过长且需要过期的批次列表。
     */
    public List<ProducerBatch> expiredBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            // 按发送顺序使批过期
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                while (!deque.isEmpty()) {
                    ProducerBatch batch = deque.getFirst();
                    if (batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)) {
                        deque.poll();
                        batch.abortRecordAppends();
                        expiredBatches.add(batch);
                    } else {
                        maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
            }
        }
        return expiredBatches;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /**
     * 在累加器中重新排队给定的记录批。在Sender.completeBatch方法中，我们检查批处理是否达到了deliveryTimeoutMs。因此，我们在这里不做交付超时检查。
     */
    public void reenqueue(ProducerBatch batch, long now) {
        batch.reenqueued(now);
        Deque<ProducerBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            if (transactionManager != null)
                insertInSequenceOrder(deque, batch);
            else
                deque.addFirst(batch);
        }
    }

    /**
     * 拆分已被拒绝的大批，并将拆分后的批重新排队到累加器中。
     */
    public int splitAndReenqueue(ProducerBatch bigBatch) {
        // 将估计的压缩比重置为初始值或大批量压缩比，以较大者为准。有几种不同的重置方法。我们选择了最保守的一个，以确保分裂不会发生太频繁。
        CompressionRatioEstimator.setEstimation(bigBatch.topicPartition.topic(), compression,
                                                Math.max(1.0f, (float) bigBatch.compressionRatio()));
        Deque<ProducerBatch> dq = bigBatch.split(this.batchSize);
        int numSplitBatches = dq.size();
        Deque<ProducerBatch> partitionDequeue = getOrCreateDeque(bigBatch.topicPartition);
        while (!dq.isEmpty()) {
            ProducerBatch batch = dq.pollLast();
            incomplete.add(batch);
            // We treat the newly split batches as if they are not even tried.
            synchronized (partitionDequeue) {
                if (transactionManager != null) {
                    // We should track the newly created batches since they already have assigned sequences.
                    transactionManager.addInFlightBatch(batch);
                    insertInSequenceOrder(partitionDequeue, batch);
                } else {
                    partitionDequeue.addFirst(batch);
                }
            }
        }
        return numSplitBatches;
    }

    // 我们必须做额外的工作，以确保在重试请求时队列是有序的，并且有多个请求正在向该分区发送。如果第一个在途请求未能追加，
    // 那么所有后续的在途请求也将失败，因为序列号将不被接受。此外，一旦重新尝试批处理，我们就会减少到对该分区的单个在线请求。所以,当
    // 随后的批次按顺序返回，它们将被放置在队列的更后面。注意，这假定队列中具有分配序列的所有批也具有当前生产者id。如果生产者id已经更改，
    // 我们将不会尝试重新排序消息，我们将抛出一个IllegalStateException代替。
    private void insertInSequenceOrder(Deque<ProducerBatch> deque, ProducerBatch batch) {
        // When we are requeing and have enabled idempotence, the reenqueued batch must always have a sequence.
        if (batch.baseSequence() == RecordBatch.NO_SEQUENCE)
            throw new IllegalStateException("Trying to re-enqueue a batch which doesn't have a sequence even " +
                "though idempotency is enabled.");

        if (transactionManager.nextBatchBySequence(batch.topicPartition) == null)
            throw new IllegalStateException("We are re-enqueueing a batch which is not tracked as part of the in flight " +
                "requests. batch.topicPartition: " + batch.topicPartition + "; batch.baseSequence: " + batch.baseSequence());

        ProducerBatch firstBatchInQueue = deque.peekFirst();
        if (firstBatchInQueue != null && firstBatchInQueue.hasSequence() && firstBatchInQueue.baseSequence() < batch.baseSequence()) {
            // 在不违反序列顺序的情况下，进入的批不能插入到队列的前面。这意味着进入的批次应该放在更后面的某个地方。
            // 我们需要找到合适的位置来存放这批货，并把它放在那里。只有当我们有多个航班发送给不同的代理并且我们需要重试航班批次时，
            // 我们才会进入此分支。由于我们每次只重新排队一个批次，并确保队列总是按顺序排序，
            // 因此每次对飞行批次的子集进行简单的线性扫描以找到队列中的正确位置。
            List<ProducerBatch> orderedBatches = new ArrayList<>();
            while (deque.peekFirst() != null && deque.peekFirst().hasSequence() && deque.peekFirst().baseSequence() < batch.baseSequence())
                orderedBatches.add(deque.pollFirst());

            log.debug("Reordered incoming batch with sequence {} for partition {}. It was placed in the queue at " +
                "position {}", batch.baseSequence(), batch.topicPartition, orderedBatches.size());
            // 要么我们已经达到了没有顺序的批次(即。从未被耗尽，因此在默认情况下是有序的)，或者队列前面的批处理的序列大于进入的批处理。
            // 这是添加来料批次的正确位置。
            deque.addFirst(batch);

            // 现在我们必须以正确的顺序重新插入先前排队的批。
            for (int i = orderedBatches.size() - 1; i >= 0; --i) {
                deque.addFirst(orderedBatches.get(i));
            }

            // 此时，进入的批已根据其顺序在正确的位置排队。
        } else {
            deque.addFirst(batch);
        }
    }

    /**
     * 获取分区准备发送的节点列表，以及任何不可发送分区准备就绪的最早时间;同时返回累积分区批次中是否存在未知leader的标志。
     * <p>
     *  如果满足以下条件，则目标节点已准备好发送数据:
     * <ol>
     *  至少有一个分区没有退出它的发送这些分区不暂停(防止重新排序)
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *   <li>记录集已满</li>
     *  <li>记录集在累加器中保存了至少lingerMs毫秒</li>
     *  <li>累加器内存不足，线程阻塞等待数据(在这种情况下是所有分区)立即被认为是准备就绪)。</li>
     *  <li>累加器已关闭</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();

        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                // 当生成大量分区时，这个路径是热的，deque通常是空的。我们首先检查批处理是否存在，以尽可能避免更昂贵的检查。
                ProducerBatch batch = deque.peekFirst();
                if (batch != null) {
                    TopicPartition part = entry.getKey();
                    Node leader = cluster.leaderFor(part);
                    if (leader == null) {
                        // 这是一个leader未知的分区，但是可以发送消息。请注意，当deque为空时，条目目前不会从批处理中删除。
                        unknownLeaderTopics.add(part.topic());
                    } else if (!readyNodes.contains(leader) && !isMuted(part)) {
                        long waitedTimeMs = batch.waitedTimeMs(nowMs);
                        boolean backingOff = batch.attempts() > 0 && waitedTimeMs < retryBackoffMs;
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        boolean full = deque.size() > 1 || batch.isFull();
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        boolean transactionCompleting = transactionManager != null && transactionManager.isCompleting();
                        boolean sendable = full
                            || expired
                            || exhausted
                            || closed
                            || flushInProgress()
                            || transactionCompleting;
                        if (sendable && !backingOff) {
                            readyNodes.add(leader);
                        } else {
                            long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                            // 请注意，这是一个保守的估计，因为一个不可发送的分区可能有一个leader，稍后会发现这个leader有可发送的数据。
                            // 然而，这已经足够好了，因为我们只需要醒来，然后在剩下的时间里再次睡觉。
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * 检查是否有未处理的批次
     */
    public boolean hasUndrained() {
        for (Map.Entry<TopicPartition, Deque<ProducerBatch>> entry : this.batches.entrySet()) {
            Deque<ProducerBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    private boolean shouldStopDrainBatchesForPartition(ProducerBatch first, TopicPartition tp) {
        ProducerIdAndEpoch producerIdAndEpoch = null;
        if (transactionManager != null) {
            if (!transactionManager.isSendToPartitionAllowed(tp))
                return true;

            producerIdAndEpoch = transactionManager.producerIdAndEpoch();
            if (!producerIdAndEpoch.isValid())
                // 在刷新生产者id之前，我们无法发送批次
                return true;

            if (!first.hasSequence()) {
                if (transactionManager.hasInflightBatches(tp) && transactionManager.hasStaleProducerIdAndEpoch(tp)) {
                    // 当分区具有具有不同epoch和/或生产者ID的运行中的批时，不要消耗任何新批。否则，在较早的批完成之前，
                    // 可能会写入具有新epoch和序号为0的批，这将导致序列错误
                    return true;
                }

                if (transactionManager.hasUnresolvedSequence(first.topicPartition))
                    // 当之前序列号的状态未知时，不要排出任何新批次。如果之前的批次在发送到broker至少一次之后在客户端上被终止，那么它们将是未知的。
                    return true;
            }

            int firstInFlightSequence = transactionManager.firstInFlightSequence(first.topicPartition);
            if (firstInFlightSequence != RecordBatch.NO_SEQUENCE && first.hasSequence()
                && first.baseSequence() != firstInFlightSequence)
                // 如果排队的批处理已经有一个分配的序列，那么它将被重试。在这种情况下，我们等待，直到下一批立即准备好，并将其处理。
                // 只有当下一个在线批处理完成时(无论是成功还是由于致命的代理错误)，我们才会继续。这有效地将我们的飞行请求计数减少到1。
                return true;
        }
        return false;
    }

    private List<ProducerBatch> drainBatchesForOneNode(Cluster cluster, Node node, int maxSize, long now) {
        int size = 0;
        List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
        List<ProducerBatch> ready = new ArrayList<>();
        /* 为了减少饥饿的可能性，这个循环不是从0开始的 */
        int start = drainIndex = drainIndex % parts.size();
        do {
            PartitionInfo part = parts.get(drainIndex);
            TopicPartition tp = new TopicPartition(part.topic(), part.partition());
            this.drainIndex = (this.drainIndex + 1) % parts.size();

            // 只有在分区没有运行中的批处理时才继续。
            if (isMuted(tp))
                continue;

            Deque<ProducerBatch> deque = getDeque(tp);
            if (deque == null)
                continue;

            synchronized (deque) {
                // invariant: !isMuted(tp,now) && deque != null
                ProducerBatch first = deque.peekFirst();
                if (first == null)
                    continue;

                // first != null
                boolean backoff = first.attempts() > 0 && first.waitedTimeMs(now) < retryBackoffMs;
                // 如果不是在回撤期间，只排干该批次的水分。
                if (backoff)
                    continue;

                if (size + first.estimatedSizeInBytes() > maxSize && !ready.isEmpty()) {
                    // 由于压缩，单个批处理大小比请求大小大的情况很少见;在这种情况下，我们最终仍将在单个请求中发送此批处理
                    break;
                } else {
                    if (shouldStopDrainBatchesForPartition(first, tp))
                        break;

                    boolean isTransactional = transactionManager != null && transactionManager.isTransactional();
                    ProducerIdAndEpoch producerIdAndEpoch =
                        transactionManager != null ? transactionManager.producerIdAndEpoch() : null;
                    ProducerBatch batch = deque.pollFirst();
                    if (producerIdAndEpoch != null && !batch.hasSequence()) {
                        // 如果分区的生产者id/epoch与最新的生产者id/epoch不匹配，我们更新它并重置序列。这应该是
                        //只有当所有飞行中的批次都完成时才会执行。这是' shouldStopDrainBatchesForPartition '中的保证。
                        transactionManager.maybeUpdateProducerIdAndEpoch(batch.topicPartition);

                        // 如果批处理已经有一个分配的序列，那么我们不应该更改生产者id和序列号，因为这可能会引入重复。特别是，
                        // 之前的尝试实际上可能已经被接受，如果我们在这里更改生产者id和序列，这个尝试也将被接受，从而导致重复。
                        // 此外，我们更新分区的下一个序列号，并让事务管理器跟踪批处理，以确保即使我们收到无序的响应，也能保持序列顺序。
                        batch.setProducerState(producerIdAndEpoch, transactionManager.sequenceNumber(batch.topicPartition), isTransactional);
                        transactionManager.incrementSequenceNumber(batch.topicPartition, batch.recordCount);
                        log.debug("Assigned producerId {} and producerEpoch {} to batch with base sequence " +
                                "{} being sent to partition {}", producerIdAndEpoch.producerId,
                            producerIdAndEpoch.epoch, batch.baseSequence(), tp);

                        transactionManager.addInFlightBatch(batch);
                    }
                    batch.close();
                    size += batch.records().sizeInBytes();
                    ready.add(batch);

                    batch.drained(now);
                }
            }
        } while (start != drainIndex);
        return ready;
    }

    /**
     * 抽取给定节点的所有数据，并将它们整理成批列表，这些批列表将适合每个节点的指定大小。这种方法试图避免一次又一次地选择相同的主题节点。
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link ProducerBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<ProducerBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<ProducerBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            List<ProducerBatch> ready = drainBatchesForOneNode(cluster, node, maxSize, now);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    /**
     * 批处理将过期的最早绝对时间(单位为毫秒)
     */
    public long nextExpiryTimeMs() {
        return this.nextBatchExpiryTimeMs;
    }

    private Deque<ProducerBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * 获取给定主题分区的队列，必要时创建它。
     */
    private Deque<ProducerBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<ProducerBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        d = new ArrayDeque<>();
        Deque<ProducerBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
     * 释放记录批处理
     */
    public void deallocate(ProducerBatch batch) {
        incomplete.remove(batch);
        // 只有在批处理不是拆分批处理时才释放该批处理，因为拆分批处理是在缓冲池之外分配的。
        if (!batch.isSplitBatch())
            free.deallocate(batch.buffer(), batch.initialCapacity());
    }

    /**
     * 包私有用于单元测试。获取缓冲池剩余大小(以字节为单位)。
     */
    long bufferPoolAvailableMemory() {
        return free.availableMemory();
    }


    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<ProducerBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }

    /**
     * 启动累加器的数据刷新…这使所有请求立即准备好
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * 当前是否有线程附加消息?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * 将所有分区标记为准备发送并阻塞，直到发送完成
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            // 在刷新时获取所有不完整的ProduceRequestResult的副本。我们必须小心不要保留对ProduceBatch的引用，
            // 以便可以对内容进行垃圾收集。发送方将从原始的不完整集合中删除ProducerBatch。
            for (ProduceRequestResult result : this.incomplete.requestResults())
                result.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * 检查是否有待处理的批处理(是否已发送或未发送)。
     */
    public boolean hasIncomplete() {
        return !this.incomplete.isEmpty();
    }

    /**
     * 此函数仅在强制关闭发送方时调用。它将使所有不完整的批次不合格并返回。
     */
    public void abortIncompleteBatches() {
        // 我们需要继续中止未完成的批处理，直到没有线程试图追加到
        // 1。避免丢失批次。
        // 2. 释放内存，以防附加线程在缓冲区满时被阻塞。这是一个紧密的循环，但应该能够很快通过。
        do {
            abortBatches();
        } while (appendsInProgress());
        // 在此之后，没有线程将追加任何消息，因为它们将看到关闭标志设置。我们需要在没有线程追加后执行最后一个中止，
        // 以防最后一个追加线程追加了一个新批。
        abortBatches();
        this.batches.clear();
    }

    /**
     * 检查不完整的批次并中止它们。
     */
    private void abortBatches() {
        abortBatches(new KafkaException("Producer is closed forcefully."));
    }

    /**
     * 中止所有不完整的批次(无论它们是否已发送)
     */
    void abortBatches(final RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            synchronized (dq) {
                batch.abortRecordAppends();
                dq.remove(batch);
            }
            batch.abort(reason);
            deallocate(batch);
        }
    }

    /**
     * 中止任何未处理的批次
     */
    void abortUndrainedBatches(RuntimeException reason) {
        for (ProducerBatch batch : incomplete.copyAll()) {
            Deque<ProducerBatch> dq = getDeque(batch.topicPartition);
            boolean aborted = false;
            synchronized (dq) {
                if ((transactionManager != null && !batch.hasSequence()) || (transactionManager == null && !batch.isClosed())) {
                    aborted = true;
                    batch.abortRecordAppends();
                    dq.remove(batch);
                }
            }
            if (aborted) {
                batch.abort(reason);
                deallocate(batch);
            }
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * 关闭此累加器并强制清空所有记录缓冲区
     */
    public void close() {
        this.closed = true;
        this.free.close();
    }

    /*
     * 关于刚刚附加到记录累加器的记录的元数据
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;
        public final boolean abortForNewBatch;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated, boolean abortForNewBatch) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
            this.abortForNewBatch = abortForNewBatch;
        }
    }

    /*
     * 在累加器中至少有一个完整记录批的节点集
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
}
