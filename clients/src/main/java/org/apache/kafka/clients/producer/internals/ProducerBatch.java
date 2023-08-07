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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 *  正在发送或将要发送的一批记录。这个类不是线程安全的，在修改它时必须使用外部同步
 */
public final class ProducerBatch {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    private enum FinalState { ABORTED, FAILED, SUCCEEDED }

    final long createdMs;
    final TopicPartition topicPartition;
    final ProduceRequestResult produceFuture;

    private final List<Thunk> thunks = new ArrayList<>();
    private final MemoryRecordsBuilder recordsBuilder;
    private final AtomicInteger attempts = new AtomicInteger(0);
    private final boolean isSplitBatch;
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);

    int recordCount;
    int maxRecordSize;
    private long lastAttemptMs;
    private long lastAppendTime;
    private long drainedMs;
    private boolean retry;
    private boolean reopened;

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        this(tp, recordsBuilder, createdMs, false);
    }

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
        this.createdMs = createdMs;
        this.lastAttemptMs = createdMs;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.retry = false;
        this.isSplitBatch = isSplitBatch;
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                                                                                recordsBuilder.compressionType());
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * 将记录追加到当前记录集中，并返回该记录集中的相对偏移量
     *
     * @return 与此记录对应的RecordSend，如果没有足够的空间，则为null。
     */
    public FutureRecordMetadata     tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            this.lastAppendTime = now;
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length,
                                                                   Time.SYSTEM);
            // 我们必须保留每个未来返回给用户，以防需要将批拆分为几个新批并重新发送。
            thunks.add(new Thunk(callback, future));
            this.recordCount++;
            return future;
        }
    }

    /**
     *  此方法仅在将大批拆分为小批时使用{@link #split(int)}。
     * @return 如果记录已成功追加，则为True，否则为false。
     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return false;
        } else {
            // 不需要得到CRC。
            this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp,
                                                                   key == null ? -1 : key.remaining(),
                                                                   value == null ? -1 : value.remaining(),
                                                                   Time.SYSTEM);
            // Chain the future to the original thunk.
            thunk.future.chain(future);
            this.thunks.add(thunk);
            this.recordCount++;
            return true;
        }
    }

    /**
     * 中止批处理并完成future和回调。
     *
     * @param exception 用于完成未来和等待回调的异常。
     */
    public void abort(RuntimeException exception) {
        if (!finalState.compareAndSet(null, FinalState.ABORTED))
            throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());

        log.trace("Aborting batch for partition {}", topicPartition, exception);
        completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, index -> exception);
    }

    /**
     * 检查批处理是否已完成(成功或异常)。
     * @return 如果批处理已完成，则为true，否则为false。
     */
    public boolean isDone() {
        return finalState() != null;
    }

    /**
     *  成功完成批处理。
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @return true if the batch was completed as a result of this call, and false
     *   if it had been completed previously
     */
    public boolean complete(long baseOffset, long logAppendTime) {
        return done(baseOffset, logAppendTime, null, null);
    }

    /**
     *  异常完成批处理。所提供的顶级异常将用于批处理中包含的每个记录的未来。
     *
     * @param topLevelException top-level partition error
     * @param recordExceptions Record exception function mapping batchIndex to the respective record exception
     * @return true if the batch was completed as a result of this call, and false
     *   if it had been completed previously
     */
    public boolean completeExceptionally(
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        Objects.requireNonNull(topLevelException);
        Objects.requireNonNull(recordExceptions);
        return done(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, topLevelException, recordExceptions);
    }

    /**
     * 完成批处理的状态。最终状态一旦设置，就是不可变的。这个函数可以在批处理中调用一次或两次。它可以被称为两次if
     * 1. 在收到代理的响应之前，飞行批处理将过期。批处理的最终状态设置为FAILED。但它可能在代理上成功，而第二次执行batch.done()可能成功
     * 尝试设置成功的最终状态。
     * 2. 如果发生事务流产，或者生产者被强制关闭，则最终状态为ABORTED，但如果broker响应成功，则仍然可以成功。从[FAILED | ABORTED]到>
     * SUCCEEDED的尝试转换将被记录。从一个失败状态到相同或不同失败状态的尝试转换将被忽略。尝试从成功状态转换到相同状态或失败状态将引发异常。
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param topLevelException The exception that occurred (or null if the request was successful)
     * @param recordExceptions Record exception function mapping batchIndex to the respective record exception
     * @return true if the batch was completed successfully and false if the batch was previously aborted
     */
    private boolean done(
        long baseOffset,
        long logAppendTime,
        RuntimeException topLevelException,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        final FinalState tryFinalState = (topLevelException == null) ? FinalState.SUCCEEDED : FinalState.FAILED;
        if (tryFinalState == FinalState.SUCCEEDED) {
            log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
        } else {
            log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, topLevelException);
        }

        if (this.finalState.compareAndSet(null, tryFinalState)) {
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, recordExceptions);
            return true;
        }

        if (this.finalState.get() != FinalState.SUCCEEDED) {
            if (tryFinalState == FinalState.SUCCEEDED) {
                // 如果先前不成功的批处理稍后成功，则记录日志。
                log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
                    tryFinalState, topicPartition, baseOffset, this.finalState.get());
            } else {
                // FAILED -> FAILED和ABORTED -> FAILED转换将被忽略。
                log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
                    this.finalState.get(), tryFinalState, topicPartition, baseOffset);
            }
        } else {
            //  成功的批处理不能再尝试另一次状态更改。
            throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
        }
        return false;
    }

    private void completeFutureAndFireCallbacks(
        long baseOffset,
        long logAppendTime,
        Function<Integer, RuntimeException> recordExceptions
    ) {
        // 在调用回调之前设置future，因为我们依赖于' onCompletion '调用的状态
        produceFuture.set(baseOffset, logAppendTime, recordExceptions);

        // 执行回调函数
        for (int i = 0; i < thunks.size(); i++) {
            try {
                Thunk thunk = thunks.get(i);
                if (thunk.callback != null) {
                    if (recordExceptions == null) {
                        RecordMetadata metadata = thunk.future.value();
                        thunk.callback.onCompletion(metadata, null);
                    } else {
                        RuntimeException exception = recordExceptions.apply(i);
                        thunk.callback.onCompletion(null, exception);
                    }
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        produceFuture.done();
    }

    public Deque<ProducerBatch> split(int splitBatchSize) {
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        MemoryRecords memoryRecords = recordsBuilder.build();

        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
        if (!recordBatchIter.hasNext())
            throw new IllegalStateException("Cannot split an empty producer batch.");

        RecordBatch recordBatch = recordBatchIter.next();
        if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed())
            throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
                    "with version v0 and v1");

        if (recordBatchIter.hasNext())
            throw new IllegalArgumentException("A producer batch should only have one record batch.");

        Iterator<Thunk> thunkIter = thunks.iterator();
        // 我们总是分配批大小，因为我们已经分割了一个大的批。我们还保留了原始批的创建时间。
        ProducerBatch batch = null;

        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            Thunk thunk = thunkIter.next();
            if (batch == null)
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);

            // 新创建的批处理总是可以承载第一条消息。
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                batches.add(batch);
                batch.closeForRecordAppends();
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }

        // 关闭最后一个批处理，并在拆分后将其添加到批处理列表中。
        if (batch != null) {
            batches.add(batch);
            batch.closeForRecordAppends();
        }

        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, index -> new RecordBatchTooLargeException());
        produceFuture.done();

        if (hasSequence()) {
            int sequence = baseSequence();
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
            for (ProducerBatch newBatch : batches) {
                newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
                sequence += newBatch.recordCount;
            }
        }
        return batches;
    }

    private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
        int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                recordsBuilder.compressionType(), record.key(), record.value(), record.headers()), batchSize);
        ByteBuffer buffer = ByteBuffer.allocate(initialSize);

        // 注意，我们有意不为新创建的批设置生产者状态(producerId、epoch、sequence和is Transactional)。
        // 这将在将批处理从队列中取出以进行发送时设置(这与处理普通批处理的方式一致)。
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compressionType(),
                TimestampType.CREATE_TIME, 0L);
        return new ProducerBatch(topicPartition, builder, this.createdMs, true);
    }

    public boolean isCompressed() {
        return recordsBuilder.compressionType() != CompressionType.NONE;
    }

    /**
     * 一个回调函数和关联的FutureRecordMetadata参数传递给它。
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
        return deliveryTimeoutMs <= now - this.createdMs;
    }

    public FinalState finalState() {
        return this.finalState.get();
    }

    int attempts() {
        return attempts.get();
    }

    void reenqueued(long now) {
        attempts.getAndIncrement();
        lastAttemptMs = Math.max(lastAppendTime, now);
        lastAppendTime = Math.max(lastAppendTime, now);
        retry = true;
    }

    long queueTimeMs() {
        return drainedMs - createdMs;
    }

    long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * 如果批处理被重试发送到kafka返回
     */
    public boolean inRetry() {
        return this.retry;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                this.baseSequence(), this.topicPartition, baseSequence);
        reopened = true;
        recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    /**
     * 释放附加记录所需的资源(例如压缩缓冲区)。一旦调用了这个方法，就只能更新RecordBatch头。
     */
    public void closeForRecordAppends() {
        recordsBuilder.closeForRecordAppends();
    }

    public void close() {
        recordsBuilder.close();
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                                                       recordsBuilder.compressionType(),
                                                       (float) recordsBuilder.compressionRatio());
        }
        reopened = false;
    }

    /**
     * 中止记录生成器并重置底层缓冲区的状态。这是在流产之前使用的使用{@link #abort(RuntimeException)}进行批处理，并确保之前添加的记录不能被删除
     * 读。这在我们希望确保批处理最终被终止的场景中使用，但是在其中调用完成回调是不安全的(例如，因为我们持有一个锁，如当在{@link RecordAccumulator})中终止批处理时。
     */
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }

    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    public byte magic() {
        return recordsBuilder.magic();
    }

    public long producerId() {
        return recordsBuilder.producerId();
    }

    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }

    public int baseSequence() {
        return recordsBuilder.baseSequence();
    }

    public int lastSequence() {
        return recordsBuilder.baseSequence() + recordsBuilder.numRecords() - 1;
    }

    public boolean hasSequence() {
        return baseSequence() != RecordBatch.NO_SEQUENCE;
    }

    public boolean isTransactional() {
        return recordsBuilder.isTransactional();
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }
}
