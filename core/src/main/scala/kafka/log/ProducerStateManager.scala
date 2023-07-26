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
package kafka.log

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, NoSuchFileException, StandardOpenOption}
import java.util.concurrent.ConcurrentSkipListMap
import kafka.log.Log.offsetFromFile
import kafka.server.LogOffsetMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe, threadsafe}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.{ControlRecordType, DefaultRecordBatch, EndTransactionMarker, RecordBatch}
import org.apache.kafka.common.utils.{ByteUtils, Crc32C, Time, Utils}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

class CorruptSnapshotException(msg: String) extends KafkaException(msg)

/**
 * 某一制作人的最后书面记录。如果生产者的唯一日志条目是事务标记，则最后的数据偏移量可能未定义。
 */
case class LastRecord(lastDataOffset: Option[Long], producerEpoch: Short)


private[log] case class TxnMetadata(
  producerId: Long,
  firstOffset: LogOffsetMetadata,
  var lastOffset: Option[Long] = None
) {
  def this(producerId: Long, firstOffset: Long) = this(producerId, LogOffsetMetadata(firstOffset))

  override def toString: String = {
    "TxnMetadata(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset)"
  }
}

private[log] object ProducerStateEntry {
  private[log] val NumBatchesToRetain = 5

  def empty(producerId: Long) = new ProducerStateEntry(producerId,
    batchMetadata = mutable.Queue[BatchMetadata](),
    producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
    coordinatorEpoch = -1,
    lastTimestamp = RecordBatch.NO_TIMESTAMP,
    currentTxnFirstOffset = None)
}

private[log] case class BatchMetadata(lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) {
  def firstSeq: Int =  DefaultRecordBatch.decrementSequence(lastSeq, offsetDelta)
  def firstOffset: Long = lastOffset - offsetDelta

  override def toString: String = {
    "BatchMetadata(" +
      s"firstSeq=$firstSeq, " +
      s"lastSeq=$lastSeq, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"timestamp=$timestamp)"
  }
}

// 对batchMetadata进行排序，以便具有最低序列的批位于队列的头部，而序列最高的批位于队列的尾部。我们将最多保留ProducerStateEntry。
// NumBatchesToRetain队列中的元素。当队列达到容量时，我们删除第一个元素，为进入的批腾出空间。
private[log] class ProducerStateEntry(val producerId: Long,
                                      val batchMetadata: mutable.Queue[BatchMetadata],
                                      var producerEpoch: Short,
                                      var coordinatorEpoch: Int,
                                      var lastTimestamp: Long,
                                      var currentTxnFirstOffset: Option[Long]) {

  def firstSeq: Int = if (isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.front.firstSeq

  def firstDataOffset: Long = if (isEmpty) -1L else batchMetadata.front.firstOffset

  def lastSeq: Int = if (isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.last.lastSeq

  def lastDataOffset: Long = if (isEmpty) -1L else batchMetadata.last.lastOffset

  def lastOffsetDelta : Int = if (isEmpty) 0 else batchMetadata.last.offsetDelta

  def isEmpty: Boolean = batchMetadata.isEmpty

  def addBatch(producerEpoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long): Unit = {
    maybeUpdateProducerEpoch(producerEpoch)
    addBatchMetadata(BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp))
    this.lastTimestamp = timestamp
  }

  def maybeUpdateProducerEpoch(producerEpoch: Short): Boolean = {
    if (this.producerEpoch != producerEpoch) {
      batchMetadata.clear()
      this.producerEpoch = producerEpoch
      true
    } else {
      false
    }
  }

  private def addBatchMetadata(batch: BatchMetadata): Unit = {
    if (batchMetadata.size == ProducerStateEntry.NumBatchesToRetain)
      batchMetadata.dequeue()
    batchMetadata.enqueue(batch)
  }

  def update(nextEntry: ProducerStateEntry): Unit = {
    maybeUpdateProducerEpoch(nextEntry.producerEpoch)
    while (nextEntry.batchMetadata.nonEmpty)
      addBatchMetadata(nextEntry.batchMetadata.dequeue())
    this.coordinatorEpoch = nextEntry.coordinatorEpoch
    this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset
    this.lastTimestamp = nextEntry.lastTimestamp
  }

  def findDuplicateBatch(batch: RecordBatch): Option[BatchMetadata] = {
    if (batch.producerEpoch != producerEpoch)
       None
    else
      batchWithSequenceRange(batch.baseSequence, batch.lastSequence)
  }

  // 返回具有精确序列范围(如果有的话)的缓存批处理的批元数据。
  def batchWithSequenceRange(firstSeq: Int, lastSeq: Int): Option[BatchMetadata] = {
    val duplicate = batchMetadata.filter { metadata =>
      firstSeq == metadata.firstSeq && lastSeq == metadata.lastSeq
    }
    duplicate.headOption
  }

  override def toString: String = {
    "ProducerStateEntry(" +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"currentTxnFirstOffset=$currentTxnFirstOffset, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"lastTimestamp=$lastTimestamp, " +
      s"batchMetadata=$batchMetadata"
  }
}

/**
 * 该类用于在将给定生成器附加的记录写入日志之前对其进行验证。它在最后一次成功追加后使用生产者的状态初始化，
 * 并传递地验证每个新记录的序列号和epoch。此外，该类在验证传入记录时积累事务元数据。
 *
 * @param producerId 附加到日志的生产者的id
 * @param currentEntry  与生产者id关联的当前条目，其中包含生产者最近添加的固定数量的元数据。第一个传入的追加将根据当前条目中的最新追
 *                      加进行验证。新的追加项将取代当前条目中的旧追加项，因此空间开销是恒定的。
 * @param origin 指示追加的起始位置，这暗示了验证的范围。例如，来自组协调器的偏移提交没有序列号，因此只执行生产者纪元验证。
 *               通过复制产生的追加不会被验证(我们假设验证已经完成)，来自客户端的追加需要完全验证。
 */
private[log] class ProducerAppendInfo(val topicPartition: TopicPartition,
                                      val producerId: Long,
                                      val currentEntry: ProducerStateEntry,
                                      val origin: AppendOrigin) extends Logging {

  private val transactions = ListBuffer.empty[TxnMetadata]
  private val updatedEntry = ProducerStateEntry.empty(producerId)

  updatedEntry.producerEpoch = currentEntry.producerEpoch
  updatedEntry.coordinatorEpoch = currentEntry.coordinatorEpoch
  updatedEntry.lastTimestamp = currentEntry.lastTimestamp
  updatedEntry.currentTxnFirstOffset = currentEntry.currentTxnFirstOffset

  private def maybeValidateDataBatch(producerEpoch: Short, firstSeq: Int, offset: Long): Unit = {
    checkProducerEpoch(producerEpoch, offset)
    if (origin == AppendOrigin.Client) {
      checkSequence(producerEpoch, firstSeq, offset)
    }
  }

  private def checkProducerEpoch(producerEpoch: Short, offset: Long): Unit = {
    if (producerEpoch < updatedEntry.producerEpoch) {
      val message = s"Epoch of producer $producerId at offset $offset in $topicPartition is $producerEpoch, " +
        s"which is smaller than the last seen epoch ${updatedEntry.producerEpoch}"

      if (origin == AppendOrigin.Replication) {
        warn(message)
      } else {
        // 从2.7开始，我们将生产者发送响应回调中的ProducerFenced error替换为InvalidProducerEpoch，以区别于之前的致命异常，
        // 允许客户端中止正在进行的事务并重试。
        throw new InvalidProducerEpochException(message)
      }
    }
  }

  private def checkSequence(producerEpoch: Short, appendFirstSeq: Int, offset: Long): Unit = {
    if (producerEpoch != updatedEntry.producerEpoch) {
      if (appendFirstSeq != 0) {
        if (updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch of producer $producerId " +
            s"at offset $offset in partition $topicPartition: $producerEpoch (request epoch), $appendFirstSeq (seq. number), " +
            s"${updatedEntry.producerEpoch} (current producer epoch)")
        }
      }
    } else {
      val currentLastSeq = if (!updatedEntry.isEmpty)
        updatedEntry.lastSeq
      else if (producerEpoch == currentEntry.producerEpoch)
        currentEntry.lastSeq
      else
        RecordBatch.NO_SEQUENCE

      // 如果没有当前的生产者纪元(可能是因为由于保留或DeleteRecords API而删除了所有生产者记录)，则接受任何序列号的写入
      if (!(currentEntry.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH || inSequence(currentLastSeq, appendFirstSeq))) {
        throw new OutOfOrderSequenceException(s"Out of order sequence number for producer $producerId at " +
          s"offset $offset in partition $topicPartition: $appendFirstSeq (incoming seq. number), " +
          s"$currentLastSeq (current end sequence number)")
      }
    }
  }

  private def inSequence(lastSeq: Int, nextSeq: Int): Boolean = {
    nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Int.MaxValue)
  }

  def append(batch: RecordBatch, firstOffsetMetadataOpt: Option[LogOffsetMetadata]): Option[CompletedTxn] = {
    if (batch.isControlBatch) {
      val recordIterator = batch.iterator
      if (recordIterator.hasNext) {
        val record = recordIterator.next()
        val endTxnMarker = EndTransactionMarker.deserialize(record)
        appendEndTxnMarker(endTxnMarker, batch.producerEpoch, batch.baseOffset, record.timestamp)
      } else {
        // 空的控制批意味着整个事务已从日志中清除，因此不需要追加
        None
      }
    } else {
      val firstOffsetMetadata = firstOffsetMetadataOpt.getOrElse(LogOffsetMetadata(batch.baseOffset))
      appendDataBatch(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp,
        firstOffsetMetadata, batch.lastOffset, batch.isTransactional)
      None
    }
  }

  def appendDataBatch(epoch: Short,
                      firstSeq: Int,
                      lastSeq: Int,
                      lastTimestamp: Long,
                      firstOffsetMetadata: LogOffsetMetadata,
                      lastOffset: Long,
                      isTransactional: Boolean): Unit = {
    val firstOffset = firstOffsetMetadata.messageOffset
    maybeValidateDataBatch(epoch, firstSeq, firstOffset)
    updatedEntry.addBatch(epoch, lastSeq, lastOffset, (lastOffset - firstOffset).toInt, lastTimestamp)

    updatedEntry.currentTxnFirstOffset match {
      case Some(_) if !isTransactional =>
        // 在事务处于活动状态时收到非事务消息
        throw new InvalidTxnStateException(s"Expected transactional write from producer $producerId at " +
          s"offset $firstOffsetMetadata in partition $topicPartition")

      case None if isTransactional =>
        // Began a new transaction
        updatedEntry.currentTxnFirstOffset = Some(firstOffset)
        transactions += TxnMetadata(producerId, firstOffsetMetadata)

      case _ => // nothing to do
    }
  }

  private def checkCoordinatorEpoch(endTxnMarker: EndTransactionMarker, offset: Long): Unit = {
    if (updatedEntry.coordinatorEpoch > endTxnMarker.coordinatorEpoch) {
      if (origin == AppendOrigin.Replication) {
        info(s"Detected invalid coordinator epoch for producerId $producerId at " +
          s"offset $offset in partition $topicPartition: ${endTxnMarker.coordinatorEpoch} " +
          s"is older than previously known coordinator epoch ${updatedEntry.coordinatorEpoch}")
      } else {
        throw new TransactionCoordinatorFencedException(s"Invalid coordinator epoch for producerId $producerId at " +
          s"offset $offset in partition $topicPartition: ${endTxnMarker.coordinatorEpoch} " +
          s"(zombie), ${updatedEntry.coordinatorEpoch} (current)")
      }
    }
  }

  def appendEndTxnMarker(
    endTxnMarker: EndTransactionMarker,
    producerEpoch: Short,
    offset: Long,
    timestamp: Long
  ): Option[CompletedTxn] = {
    checkProducerEpoch(producerEpoch, offset)
    checkCoordinatorEpoch(endTxnMarker, offset)

    // 只对非空事务发出' CompletedTxn '。没有任何关联数据的事务标记不会对最后一个稳定偏移量产生任何影响，也不需要反映在事务索引中。
    val completedTxn = updatedEntry.currentTxnFirstOffset.map { firstOffset =>
      CompletedTxn(producerId, firstOffset, offset, endTxnMarker.controlType == ControlRecordType.ABORT)
    }

    updatedEntry.maybeUpdateProducerEpoch(producerEpoch)
    updatedEntry.currentTxnFirstOffset = None
    updatedEntry.coordinatorEpoch = endTxnMarker.coordinatorEpoch
    updatedEntry.lastTimestamp = timestamp

    completedTxn
  }

  def toEntry: ProducerStateEntry = updatedEntry

  def startedTransactions: List[TxnMetadata] = transactions.toList

  override def toString: String = {
    "ProducerAppendInfo(" +
      s"producerId=$producerId, " +
      s"producerEpoch=${updatedEntry.producerEpoch}, " +
      s"firstSequence=${updatedEntry.firstSeq}, " +
      s"lastSequence=${updatedEntry.lastSeq}, " +
      s"currentTxnFirstOffset=${updatedEntry.currentTxnFirstOffset}, " +
      s"coordinatorEpoch=${updatedEntry.coordinatorEpoch}, " +
      s"lastTimestamp=${updatedEntry.lastTimestamp}, " +
      s"startedTransactions=$transactions)"
  }
}

object ProducerStateManager {
  private val ProducerSnapshotVersion: Short = 1
  private val VersionField = "version"
  private val CrcField = "crc"
  private val ProducerIdField = "producer_id"
  private val LastSequenceField = "last_sequence"
  private val ProducerEpochField = "epoch"
  private val LastOffsetField = "last_offset"
  private val OffsetDeltaField = "offset_delta"
  private val TimestampField = "timestamp"
  private val ProducerEntriesField = "producer_entries"
  private val CoordinatorEpochField = "coordinator_epoch"
  private val CurrentTxnFirstOffsetField = "current_txn_first_offset"

  private val VersionOffset = 0
  private val CrcOffset = VersionOffset + 2
  private val ProducerEntriesOffset = CrcOffset + 4

  val ProducerSnapshotEntrySchema = new Schema(
    new Field(ProducerIdField, Type.INT64, "The producer ID"),
    new Field(ProducerEpochField, Type.INT16, "Current epoch of the producer"),
    new Field(LastSequenceField, Type.INT32, "Last written sequence of the producer"),
    new Field(LastOffsetField, Type.INT64, "Last written offset of the producer"),
    new Field(OffsetDeltaField, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
    new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"),
    new Field(CoordinatorEpochField, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
    new Field(CurrentTxnFirstOffsetField, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"))
  val PidSnapshotMapSchema = new Schema(
    new Field(VersionField, Type.INT16, "Version of the snapshot file"),
    new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
    new Field(ProducerEntriesField, new ArrayOf(ProducerSnapshotEntrySchema), "The entries in the producer table"))

  def readSnapshot(file: File): Iterable[ProducerStateEntry] = {
    try {
      val buffer = Files.readAllBytes(file.toPath)
      val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer))

      val version = struct.getShort(VersionField)
      if (version != ProducerSnapshotVersion)
        throw new CorruptSnapshotException(s"Snapshot contained an unknown file version $version")

      val crc = struct.getUnsignedInt(CrcField)
      val computedCrc =  Crc32C.compute(buffer, ProducerEntriesOffset, buffer.length - ProducerEntriesOffset)
      if (crc != computedCrc)
        throw new CorruptSnapshotException(s"Snapshot is corrupt (CRC is no longer valid). " +
          s"Stored crc: $crc. Computed crc: $computedCrc")

      struct.getArray(ProducerEntriesField).map { producerEntryObj =>
        val producerEntryStruct = producerEntryObj.asInstanceOf[Struct]
        val producerId = producerEntryStruct.getLong(ProducerIdField)
        val producerEpoch = producerEntryStruct.getShort(ProducerEpochField)
        val seq = producerEntryStruct.getInt(LastSequenceField)
        val offset = producerEntryStruct.getLong(LastOffsetField)
        val timestamp = producerEntryStruct.getLong(TimestampField)
        val offsetDelta = producerEntryStruct.getInt(OffsetDeltaField)
        val coordinatorEpoch = producerEntryStruct.getInt(CoordinatorEpochField)
        val currentTxnFirstOffset = producerEntryStruct.getLong(CurrentTxnFirstOffsetField)
        val lastAppendedDataBatches = mutable.Queue.empty[BatchMetadata]
        if (offset >= 0)
          lastAppendedDataBatches += BatchMetadata(seq, offset, offsetDelta, timestamp)

        val newEntry = new ProducerStateEntry(producerId, lastAppendedDataBatches, producerEpoch,
          coordinatorEpoch, timestamp, if (currentTxnFirstOffset >= 0) Some(currentTxnFirstOffset) else None)
        newEntry
      }
    } catch {
      case e: SchemaException =>
        throw new CorruptSnapshotException(s"Snapshot failed schema validation: ${e.getMessage}")
    }
  }

  private def writeSnapshot(file: File, entries: mutable.Map[Long, ProducerStateEntry]): Unit = {
    val struct = new Struct(PidSnapshotMapSchema)
    struct.set(VersionField, ProducerSnapshotVersion)
    struct.set(CrcField, 0L) // we'll fill this after writing the entries
    val entriesArray = entries.map {
      case (producerId, entry) =>
        val producerEntryStruct = struct.instance(ProducerEntriesField)
        producerEntryStruct.set(ProducerIdField, producerId)
          .set(ProducerEpochField, entry.producerEpoch)
          .set(LastSequenceField, entry.lastSeq)
          .set(LastOffsetField, entry.lastDataOffset)
          .set(OffsetDeltaField, entry.lastOffsetDelta)
          .set(TimestampField, entry.lastTimestamp)
          .set(CoordinatorEpochField, entry.coordinatorEpoch)
          .set(CurrentTxnFirstOffsetField, entry.currentTxnFirstOffset.getOrElse(-1L))
        producerEntryStruct
    }.toArray
    struct.set(ProducerEntriesField, entriesArray)

    val buffer = ByteBuffer.allocate(struct.sizeOf)
    struct.writeTo(buffer)
    buffer.flip()

    // now fill in the CRC
    val crc = Crc32C.compute(buffer, ProducerEntriesOffset, buffer.limit() - ProducerEntriesOffset)
    ByteUtils.writeUnsignedInt(buffer, CrcOffset, crc)

    val fileChannel = FileChannel.open(file.toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    try {
      fileChannel.write(buffer)
      fileChannel.force(true)
    } finally {
      fileChannel.close()
    }
  }

  private def isSnapshotFile(file: File): Boolean = file.getName.endsWith(Log.ProducerSnapshotFileSuffix)

  // visible for testing
  private[log] def listSnapshotFiles(dir: File): Seq[SnapshotFile] = {
    if (dir.exists && dir.isDirectory) {
      Option(dir.listFiles).map { files =>
        files.filter(f => f.isFile && isSnapshotFile(f)).map(SnapshotFile(_)).toSeq
      }.getOrElse(Seq.empty)
    } else Seq.empty
  }
}

/**
 * 维护一个从ProducerIds到元数据的映射，关于最后添加的条目(例如纪元，序列号，最后偏移量等)。
 *
 * 序列号是为给定标识符成功追加到分区的最后一个数字。epoch是用来防御僵尸作家的。偏移量是追加到分区的最后一条成功消息之一。
 *
 * 只要映射中包含生产者id，对应的生产者就可以继续写数据。但是，生产者id可能由于最近没有使用或最后一个书面条目已从日志中删除而过期
 * (例如，如果保留策略为“delete”)。对于压缩主题，日志清理器将确保来自给定生产者id的最新条目保留在日志中，前提是它没有因年龄而过期。
 * 这确保生产者id在达到最大过期时间之前不会过期，或者如果主题也配置为删除，则包含最后写入偏移量的段已被删除。
 */
@nonthreadsafe
class ProducerStateManager(val topicPartition: TopicPartition,
                           @volatile var _logDir: File,
                           val maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                           val time: Time = Time.SYSTEM) extends Logging {
  import ProducerStateManager._
  import java.util

  this.logIdent = s"[ProducerStateManager partition=$topicPartition] "

  private var snapshots: ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = locally {
    loadSnapshots()
  }

  private val producers = mutable.Map.empty[Long, ProducerStateEntry]
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  // 按事务的第一个偏移量排序的正在进行的事务
  private val ongoingTxns = new util.TreeMap[Long, TxnMetadata]

  // 已完成的交易，其标记位于高水位之上的偏移量
  private val unreplicatedTxns = new util.TreeMap[Long, TxnMetadata]

  /**
   * 通过扫描_logDir加载生产者状态快照。
   */
  private def loadSnapshots(): ConcurrentSkipListMap[java.lang.Long, SnapshotFile] = {
    val tm = new ConcurrentSkipListMap[java.lang.Long, SnapshotFile]()
    for (f <- listSnapshotFiles(_logDir)) {
      tm.put(f.offset, f)
    }
    tm
  }

  /**
   * 扫描日志目录，收集所有生产者状态快照文件。快照文件没有对应于segmentBaseOffsets中提供的偏移量之一的偏移量将被删除，
   * 除非快照文件的偏移量高于segmentBaseOffsets中的任何偏移量。
   *
   * 这里的目标是删除任何没有关联段文件的快照文件，但不删除在干净关机期间发出的最大的随机快照文件。
   */
  private[log] def removeStraySnapshots(segmentBaseOffsets: Seq[Long]): Unit = {
    val maxSegmentBaseOffset = if (segmentBaseOffsets.isEmpty) None else Some(segmentBaseOffsets.max)
    val baseOffsets = segmentBaseOffsets.toSet
    var latestStraySnapshot: Option[SnapshotFile] = None

    val ss = loadSnapshots()
    for (snapshot <- ss.values().asScala) {
      val key = snapshot.offset
      latestStraySnapshot match {
        case Some(prev) =>
          if (!baseOffsets.contains(key)) {
            // 这个快照现在是最大的散列快照。
            prev.deleteIfExists()
            ss.remove(prev.offset)
            latestStraySnapshot = Some(snapshot)
          }
        case None =>
          if (!baseOffsets.contains(key)) {
            latestStraySnapshot = Some(snapshot)
          }
      }
    }

    // 检查latestStraySnapshot是否大于最大段基数偏移量，如果不是，则删除largestStraySnapshot。
    for (strayOffset <- latestStraySnapshot.map(_.offset); maxOffset <- maxSegmentBaseOffset) {
      if (strayOffset < maxOffset) {
        Option(ss.remove(strayOffset)).foreach(_.deleteIfExists())
      }
    }

    this.snapshots = ss
  }

  /**
   * 一个不稳定的偏移量是一个未确定的(即它的最终结果还不知道)，或者是一个已经确定的，但可能没有被复制的(即任何事务，
   * 其COMMIT/ABORT标记写在比当前高水位更高的偏移量)。
   */
  def firstUnstableOffset: Option[LogOffsetMetadata] = {
    val unreplicatedFirstOffset = Option(unreplicatedTxns.firstEntry).map(_.getValue.firstOffset)
    val undecidedFirstOffset = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset)
    if (unreplicatedFirstOffset.isEmpty)
      undecidedFirstOffset
    else if (undecidedFirstOffset.isEmpty)
      unreplicatedFirstOffset
    else if (undecidedFirstOffset.get.messageOffset < unreplicatedFirstOffset.get.messageOffset)
      undecidedFirstOffset
    else
      unreplicatedFirstOffset
  }

  /**
   * 确认在给定的抵消之前完成的所有交易。这允许LSO推进到下一个不稳定偏移量。
   */
  def onHighWatermarkUpdated(highWatermark: Long): Unit = {
    removeUnreplicatedTransactions(highWatermark)
  }

  /**
   * 第一个未确定偏移量是尚未提交或中止的最早的事务消息。不像[[firstUnstableOffset]],
   * 这并不反映复制的状态(即，已完成的事务标记是否超出高水位)。
   */
  private[log] def firstUndecidedOffset: Option[Long] = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset.messageOffset)

  /**
   * Returns the last offset of this map
   */
  def mapEndOffset: Long = lastMapOffset

  /**
   * Get a copy of the active producers
   */
  def activeProducers: immutable.Map[Long, ProducerStateEntry] = producers.toMap

  def isEmpty: Boolean = producers.isEmpty && unreplicatedTxns.isEmpty

  private def loadFromSnapshot(logStartOffset: Long, currentTime: Long): Unit = {
    while (true) {
      latestSnapshotFile match {
        case Some(snapshot) =>
          try {
            info(s"Loading producer state from snapshot file '$snapshot'")
            val loadedProducers = readSnapshot(snapshot.file).filter { producerEntry => !isProducerExpired(currentTime, producerEntry) }
            loadedProducers.foreach(loadProducerEntry)
            lastSnapOffset = snapshot.offset
            lastMapOffset = lastSnapOffset
            return
          } catch {
            case e: CorruptSnapshotException =>
              warn(s"Failed to load producer snapshot from '${snapshot.file}': ${e.getMessage}")
              removeAndDeleteSnapshot(snapshot.offset)
          }
        case None =>
          lastSnapOffset = logStartOffset
          lastMapOffset = logStartOffset
          return
      }
    }
  }

  // visible for testing
  private[log] def loadProducerEntry(entry: ProducerStateEntry): Unit = {
    val producerId = entry.producerId
    producers.put(producerId, entry)
    entry.currentTxnFirstOffset.foreach { offset =>
      ongoingTxns.put(offset, new TxnMetadata(producerId, offset))
    }
  }

  private def isProducerExpired(currentTimeMs: Long, producerState: ProducerStateEntry): Boolean =
    producerState.currentTxnFirstOffset.isEmpty && currentTimeMs - producerState.lastTimestamp >= maxProducerIdExpirationMs

  /**
   * 过期任何闲置时间超过配置的最大过期超时的生产者id。
   */
  def removeExpiredProducers(currentTimeMs: Long): Unit = {
    producers --= producers.filter { case (_, lastEntry) => isProducerExpired(currentTimeMs, lastEntry) }.keySet
  }

  /**
   * 截断生产者id映射到给定偏移范围，并从范围内最近的快照(如果有)重新加载条目。我们删除logStartOffset之前的快照文件，
   * 但不从映射中删除生产者状态。这意味着内存状态和磁盘状态可能会出现分歧，在代理故障转移或不干净关机的情况下，
   * 任何未在快照中持久化的内存状态都将丢失，这将导致UNKNOWN_PRODUCER_ID错误。请注意，假设日志端的偏移量小于或等于高水位。
   */
  def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long): Unit = {
    // remove all out of range snapshots
    snapshots.values().asScala.foreach { snapshot =>
      if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
        removeAndDeleteSnapshot(snapshot.offset)
      }
    }

    if (logEndOffset != mapEndOffset) {
      producers.clear()
      ongoingTxns.clear()

      // 由于我们假设偏移量小于或等于高水位，因此清除未复制的事务是安全的
      unreplicatedTxns.clear()
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      onLogStartOffsetIncremented(logStartOffset)
    }
  }

  def prepareUpdate(producerId: Long, origin: AppendOrigin): ProducerAppendInfo = {
    val currentEntry = lastEntry(producerId).getOrElse(ProducerStateEntry.empty(producerId))
    new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin)
  }

  /**
   * 使用给定的追加信息更新映射
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException(s"Invalid producer id ${appendInfo.producerId} passed to update " +
        s"for partition $topicPartition")

    trace(s"Updated producer ${appendInfo.producerId} state to $appendInfo")
    val updatedEntry = appendInfo.toEntry
    producers.get(appendInfo.producerId) match {
      case Some(currentEntry) =>
        currentEntry.update(updatedEntry)

      case None =>
        producers.put(appendInfo.producerId, updatedEntry)
    }

    appendInfo.startedTransactions.foreach { txn =>
      ongoingTxns.put(txn.firstOffset.messageOffset, txn)
    }
  }

  def updateMapEndOffset(lastOffset: Long): Unit = {
    lastMapOffset = lastOffset
  }

  /**
   * 获取给定生产者id的最后一个写入条目。
   */
  def lastEntry(producerId: Long): Option[ProducerStateEntry] = producers.get(producerId)

  /**
   * 如果当前结束偏移量不存在，则在当前结束偏移量处快照。
   */
  def takeSnapshot(): Unit = {
    // 如果不是新的偏移量，那么就不值得再拍一次快照
    if (lastMapOffset > lastSnapOffset) {
      val snapshotFile = SnapshotFile(Log.producerSnapshotFile(_logDir, lastMapOffset))
      val start = time.hiResClockMs()
      writeSnapshot(snapshotFile.file, producers)
      info(s"Wrote producer snapshot at offset $lastMapOffset with ${producers.size} producer ids in ${time.hiResClockMs() - start} ms.")

      snapshots.put(snapshotFile.offset, snapshotFile)

      // 根据序列化映射更新最后一个快照偏移量
      lastSnapOffset = lastMapOffset
    }
  }

  /**
   * 更新这个ProducerStateManager和它管理的所有快照文件的parentDir。
   */
  def updateParentDir(parentDir: File): Unit = {
    _logDir = parentDir
    snapshots.forEach((_, s) => s.updateParentDir(parentDir))
  }

  /**
   * 获取最新快照文件的最后偏移量(不包含)。
   */
  def latestSnapshotOffset: Option[Long] = latestSnapshotFile.map(_.offset)

  /**
   * Get the last offset (exclusive) of the oldest snapshot file.
   */
  def oldestSnapshotOffset: Option[Long] = oldestSnapshotFile.map(_.offset)

  /**
   * Visible for testing
   */
  private[log] def snapshotFileForOffset(offset: Long): Option[SnapshotFile] = {
    Option(snapshots.get(offset))
  }

  /**
   * 删除任何低于提供的logStartOffset的未复制事务，并在必要时将lastMapOffset提前。
   */
  def onLogStartOffsetIncremented(logStartOffset: Long): Unit = {
    removeUnreplicatedTransactions(logStartOffset)

    if (lastMapOffset < logStartOffset)
      lastMapOffset = logStartOffset

    lastSnapOffset = latestSnapshotOffset.getOrElse(logStartOffset)
  }

  private def removeUnreplicatedTransactions(offset: Long): Unit = {
    val iterator = unreplicatedTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      val lastOffset = txnEntry.getValue.lastOffset
      if (lastOffset.exists(_ < offset))
        iterator.remove()
    }
  }

  /**
   * 截断生产者id映射，并删除所有快照。这将重置映射的状态。
   */
  def truncateFullyAndStartAt(offset: Long): Unit = {
    producers.clear()
    ongoingTxns.clear()
    unreplicatedTxns.clear()
    snapshots.values().asScala.foreach { snapshot =>
      removeAndDeleteSnapshot(snapshot.offset)
    }
    lastSnapOffset = 0L
    lastMapOffset = offset
  }

  /**
   * 计算已完成事务的最后稳定偏移量，但尚未将事务标记为已完成。这将在下面的' completeTxn '中完成。
   * 这用于计算将追加到事务索引的LSO，但是只有在成功追加到索引之后才能完成。
   */
  def lastStableOffset(completedTxn: CompletedTxn): Long = {
    val nextIncompleteTxn = ongoingTxns.values.asScala.find(_.producerId != completedTxn.producerId)
    nextIncompleteTxn.map(_.firstOffset.messageOffset).getOrElse(completedTxn.lastOffset  + 1)
  }

  /**
   * 将事务标记为已完成。在推进第一个不稳定偏移之前，我们仍将等待高水位的推进。
   */
  def completeTxn(completedTxn: CompletedTxn): Unit = {
    val txnMetadata = ongoingTxns.remove(completedTxn.firstOffset)
    if (txnMetadata == null)
      throw new IllegalArgumentException(s"Attempted to complete transaction $completedTxn on partition $topicPartition " +
        s"which was not started")

    txnMetadata.lastOffset = Some(completedTxn.lastOffset)
    unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata)
  }

  @threadsafe
  def deleteSnapshotsBefore(offset: Long): Unit = {
    snapshots.subMap(0, offset).values().asScala.foreach { snapshot =>
      removeAndDeleteSnapshot(snapshot.offset)
    }
  }

  private def oldestSnapshotFile: Option[SnapshotFile] = {
    Option(snapshots.firstEntry()).map(_.getValue)
  }

  private def latestSnapshotFile: Option[SnapshotFile] = {
    Option(snapshots.lastEntry()).map(_.getValue)
  }

  /**
   * 删除与提供的偏移量对应的生产者状态快照文件元数据(如果它存在于此ProducerStateManager中)，并删除备用快照文件
   */
  private def removeAndDeleteSnapshot(snapshotOffset: Long): Unit = {
    Option(snapshots.remove(snapshotOffset)).foreach(_.deleteIfExists())
  }

  /**
   * 删除与提供的偏移量相对应的生产者状态快照文件元数据，如果它存在于此ProducerStateManager中，并将备用快照文件重命名为具有
   * Log.DeletionSuffix。注意:此方法用于异步删除是安全的。如果发生了争用，并且在这个ProducerStateManager实例不知道的情况
   * 下删除了快照文件，那么在SnapshotFile重命名时产生的异常将被忽略，并返回None。
   */
  private[log] def removeAndMarkSnapshotForDeletion(snapshotOffset: Long): Option[SnapshotFile] = {
    Option(snapshots.remove(snapshotOffset)).flatMap { snapshot => {
      // 如果不能重命名该文件，则可能意味着该文件已被删除。这可能是由于我们在日志恢复期间构造中间生产者状态管理器的方式，
      // 并在创建“真正的”生产者状态管理器之前使用它来发出删除操作。
      //
      // 在任何情况下，removeAndMarkSnapshotForDeletion都是用来删除快照文件的，所以忽略这里的异常仅仅意味着预期的操作已经完成。
      try {
        snapshot.renameTo(Log.DeletedFileSuffix)
        Some(snapshot)
      } catch {
        case _: NoSuchFileException =>
          info(s"Failed to rename producer state snapshot ${snapshot.file.getAbsoluteFile} with deletion suffix because it was already deleted")
          None
      }
    }
    }
  }
}

case class SnapshotFile private[log] (@volatile private var _file: File,
                                      offset: Long) extends Logging {
  def deleteIfExists(): Boolean = {
    val deleted = Files.deleteIfExists(file.toPath)
    if (deleted) {
      info(s"Deleted producer state snapshot ${file.getAbsolutePath}")
    } else {
      info(s"Failed to delete producer state snapshot ${file.getAbsolutePath} because it does not exist.")
    }
    deleted
  }

  def updateParentDir(parentDir: File): Unit = {
    _file = new File(parentDir, _file.getName)
  }

  def file: File = {
    _file
  }

  def renameTo(newSuffix: String): Unit = {
    val renamed = new File(CoreUtils.replaceSuffix(_file.getPath, "", newSuffix))
    try {
      Utils.atomicMoveWithFallback(_file.toPath, renamed.toPath)
    } finally {
      _file = renamed
    }
  }
}

object SnapshotFile {
  def apply(file: File): SnapshotFile = {
    val offset = offsetFromFile(file)
    SnapshotFile(file, offset)
  }
}
