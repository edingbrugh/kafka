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

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ListBuffer

private[log] case class TxnIndexSearchResult(abortedTransactions: List[AbortedTxn], isComplete: Boolean)

/**
 * 事务索引维护关于每个段的终止事务的元数据。这包括中止事务的开始和结束偏移量以及中止时的最后稳定偏移量(LSO)。
 * 该索引用于在READ_COMMITTED隔离级别查找给定读取请求范围内的已终止事务。每个日志段最多有一个事务索引。
 * 这些条目对应于在相应日志段中写入提交标记的事务。但是，请注意，单个事务可能跨越多个段。因此，恢复索引需要扫描较早的段，以便找到事务的开始。
 */
@nonthreadsafe
class TransactionIndex(val startOffset: Long, @volatile private var _file: File) extends Logging {

  // 请注意，直到我们需要它时才会创建该文件
  @volatile private var maybeChannel: Option[FileChannel] = None
  private var lastOffset: Option[Long] = None

  if (_file.exists)
    openChannel()

  def append(abortedTxn: AbortedTxn): Unit = {
    lastOffset.foreach { offset =>
      if (offset >= abortedTxn.lastOffset)
        throw new IllegalArgumentException(s"The last offset of appended transactions must increase sequentially, but " +
          s"${abortedTxn.lastOffset} is not greater than current last offset $offset of index ${file.getAbsolutePath}")
    }
    lastOffset = Some(abortedTxn.lastOffset)
    Utils.writeFully(channel(), abortedTxn.buffer.duplicate())
  }

  def flush(): Unit = maybeChannel.foreach(_.force(true))

  def file: File = _file

  def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

  /**
   * 删除该索引。
   *
   * @throws IOException 如果由于IO错误导致删除失败
   * @return 如果使用此方法删除文件，则为“true”;如果文件不存在而无法删除，则为false
   */
  def deleteIfExists(): Boolean = {
    close()
    Files.deleteIfExists(file.toPath)
  }

  private def channel(): FileChannel = {
    maybeChannel match {
      case Some(channel) => channel
      case None => openChannel()
    }
  }

  private def openChannel(): FileChannel = {
    val channel = FileChannel.open(file.toPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
      StandardOpenOption.WRITE)
    maybeChannel = Some(channel)
    channel.position(channel.size)
    channel
  }

  /**
   * 从索引中删除所有条目。与“AbstractIndex”不同，此索引不会提前调整大小。
   */
  def reset(): Unit = {
    maybeChannel.foreach(_.truncate(0))
    lastOffset = None
  }

  def close(): Unit = {
    maybeChannel.foreach(_.close())
    maybeChannel = None
  }

  def renameTo(f: File): Unit = {
    try {
      if (file.exists)
        Utils.atomicMoveWithFallback(file.toPath, f.toPath, false)
    } finally _file = f
  }

  def truncateTo(offset: Long): Unit = {
    val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
    var newLastOffset: Option[Long] = None
    for ((abortedTxn, position) <- iterator(() => buffer)) {
      if (abortedTxn.lastOffset >= offset) {
        channel().truncate(position)
        lastOffset = newLastOffset
        return
      }
      newLastOffset = Some(abortedTxn.lastOffset)
    }
  }

  private def iterator(allocate: () => ByteBuffer = () => ByteBuffer.allocate(AbortedTxn.TotalSize)): Iterator[(AbortedTxn, Int)] = {
    maybeChannel match {
      case None => Iterator.empty
      case Some(channel) =>
        var position = 0

        new Iterator[(AbortedTxn, Int)] {
          override def hasNext: Boolean = channel.position - position >= AbortedTxn.TotalSize

          override def next(): (AbortedTxn, Int) = {
            try {
              val buffer = allocate()
              Utils.readFully(channel, buffer, position)
              buffer.flip()

              val abortedTxn = new AbortedTxn(buffer)
              if (abortedTxn.version > AbortedTxn.CurrentVersion)
                throw new KafkaException(s"Unexpected aborted transaction version ${abortedTxn.version} " +
                  s"in transaction index ${file.getAbsolutePath}, current version is ${AbortedTxn.CurrentVersion}")
              val nextEntry = (abortedTxn, position)
              position += AbortedTxn.TotalSize
              nextEntry
            } catch {
              case e: IOException =>
                // 从索引中删除所有条目。与“AbstractIndex”不同，此索引不会提前调整大小。
                throw new KafkaException(s"Failed to read from the transaction index ${file.getAbsolutePath}", e)
            }
          }
        }
    }
  }

  def allAbortedTxns: List[AbortedTxn] = {
    iterator().map(_._1).toList
  }

  /**
   * 收集与给定取范围重叠的所有终止事务。
   *
   * @param fetchOffset Inclusive first offset of the fetch range
   * @param upperBoundOffset Exclusive last offset in the fetch range
   * @return An object containing the aborted transactions and whether the search needs to continue
   *         into the next log segment.
   */
  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult = {
    val abortedTransactions = ListBuffer.empty[AbortedTxn]
    for ((abortedTxn, _) <- iterator()) {
      if (abortedTxn.lastOffset >= fetchOffset && abortedTxn.firstOffset < upperBoundOffset)
        abortedTransactions += abortedTxn

      if (abortedTxn.lastStableOffset >= upperBoundOffset)
        return TxnIndexSearchResult(abortedTransactions.toList, isComplete = true)
    }
    TxnIndexSearchResult(abortedTransactions.toList, isComplete = false)
  }

  /**
   * 对该索引执行基本的完整性检查，以发现明显的问题。
   *
   * @throws CorruptIndexException if any problems are found.
   */
  def sanityCheck(): Unit = {
    val buffer = ByteBuffer.allocate(AbortedTxn.TotalSize)
    for ((abortedTxn, _) <- iterator(() => buffer)) {
      if (abortedTxn.lastOffset < startOffset)
        throw new CorruptIndexException(s"Last offset of aborted transaction $abortedTxn in index " +
          s"${file.getAbsolutePath} is less than start offset $startOffset")
    }
  }

}

private[log] object AbortedTxn {
  val VersionOffset = 0
  val VersionSize = 2
  val ProducerIdOffset = VersionOffset + VersionSize
  val ProducerIdSize = 8
  val FirstOffsetOffset = ProducerIdOffset + ProducerIdSize
  val FirstOffsetSize = 8
  val LastOffsetOffset = FirstOffsetOffset + FirstOffsetSize
  val LastOffsetSize = 8
  val LastStableOffsetOffset = LastOffsetOffset + LastOffsetSize
  val LastStableOffsetSize = 8
  val TotalSize = LastStableOffsetOffset + LastStableOffsetSize

  val CurrentVersion: Short = 0
}

private[log] class AbortedTxn(val buffer: ByteBuffer) {
  import AbortedTxn._

  def this(producerId: Long,
           firstOffset: Long,
           lastOffset: Long,
           lastStableOffset: Long) = {
    this(ByteBuffer.allocate(AbortedTxn.TotalSize))
    buffer.putShort(CurrentVersion)
    buffer.putLong(producerId)
    buffer.putLong(firstOffset)
    buffer.putLong(lastOffset)
    buffer.putLong(lastStableOffset)
    buffer.flip()
  }

  def this(completedTxn: CompletedTxn, lastStableOffset: Long) =
    this(completedTxn.producerId, completedTxn.firstOffset, completedTxn.lastOffset, lastStableOffset)

  def version: Short = buffer.get(VersionOffset)

  def producerId: Long = buffer.getLong(ProducerIdOffset)

  def firstOffset: Long = buffer.getLong(FirstOffsetOffset)

  def lastOffset: Long = buffer.getLong(LastOffsetOffset)

  def lastStableOffset: Long = buffer.getLong(LastStableOffsetOffset)

  def asAbortedTransaction: FetchResponseData.AbortedTransaction = new FetchResponseData.AbortedTransaction()
    .setProducerId(producerId)
    .setFirstOffset(firstOffset)

  override def toString: String =
    s"AbortedTxn(version=$version, producerId=$producerId, firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, lastStableOffset=$lastStableOffset)"

  override def equals(any: Any): Boolean = {
    any match {
      case that: AbortedTxn => this.buffer.equals(that.buffer)
      case _ => false
    }
  }

  override def hashCode(): Int = buffer.hashCode
}
