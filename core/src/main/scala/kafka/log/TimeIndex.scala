/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.record.RecordBatch

/**
 * 从时间戳映射到段中消息的逻辑偏移量的索引。这个索引可能是稀疏的，也就是说，它可能不包含段中所有消息的条目。
 *
 * 索引存储在一个预先分配的文件中，以保存固定数量的最大12字节时间索引项。文件格式是一系列时间索引条目。物理格式是一个8字节的时间戳和一个4字节的
 * “相对”偏移量 [[OffsetIndex]]. 时间索引条目(TIMESTAMP, OFFSET)表示所看到的最大时间戳
 * 在OFFSET之前是TIMESTAMP。即任何时间戳大于timestamp的消息都必须出现在OFFSET之后。
 *
 * 所有外部api都将相对偏移量转换为完整偏移量，因此该类的用户不与内部存储格式交互。
 *
 * 同一时间索引文件中的时间戳保证是单调递增的。
 *
 * 索引支持该文件的内存映射的时间戳查找。查找是使用二进制搜索来查找其索引时间戳最接近但小于或等于目标时间戳的消息的偏移量。
 *
 * 时间索引文件可以通过两种方式打开:一种是允许追加的空可变索引，另一种是以前已填充的不可变只读索引文件。
 * makeReadOnly方法将可变文件转换为不可变文件，并截断任何额外的字节。这是在滚动索引文件时完成的。
 *
 * 不尝试对该文件的内容进行校验和，如果发生崩溃，则重新构建该文件。
 *
 */
// 避免在AbstractIndex中隐藏可变文件
class TimeIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
    extends AbstractIndex(_file, baseOffset, maxIndexSize, writable) {
  import TimeIndex._

  @volatile private var _lastEntry = lastEntryFromIndexFile

  override def entrySize = 12

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, maxIndexSize = $maxIndexSize," +
    s" entries = ${_entries}, lastOffset = ${_lastEntry}, file position = ${mmap.position()}")

  // 我们覆盖完整检查，为点名保留最后一次索引条目槽位。
  override def isFull: Boolean = entries >= maxEntries - 1

  private def timestamp(buffer: ByteBuffer, n: Int): Long = buffer.getLong(n * entrySize)

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 8)

  def lastEntry: TimestampOffset = _lastEntry

  /**
   * 从索引文件中读取最后一个条目。该操作涉及磁盘访问。
   */
  private def lastEntryFromIndexFile: TimestampOffset = {
    inLock(lock) {
      _entries match {
        case 0 => TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
        case s => parseEntry(mmap, s - 1)
      }
    }
  }

  /**
   * 从时间索引获取第n个时间戳映射
   * @param n 时间索引中的条目号
   * @return 该条目的时间戳对
   */
  def entry(n: Int): TimestampOffset = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from  time index ${file.getAbsolutePath} " +
          s"which has size ${_entries}.")
      parseEntry(mmap, n)
    }
  }

  override def parseEntry(buffer: ByteBuffer, n: Int): TimestampOffset = {
    TimestampOffset(timestamp(buffer, n), baseOffset + relativeOffset(buffer, n))
  }

  /**
   * 尝试将时间索引项附加到时间索引。只有当时间戳和偏移量都大于最后一个追加的时间戳和最后一个追加的偏移量时，才追加新条目。
   *
   * @param timestamp The timestamp of the new time index entry
   * @param offset The offset of the new time index entry
   * @param skipFullCheck To skip checking whether the segment is full or not. We only skip the check when the segment
   *                      gets rolled or the segment is closed.
   */
  def maybeAppend(timestamp: Long, offset: Long, skipFullCheck: Boolean = false): Unit = {
    inLock(lock) {
      if (!skipFullCheck)
        require(!isFull, "Attempt to append to a full time index (size = " + _entries + ").")
      // 当偏移量等于上一项的偏移量时，不会抛出异常。这意味着我们要尝试插入与最后一项相同的时间索引项。如果要插入的时间戳索引条目与
      // 最后一个条目相同，我们只需忽略插入，因为在以下两个场景中可能会发生这种情况:
      // 1. 日志段关闭。
      // 2. 在滚动活动日志段时调用LogSegment.onBecomeInactiveSegment()。
      if (_entries != 0 && offset < lastEntry.offset)
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to slot ${_entries} no larger than" +
          s" the last offset appended (${lastEntry.offset}) to ${file.getAbsolutePath}.")
      if (_entries != 0 && timestamp < lastEntry.timestamp)
        throw new IllegalStateException(s"Attempt to append a timestamp ($timestamp) to slot ${_entries} no larger" +
          s" than the last timestamp appended (${lastEntry.timestamp}) to ${file.getAbsolutePath}.")
      // 只有当时间戳大于最后插入的时间戳时，才追加时间索引。如果所有消息的消息格式为v0，则时间戳将始终为NoTimestamp。
      // 在这种情况下，时间索引将为空。
      if (timestamp > lastEntry.timestamp) {
        trace(s"Adding index entry $timestamp => $offset to ${file.getAbsolutePath}.")
        mmap.putLong(timestamp)
        mmap.putInt(relativeOffset(offset))
        _entries += 1
        _lastEntry = TimestampOffset(timestamp, offset)
        require(_entries * entrySize == mmap.position(), s"${_entries} entries but file position in index is ${mmap.position()}.")
      }
    }
  }

  /**
   * 查找时间戳小于或等于给定时间戳的时间索引项。如果目标时间戳小于时间索引中最小的时间戳，则返回(NoTimestamp, baseOffset)。
   *
   * @param targetTimestamp The timestamp to look up.
   * @return The time index entry found.
   */
  def lookup(targetTimestamp: Long): TimestampOffset = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, targetTimestamp, IndexSearchType.KEY)
      if (slot == -1)
        TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
      else
        parseEntry(idx, slot)
    }
  }

  override def truncate() = truncateToEntries(0)

  /**
   * 从索引中删除偏移量大于或等于给定偏移量的所有项。截断到比索引中最大的偏移量大的偏移量没有影响。
   */
  override def truncateTo(offset: Long): Unit = {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.VALUE)

      /* 有3个箱子可以选择新的尺寸
       * 1) 如果索引<=偏移量中没有条目，则删除所有条目
       * 2) 如果有这个精确偏移量的条目，则删除它和比它大的所有条目
       * 3) 如果没有此偏移量的条目，则删除比下一个最小的条目大的所有条目
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  override def resize(newSize: Int): Boolean = {
    inLock(lock) {
      if (super.resize(newSize)) {
        _lastEntry = lastEntryFromIndexFile
        true
      } else
        false
    }
  }

  /**
   * 将索引截断为已知数量的条目。
   */
  private def truncateToEntries(entries: Int): Unit = {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastEntry = lastEntryFromIndexFile
      debug(s"Truncated index ${file.getAbsolutePath} to $entries entries; position is now ${mmap.position()} and last entry is now ${_lastEntry}")
    }
  }

  override def sanityCheck(): Unit = {
    val lastTimestamp = lastEntry.timestamp
    val lastOffset = lastEntry.offset
    if (_entries != 0 && lastTimestamp < timestamp(mmap, 0))
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last timestamp is $lastTimestamp which is less than the first timestamp " +
        s"${timestamp(mmap, 0)}")
    if (_entries != 0 && lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last offset is $lastOffset which is less than the first offset $baseOffset")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Time index file ${file.getAbsolutePath} is corrupt, found $length bytes " +
        s"which is neither positive nor a multiple of $entrySize.")
  }
}

object TimeIndex extends Logging {
  override val loggerName: String = classOf[TimeIndex].getName
}
