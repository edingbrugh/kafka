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

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * 一种操作，其处理最多需要延迟给定的延迟时间。例如，延迟的生产操作可能正在等待指定数量的ack;或者，延迟的读取操作可能会等待给定字节数的累积。
 *
 * 完成延迟操作的逻辑在onComplete()中定义，并且只会被调用一次。一旦操作完成，isCompleted()将返回true。onComplete()可以由forceComplete()触发，
 * 如果操作尚未完成，它会在delayMs之后强制调用onComplete()，或者tryComplete()触发，它首先检查操作现在是否可以完成，
 * 如果可以，则调用forceComplete()。
 *
 * DelayedOperation的子类需要同时提供onComplete()和tryComplete()的实现。
 *
 * 注意，如果你在onComplete()中添加一个像DelayedJoin那样调用ReplicaManager.appendRecords()的未来延迟操作，
 * 你必须意识到这个操作的onExpiration()需要调用actionQueue.tryCompleteAction()。
 */
abstract class DelayedOperation(override val delayMs: Long,
                                lockOpt: Option[Lock] = None)
  extends TimerTask with Logging {

  private val completed = new AtomicBoolean(false)
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * 强制完成延迟的操作(如果尚未完成)。
   * 1.触发该功能。该操作已在tryComplete()
   * 2中验证为可完成。如果操作被调用者完成，返回true:注意并发线程可以尝试完成相同的操作，
   * 但只有第一个线程会成功完成操作并返回true，其他线程仍然会返回false

   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * 检查延迟的操作是否已经完成
   */
  def isCompleted: Boolean = completed.get()

  /**
   * 当延迟的操作过期并因此被迫完成时执行的回调。
   */
  def onExpiration(): Unit

  /**
   * 完成一项操作的过程;这个函数需要在子类中定义，并且在forceComplete()中只调用一次。
   */
  def onComplete(): Unit

  /**
   * 尝试完成延迟的操作，首先检查操作现在是否可以完成。如果是，通过调用forceComplete()执行完成逻辑并返回true，
   * 如果forceComplete返回true;此函数需要在子类中定义
   */
  def tryComplete(): Boolean

  /**
   * tryComplete()的线程安全变体，如果第一个tryComplete返回false，则调用额外的函数
   * @param f else function to be executed after first tryComplete returns false
   * @return result of tryComplete
   */
  private[server] def safeTryCompleteOrElse(f: => Unit): Boolean = inLock(lock) {
    if (tryComplete()) true
    else {
      f
      // last completion check
      tryComplete()
    }
  }

  /**
   * Thread-safe variant of tryComplete()
   */
  private[server] def safeTryComplete(): Boolean = inLock(lock)(tryComplete())

  /*
   * run() method defines a task that is executed on timeout
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {
  /* 操作监视键的列表 */
  private class WatcherList {
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * 返回所有当前的监视器列表，注意返回的监视器可能会被其他线程从列表中删除
     */
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)
  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* 后台线程使操作过期 */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)
  newGauge("PurgatorySize", () => watched, metricsTags)
  newGauge("NumDelayedOperations", () => numDelayed, metricsTags)

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * 检查操作是否可以完成，如果不能，则根据给定的监视键观察。注意，延迟的操作可以在多个键上观察。有可能在某个操作被添加到某些键的监视列表后才完成，
   * 但不是所有键都被添加到监视列表。在这种情况下，操作被认为已经完成，并且不会被添加到剩余键的监视列表中。
   * 过期收割机线程将从存在该操作的任何监视器列表中删除该操作。
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // tryComplete()的成本通常与键的数量成正比。如果有很多键，对每个键调用tryComplete()将会非常昂贵。相反，
    // 我们通过safeTryCompleteOrElse()以以下方式执行检查。如果操作未完成，则将该操作添加到所有键上。然后再次调用tryComplete()。
    // 此时，如果操作仍未完成，我们可以保证它不会错过任何未来的触发事件，因为该操作已经在所有键的监视列表中。
    //
    // ==============[story about lock]==============
    // 通过safeTryCompleteOrElse()，我们在将操作添加到观察列表并执行tryComplete()检查时保持操作的锁。
    // 这是为了避免在tryCompleteElseWatch()和checkAndComplete()的调用者之间发生潜在的死锁。例如，如果锁仅在最后的tryComplete()中被持有，
    // 则可能发生以下死锁
    // 1) thread_a持有来自TransactionStateManager的stateLock的readlock
    // 2) thread_a正在执行tryCompleteElseWatch()
    // 3) Thread_a将op添加到观察列表中
    // 4) thread_b需要从TransactionStateManager获取stateLock的写锁(被thread_a阻塞)
    // 5) thread_c调用checkAndComplete()并持有op锁
    // 6) thread_c正在等待stateLock的读锁完成op(被thread_b阻塞)
    // 7) thread_a正在等待op的锁来调用最后的tryComplete()(被thread_c阻塞)
    //
    // 注意，即使使用当前的方法，仍然可能引入死锁。例如,
    // 1) thread_a调用tryCompleteElseWatch()并获得op锁
    // 2) Thread_a将op添加到观察列表中
    // 3) thread_a调用op#tryComplete并尝试请求lock_b
    // 4) thread_b持有lock_b并调用checkAndComplete()
    // 5) Thread_b从观察列表中看到op
    // 6) Thread_b需要op锁
    // 为了避免上述情况，我们建议在不持有任何排他锁的情况下调用DelayedOperationPurgatory.checkAndComplete()。
    // 由于DelayedOperationPurgatory.checkAndComplete()异步完成延迟的操作，因此持有排他锁来进行调用通常是不必要的。
    if (operation.safeTryCompleteOrElse {
      watchKeys.foreach(key => watchForOperation(key, operation))
      if (watchKeys.nonEmpty) estimatedTotalOperations.incrementAndGet()
    }) return true

    // 如果它现在还不能完成，因此被监视，那么也添加到过期队列中
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // 取消定时器任务
        operation.cancel()
      }
    }

    false
  }

  /**
   * 检查一些延迟的操作是否可以用给定的表键完成，如果可以，完成它们。
   *
   * @return 在此过程中完成的操作的数量
   */
  def checkAndComplete(key: Any): Int = {
    val wl = watcherList(key)
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    val numCompleted = if (watchers == null)
      0
    else
      watchers.tryCompleteWatched()
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    numCompleted
  }

  /**
   * 返回总大小的手表列表的炼狱。由于一个操作可能在多个列表中被监视，并且即使它已经完成，它的一些被监视的条目可能仍然在监视列表中，
   * 因此这个数字可能大于实际被监视的操作的数量
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * 返回过期队列中延迟操作的个数
   */
  def numDelayed: Int = timeoutTimer.size

  /**
    * 取消对给定键的任何延迟操作的监视。注意，操作将不会完成
    */
  def cancelForKey(key: Any): List[T] = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watchers = wl.watchersByKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * 返回给定键的监视列表，注意我们需要获取removeWatchersLock以避免将操作添加到已删除的监视列表中
   */
  private def watchForOperation(key: Any, operation: T): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * 如果该键的列表为空，则从监视列表中删除该键
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // 如果当前键不再与要删除的监视程序相关，则跳过
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
    removeMetric("PurgatorySize", metricsTags)
    removeMetric("NumDelayedOperations", metricsTags)
  }

  /**
   * 基于某个键的监视延迟操作的链表
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // 统计当前被监视操作的个数。这是O(n)，所以如果可能的话使用isEmpty()
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // 添加要监视的元素
    def watch(t: T): Unit = {
      operations.add(t)
    }

    // 遍历列表并尝试完成一些被监视的元素
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // 另一个线程已完成此操作，请删除它
          iter.remove()
        } else if (curr.safeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // 遍历列表并清除其他人已经完成的元素
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    timeoutTimer.advanceClock(timeoutMs)

    // 如果已完成但仍在监视的操作数量大于清除阈值，则触发清除。这个数字是通过估计的操作总数与挂起的延迟操作数量之差计算出来的。
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // 现在将estimatedTotalOperations设置为delayed(等待操作的数量)，因为我们要清理观察者。请注意，
      // 如果在清理过程中完成了更多的操作，我们最终可能会稍微高估操作总数。
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      val purged = watcherLists.foldLeft(0) {
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * 后台处理将延迟已超时的操作
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      advanceClock(200L)
    }
  }
}
