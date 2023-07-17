/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.Optional
import kafka.api.{ApiVersion, LeaderAndIsr}
import kafka.common.UnexpectedAppendOffsetException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zookeeper.ZooKeeperClientException
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.{DescribeProducersResponseData, FetchResponseData}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{IsolationLevel, TopicPartition, Uuid}

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

trait IsrChangeListener {
  def markExpand(): Unit

  def markShrink(): Unit

  def markFailed(): Unit
}

class DelayedOperations(topicPartition: TopicPartition,
                        produce: DelayedOperationPurgatory[DelayedProduce],
                        fetch: DelayedOperationPurgatory[DelayedFetch],
                        deleteRecords: DelayedOperationPurgatory[DelayedDeleteRecords]) {

  def checkAndCompleteAll(): Unit = {
    val requestKey = TopicPartitionOperationKey(topicPartition)
    fetch.checkAndComplete(requestKey)
    produce.checkAndComplete(requestKey)
    deleteRecords.checkAndComplete(requestKey)
  }

  def numDelayedDelete: Int = deleteRecords.numDelayed
}

object Partition extends KafkaMetricsGroup {
  def apply(topicPartition: TopicPartition,
            time: Time,
            replicaManager: ReplicaManager): Partition = {

    val isrChangeListener = new IsrChangeListener {
      override def markExpand(): Unit = {
        replicaManager.isrExpandRate.mark()
      }

      override def markShrink(): Unit = {
        replicaManager.isrShrinkRate.mark()
      }

      override def markFailed(): Unit = replicaManager.failedIsrUpdatesRate.mark()
    }

    val delayedOperations = new DelayedOperations(
      topicPartition,
      replicaManager.delayedProducePurgatory,
      replicaManager.delayedFetchPurgatory,
      replicaManager.delayedDeleteRecordsPurgatory)

    new Partition(topicPartition,
      replicaLagTimeMaxMs = replicaManager.config.replicaLagTimeMaxMs,
      interBrokerProtocolVersion = replicaManager.config.interBrokerProtocolVersion,
      localBrokerId = replicaManager.config.brokerId,
      time = time,
      isrChangeListener = isrChangeListener,
      delayedOperations = delayedOperations,
      metadataCache = replicaManager.metadataCache,
      logManager = replicaManager.logManager,
      alterIsrManager = replicaManager.alterIsrManager)
  }

  def removeMetrics(topicPartition: TopicPartition): Unit = {
    val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)
    removeMetric("UnderReplicated", tags)
    removeMetric("UnderMinIsr", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
    removeMetric("AtMinIsr", tags)
  }
}


sealed trait AssignmentState {
  def replicas: Seq[Int]

  def replicationFactor: Int = replicas.size

  def isAddingReplica(brokerId: Int): Boolean = false
}

case class OngoingReassignmentState(addingReplicas: Seq[Int],
                                    removingReplicas: Seq[Int],
                                    replicas: Seq[Int]) extends AssignmentState {

  override def replicationFactor: Int = replicas.diff(addingReplicas).size // keep the size of the original replicas

  override def isAddingReplica(replicaId: Int): Boolean = addingReplicas.contains(replicaId)
}

case class SimpleAssignmentState(replicas: Seq[Int]) extends AssignmentState


sealed trait IsrState {
  /**
   * 只包括已提交给ZK的同步副本。
   */
  def isr: Set[Int]

  /**
   * 该集合可能包括扩展后未提交的ISR成员。这种“有效的”ISR用于提高高水位以及确定ack =所有生成请求所需的副本。
   * 仅适用于IBP 2.7-IV2，对于旧版本，这将返回提交的ISR
   *
   */
  def maximalIsr: Set[Int]

  /**
   * 指示我们是否有备用请求。
   */
  def isInflight: Boolean
}

case class PendingExpandIsr(
                             isr: Set[Int],
                             newInSyncReplicaId: Int
                           ) extends IsrState {
  val maximalIsr = isr + newInSyncReplicaId
  val isInflight = true

  override def toString: String = {
    s"PendingExpandIsr(isr=$isr" +
      s", newInSyncReplicaId=$newInSyncReplicaId" +
      ")"
  }
}

case class PendingShrinkIsr(
                             isr: Set[Int],
                             outOfSyncReplicaIds: Set[Int]
                           ) extends IsrState {
  val maximalIsr = isr
  val isInflight = true

  override def toString: String = {
    s"PendingShrinkIsr(isr=$isr" +
      s", outOfSyncReplicaIds=$outOfSyncReplicaIds" +
      ")"
  }
}

case class CommittedIsr(
                         isr: Set[Int]
                       ) extends IsrState {
  val maximalIsr = isr
  val isInflight = false

  override def toString: String = {
    s"CommittedIsr(isr=$isr" +
      ")"
  }
}


/**
 * 表示主题分区的数据结构。leader维护AR, ISR, CUR, RAR并发注意事项:1)分区是线程安全的。分区上的操作可以从不同的请求处理线程并发地调用
 * 2)ISR更新使用读写锁同步。读锁用于检查是否需要更新，以避免在没有执行更新的情况下获取副本读取时获得写锁。在执行更新之前，
 * 在写锁下检查ISR更新条件第二次。
 * 3)在持有ISR写锁的同时处理各种其他操作，如leader更改。这可能会导致生产和副本获取请求的延迟，但这些操作通常不常见。
 * 4)硬件更新使用ISR读锁同步。@Log锁是在更新过程中获取的，锁顺序为Partition lock -> Log lock。
 * 5)当ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()将follower副本替换为future副本时，
 * 锁用于防止follower副本被更新。
 */
class Partition(val topicPartition: TopicPartition,
                val replicaLagTimeMaxMs: Long,
                interBrokerProtocolVersion: ApiVersion,
                localBrokerId: Int,
                time: Time,
                isrChangeListener: IsrChangeListener,
                delayedOperations: DelayedOperations,
                metadataCache: MetadataCache,
                logManager: LogManager,
                alterIsrManager: AlterIsrManager) extends Logging with KafkaMetricsGroup {

  def topic: String = topicPartition.topic

  def partitionId: Int = topicPartition.partition

  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)
  private val remoteReplicasMap = new Pool[Int, Replica]
  // 只有在执行多个读操作时才需要读锁，并且需要以一致的方式执行
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock

  // 锁定以防止后续副本日志更新，同时检查日志目录是否可以用未来的日志替换。
  private val futureLogLock = new Object()
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // 上面的'leaderEpoch'的起始偏移量(该分区当前leader的leader纪元)，在此代理为分区leader时定义
  @volatile private var leaderEpochStartOffsetOpt: Option[Long] = None
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  @volatile private[cluster] var isrState: IsrState = CommittedIsr(Set.empty)
  @volatile var assignmentState: AssignmentState = SimpleAssignmentState(Seq.empty)

  // 属于此分区的日志。大多数情况下只有一个日志，但如果日志目录被更改(由于ReplicaAlterLogDirs命令)，
  // 我们可能会有两个日志，直到复制完成并切换到新位置。下面定义的log和futureLog变量用于捕获此信息
  @volatile var log: Option[Log] = None
  // 如果正在执行ReplicaAlterLogDir命令，则表示日志的未来位置
  @volatile var futureLog: Option[Log] = None

  /* 控制器的纪元最后一次改变了领导者。这需要在代理启动时正确初始化。这样做的一种方法是通过控制器的start replica state change命令。
  当一个新的代理启动时，控制器向它发送一个启动副本命令，其中包含代理所托管的每个分区的leader。除了leader之外，
  控制器还可以发送为每个分区选出leader的控制器的epoch。*/
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated", () => if (isUnderReplicated) 1 else 0, tags)
  newGauge("InSyncReplicasCount", () => if (isLeader) isrState.isr.size else 0, tags)
  newGauge("UnderMinIsr", () => if (isUnderMinIsr) 1 else 0, tags)
  newGauge("AtMinIsr", () => if (isAtMinIsr) 1 else 0, tags)
  newGauge("ReplicasCount", () => if (isLeader) assignmentState.replicationFactor else 0, tags)
  newGauge("LastStableOffsetLag", () => log.map(_.lastStableOffsetLag).getOrElse(0), tags)

  def isUnderReplicated: Boolean = isLeader && (assignmentState.replicationFactor - isrState.isr.size) > 0

  def isUnderMinIsr: Boolean = leaderLogIfLocal.exists {
    isrState.isr.size < _.config.minInSyncReplicas
  }

  def isAtMinIsr: Boolean = leaderLogIfLocal.exists {
    isrState.isr.size == _.config.minInSyncReplicas
  }

  def isReassigning: Boolean = assignmentState.isInstanceOf[OngoingReassignmentState]

  def isAddingLocalReplica: Boolean = assignmentState.isAddingReplica(localBrokerId)

  def isAddingReplica(replicaId: Int): Boolean = assignmentState.isAddingReplica(replicaId)

  def inSyncReplicaIds: Set[Int] = isrState.isr

  /**
   * 如果
   * 1)当前副本不在给定的日志目录中，并且
   * 2)未来副本不存在，则创建未来副本。此方法假设当前副本已经创建。
   *
   * @param logDir                   log directory
   * @param highWatermarkCheckpoints Checkpoint to load initial high watermark from
   * @return true iff the future replica is created
   */
  def maybeCreateFutureReplica(logDir: String, highWatermarkCheckpoints: OffsetCheckpoints): Boolean = {
    // 需要writeLock来确保当调用者检查当前副本的日志目录和未来副本的存在时，没有其他线程可以更新当前副本的日志目录或删除未来副本。
    inWriteLock(leaderIsrUpdateLock) {
      val currentLogDir = localLogOrException.parentDir
      if (currentLogDir == logDir) {
        info(s"Current log directory $currentLogDir is same as requested log dir $logDir. " +
          s"Skipping future replica creation.")
        false
      } else {
        futureLog match {
          case Some(partitionFutureLog) =>
            val futureLogDir = partitionFutureLog.parentDir
            if (futureLogDir != logDir)
              throw new IllegalStateException(s"The future log dir $futureLogDir of $topicPartition is " +
                s"different from the requested log dir $logDir")
            false
          case None =>
            createLogIfNotExists(isNew = false, isFutureReplica = true, highWatermarkCheckpoints, topicId)
            true
        }
      }
    }
  }

  def createLogIfNotExists(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints, topicId: Option[Uuid]): Unit = {
    def maybeCreate(logOpt: Option[Log]): Log = {
      logOpt match {
        case Some(log) =>
          trace(s"${if (isFutureReplica) "Future Log" else "Log"} already exists.")
          if (log.topicId.isEmpty)
            topicId.foreach(log.assignTopicId)
          log
        case None =>
          // todo 7 创建路径
          createLog(isNew, isFutureReplica, offsetCheckpoints, topicId)
      }
    }

    if (isFutureReplica) {
      this.futureLog = Some(maybeCreate(this.futureLog))
    } else {
      // todo 6 创建路径
      this.log = Some(maybeCreate(this.log))
    }
  }

  private[cluster] def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints, topicId: Option[Uuid]): Log = {
    def updateHighWatermark(log: Log) = {
      val checkpointHighWatermark = offsetCheckpoints.fetch(log.parentDir, topicPartition).getOrElse {
        info(s"No checkpointed highwatermark is found for partition $topicPartition")
        0L
      }
      val initialHighWatermark = log.updateHighWatermark(checkpointHighWatermark)
      info(s"Log loaded for partition $topicPartition with initial high watermark $initialHighWatermark")
    }

    logManager.initializingLog(topicPartition)
    var maybeLog: Option[Log] = None
    try {
      // todo 8 创建路径
      val log = logManager.getOrCreateLog(topicPartition, isNew, isFutureReplica, topicId)
      maybeLog = Some(log)
      updateHighWatermark(log)
      log
    } finally {
      logManager.finishedInitializingLog(topicPartition, maybeLog)
    }
  }

  def getReplica(replicaId: Int): Option[Replica] = Option(remoteReplicasMap.get(replicaId))

  private def checkCurrentLeaderEpoch(remoteLeaderEpochOpt: Optional[Integer]): Errors = {
    if (!remoteLeaderEpochOpt.isPresent) {
      Errors.NONE
    } else {
      val remoteLeaderEpoch = remoteLeaderEpochOpt.get
      val localLeaderEpoch = leaderEpoch
      if (localLeaderEpoch > remoteLeaderEpoch)
        Errors.FENCED_LEADER_EPOCH
      else if (localLeaderEpoch < remoteLeaderEpoch)
        Errors.UNKNOWN_LEADER_EPOCH
      else
        Errors.NONE
    }
  }

  private def getLocalLog(currentLeaderEpoch: Optional[Integer],
                          requireLeader: Boolean): Either[Log, Errors] = {
    checkCurrentLeaderEpoch(currentLeaderEpoch) match {
      case Errors.NONE =>
        if (requireLeader && !isLeader) {
          Right(Errors.NOT_LEADER_OR_FOLLOWER)
        } else {
          log match {
            case Some(partitionLog) =>
              Left(partitionLog)
            case _ =>
              Right(Errors.NOT_LEADER_OR_FOLLOWER)
          }
        }
      case error =>
        Right(error)
    }
  }

  def localLogOrException: Log = log.getOrElse {
    throw new NotLeaderOrFollowerException(s"Log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def futureLocalLogOrException: Log = futureLog.getOrElse {
    throw new NotLeaderOrFollowerException(s"Future log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def leaderLogIfLocal: Option[Log] = {
    log.filter(_ => isLeader)
  }

  /**
   * 如果此节点当前是分区的领导节点，则返回true。
   */
  def isLeader: Boolean = leaderReplicaIdOpt.contains(localBrokerId)

  private def localLogWithEpochOrException(currentLeaderEpoch: Optional[Integer],
                                           requireLeader: Boolean): Log = {
    getLocalLog(currentLeaderEpoch, requireLeader) match {
      case Left(localLog) => localLog
      case Right(error) =>
        throw error.exception(s"Failed to find ${if (requireLeader) "leader" else ""} log for " +
          s"partition $topicPartition with leader epoch $currentLeaderEpoch. The current leader " +
          s"is $leaderReplicaIdOpt and the current epoch $leaderEpoch")
    }
  }

  // 测试可见——由单元测试使用，用于为该分区设置日志
  def setLog(log: Log, isFutureLog: Boolean): Unit = {
    if (isFutureLog)
      futureLog = Some(log)
    else
      this.log = Some(log)
  }

  /**
   * @return 如果日志或主题ID不存在，则为日志对应的主题ID，如果不存在则为None。
   */
  def topicId: Option[Uuid] = {
    val log = this.log.orElse(logManager.getLog(topicPartition))
    log.flatMap(_.topicId)
  }

  // remoteReplicas将在热路径中调用，并且必须便宜
  def remoteReplicas: Iterable[Replica] =
    remoteReplicasMap.values

  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      futureLog.exists(_.parentDir != newDestinationDir)
    }
  }

  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      futureLog = None
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  // 如果未来的副本存在，并且它已经赶上了该分区的当前副本，则返回true。只有ReplicaAlterDirThread会调用该方法，
  // 如果该方法返回true，则ReplicaAlterDirThread应该从其partitionStates中删除该分区
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    // 锁定以防止跟踪者追加日志，同时检查日志目录是否可以用将来的日志替换。
    futureLogLock.synchronized {
      val localReplicaLEO = localLogOrException.logEndOffset
      val futureReplicaLEO = futureLog.map(_.logEndOffset)
      if (futureReplicaLEO.contains(localReplicaLEO)) {
        // 需要写锁来确保当ReplicaAlterDirThread检查当前副本的LEO时，没有其他线程可以通过日志截断或日志追加操作来更新当前副本的LEO。
        inWriteLock(leaderIsrUpdateLock) {
          futureLog match {
            case Some(futurePartitionLog) =>
              if (log.exists(_.logEndOffset == futurePartitionLog.logEndOffset)) {
                logManager.replaceCurrentWithFutureLog(topicPartition)
                log = futureLog
                removeFutureLocalReplica(false)
                true
              } else false
            case None =>
              // 在这种情况下，该分区应该已经从ReplicaAlterLogDirsThread的状态中删除，
              // 这样ReplicaAlterLogDirsThread就不必再次从状态中删除该分区以避免竞争条件
              false
          }
        }
      } else false
    }
  }

  /**
   * 删除分区。注意，删除分区并不会删除底层日志。删除分区后，复制管理器将删除日志。
   */
  def delete(): Unit = {
    // 需要持有锁以防止appendMessagesToLeader()由于日志被删除而遇到IO异常
    inWriteLock(leaderIsrUpdateLock) {
      remoteReplicasMap.clear()
      assignmentState = SimpleAssignmentState(Seq.empty)
      log = None
      futureLog = None
      isrState = CommittedIsr(Set.empty)
      leaderReplicaIdOpt = None
      leaderEpochStartOffsetOpt = None
      Partition.removeMetrics(topicPartition)
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  def getZkVersion: Int = this.zkVersion

  /**
   * 通过重置远程副本的LogEndOffset(该代理上次成为leader时可能存在旧的LogEndOffset)并设置新的leader和ISR，
   * 使本地副本成为leader。如果leader副本id不变，则返回false，表示副本管理器。
   */
  def makeLeader(partitionState: LeaderAndIsrPartitionState,
                 highWatermarkCheckpoints: OffsetCheckpoints,
                 topicId: Option[Uuid]): Boolean = {
    // todo 3 创建路径
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // 记录做出领导决策的控制器的纪元。这在更新isr以在zookeeper路径中维护决策者控制器的epoch时非常有用
      controllerEpoch = partitionState.controllerEpoch

      val isr = partitionState.isr.asScala.map(_.toInt).toSet
      val addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt)
      val removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)

      updateAssignmentAndIsr(
        assignment = partitionState.replicas.asScala.map(_.toInt),
        isr = isr,
        addingReplicas = addingReplicas,
        removingReplicas = removingReplicas
      )
      try {
        // todo 4 创建路径
        createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints, topicId)
      } catch {
        case e: ZooKeeperClientException =>
          stateChangeLogger.error(s"A ZooKeeper client exception has occurred and makeLeader will be skipping the " +
            s"state change for the partition $topicPartition with leader epoch: $leaderEpoch ", e)

          return false
      }

      val leaderLog = localLogOrException
      val leaderEpochStartOffset = leaderLog.logEndOffset
      stateChangeLogger.info(s"Leader $topicPartition starts at leader epoch ${partitionState.leaderEpoch} from " +
        s"offset $leaderEpochStartOffset with high watermark ${leaderLog.highWatermark} " +
        s"ISR ${isr.mkString("[", ",", "]")} addingReplicas ${addingReplicas.mkString("[", ",", "]")} " +
        s"removingReplicas ${removingReplicas.mkString("[", ",", "]")}. Previous leader epoch was $leaderEpoch.")

      // 我们在这里缓存leader epoch，只有当它在本地时才持久化它(因此有一个日志目录)
      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
      zkVersion = partitionState.zkVersion

      // 在短时间内连续进行领导人选举的情况下，追随者在其日志中的条目可能比新领导人日志中的任何条目更晚。
      // 为了确保这些follower能够截断到正确的偏移量，我们必须缓存新的leader epoch和开始偏移量，
      // 因为它应该大于follower尝试查询的任何epoch。
      leaderLog.maybeAssignEpochStartOffset(leaderEpoch, leaderEpochStartOffset)

      val isNewLeader = !isLeader
      val curTimeMs = time.milliseconds
      // 初始化副本的lastCaughtUpTime以及它们的lastFetchTimeMs和lastFetchLeaderLogEndOffset。
      remoteReplicas.foreach { replica =>
        val lastCaughtUpTimeMs = if (isrState.isr.contains(replica.brokerId)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(leaderEpochStartOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      if (isNewLeader) {
        // 转换hw后，将本地副本标记为leader
        leaderReplicaIdOpt = Some(localBrokerId)
        // 重置远程副本的日志端偏移量
        remoteReplicas.foreach { replica =>
          replica.updateFetchState(
            followerFetchOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata,
            followerStartOffset = Log.UnknownOffset,
            followerFetchTimeMs = 0L,
            leaderEndOffset = Log.UnknownOffset)
        }
      }
      // 我们可能需要增加高水位，因为ISR可以降低到1
      (maybeIncrementLeaderHW(leaderLog), isNewLeader)
    }
    // HW改变后，一些延迟的操作可能会被解除
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   * 通过将新的leader和ISR设置为空，使本地副本成为follower。如果leader副本id没有更改，
   * 并且新epoch等于或大于1(即没有错过更新)，则返回false以向副本管理器指示状态已经正确，可以跳过成为follower的步骤
   */
  def makeFollower(partitionState: LeaderAndIsrPartitionState,
                   highWatermarkCheckpoints: OffsetCheckpoints,
                   topicId: Option[Uuid]): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val newLeaderBrokerId = partitionState.leader
      val oldLeaderEpoch = leaderEpoch
      // 记录做出领导决策的控制器的纪元。这在更新isr以在zookeeper路径中维护决策者控制器的epoch时非常有用
      controllerEpoch = partitionState.controllerEpoch

      updateAssignmentAndIsr(
        assignment = partitionState.replicas.asScala.iterator.map(_.toInt).toSeq,
        isr = Set.empty[Int],
        addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt),
        removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)
      )
      try {
        createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints, topicId)
      } catch {
        case e: ZooKeeperClientException =>
          stateChangeLogger.error(s"A ZooKeeper client exception has occurred. makeFollower will be skipping the " +
            s"state change for the partition $topicPartition with leader epoch: $leaderEpoch.", e)

          return false
      }

      val followerLog = localLogOrException
      val leaderEpochEndOffset = followerLog.logEndOffset
      stateChangeLogger.info(s"Follower $topicPartition starts at leader epoch ${partitionState.leaderEpoch} from " +
        s"offset $leaderEpochEndOffset with high watermark ${followerLog.highWatermark}. " +
        s"Previous leader epoch was $leaderEpoch.")

      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = None
      zkVersion = partitionState.zkVersion

      if (leaderReplicaIdOpt.contains(newLeaderBrokerId) && leaderEpoch == oldLeaderEpoch) {
        false
      } else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * 根据最后一次获取请求更新leader中的follower状态。看到
   * [[Replica.updateFetchState()]] 获取详细信息。
   *
   * @return 如果追随者的获取状态已更新，则为true;如果无法识别followerId，则为false
   */
  def updateFollowerFetchState(followerId: Int,
                               followerFetchOffsetMetadata: LogOffsetMetadata,
                               followerStartOffset: Long,
                               followerFetchTimeMs: Long,
                               leaderEndOffset: Long): Boolean = {
    getReplica(followerId) match {
      case Some(followerReplica) =>
        // 如果没有DeleteRecordsRequest延迟，则不需要计算低水位
        val oldLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
        val prevFollowerEndOffset = followerReplica.logEndOffset
        followerReplica.updateFetchState(
          followerFetchOffsetMetadata,
          followerStartOffset,
          followerFetchTimeMs,
          leaderEndOffset)

        val newLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
        // 检查分区的LW是否增加了，因为副本的logStartOffset可能已经增加了
        val leaderLWIncremented = newLeaderLW > oldLeaderLW

        // 检查是否需要将此同步副本添加到ISR中。
        maybeExpandIsr(followerReplica, followerFetchTimeMs)

        // 检查分区的HW现在是否可以增加，因为副本可能已经在ISR中，并且它的LEO刚刚增加
        val leaderHWIncremented = if (prevFollowerEndOffset != followerReplica.logEndOffset) {
          // leader日志可能会被ReplicaAlterLogDirsThread更新，所以下面的方法必须在leaderIsrUpdateLock的锁定中，
          // 以防止在无效日志中添加新的hw
          inReadLock(leaderIsrUpdateLock) {
            leaderLogIfLocal.exists(leaderLog => maybeIncrementLeaderHW(leaderLog, followerFetchTimeMs))
          }
        } else {
          false
        }

        // 更改HW或LW后，一些延迟的操作可能会被解除阻塞
        if (leaderLWIncremented || leaderHWIncremented)
          tryCompleteDelayedRequests()

        debug(s"Recorded replica $followerId log end offset (LEO) position " +
          s"${followerFetchOffsetMetadata.messageOffset} and log start offset $followerStartOffset.")
        true

      case None =>
        false
    }
  }

  /**
   * 存储主题分区分配和ISR。它为任何新的远程代理创建一个新的Replica对象。isr参数应该是赋值参数的一个子集。
   *
   * 注意:测试的公开可见性。
   *
   * @param assignment       An ordered sequence of all the broker ids that were assigned to this
   *                         topic partition
   * @param isr              The set of broker ids that are known to be insync with the leader
   * @param addingReplicas   An ordered sequence of all broker ids that will be added to the
   *                         assignment
   * @param removingReplicas An ordered sequence of all broker ids that will be removed from
   *                         the assignment
   */
  def updateAssignmentAndIsr(assignment: Seq[Int],
                             isr: Set[Int],
                             addingReplicas: Seq[Int],
                             removingReplicas: Seq[Int]): Unit = {
    val newRemoteReplicas = assignment.filter(_ != localBrokerId)
    val removedReplicas = remoteReplicasMap.keys.filter(!newRemoteReplicas.contains(_))

    // 由于代码路径访问remoteReplicasMap没有锁，首先添加新的副本，然后删除旧的副本
    newRemoteReplicas.foreach(id => remoteReplicasMap.getAndMaybePut(id, new Replica(id, topicPartition)))
    remoteReplicasMap.removeAll(removedReplicas)

    if (addingReplicas.nonEmpty || removingReplicas.nonEmpty)
      assignmentState = OngoingReassignmentState(addingReplicas, removingReplicas, assignment)
    else
      assignmentState = SimpleAssignmentState(assignment)
    isrState = CommittedIsr(isr)
  }

  /**
   * 检查并可能扩展分区的ISR。如果一个副本的LEO >=当前分区的hw，并且它在当前leader epoch内的偏移量被捕获，则该副本将被添加到ISR中。
   * 副本必须赶上当前leader epoch才能加入ISR，否则，如果当前leader的HW和LEO之间有提交的数据，副本可能在获取提交的数据之前就成为leader，
   * 导致数据丢失。从技术上讲，如果一个副本没有赶上的时间超过replicaLagTimeMaxMs，那么它就不应该在ISR中，即使它的日志端偏移量>= HW。
   * 但是，为了与follower判断副本是否同步的方式一致，我们只检查HW。当副本的LEO增加时，可以触发此函数。
   */
  private def maybeExpandIsr(followerReplica: Replica, followerFetchTimeMs: Long): Unit = {
    val needsIsrUpdate = canAddReplicaToIsr(followerReplica.brokerId) && inReadLock(leaderIsrUpdateLock) {
      needsExpandIsr(followerReplica)
    }
    if (needsIsrUpdate) {
      inWriteLock(leaderIsrUpdateLock) {
        // 检查该副本是否需要添加到ISR中
        if (needsExpandIsr(followerReplica)) {
          expandIsr(followerReplica.brokerId)
        }
      }
    }
  }

  private def needsExpandIsr(followerReplica: Replica): Boolean = {
    canAddReplicaToIsr(followerReplica.brokerId) && isFollowerAtHighwatermark(followerReplica)
  }

  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    val current = isrState
    !current.isInflight && !current.isr.contains(followerReplicaId)
  }

  private def isFollowerAtHighwatermark(followerReplica: Replica): Boolean = {
    leaderLogIfLocal.exists { leaderLog =>
      val followerEndOffset = followerReplica.logEndOffset
      followerEndOffset >= leaderLog.highWatermark && leaderEpochStartOffsetOpt.exists(followerEndOffset >= _)
    }
  }

  /*
   * 返回一个元组，其中第一个元素是一个布尔值，指示是否有足够的副本达到' requiredOffset '，第二个元素是一个错误(将是' Errors ')。
   * NONE(表示没有错误)。
   * 注意，只有在requiredAcks = -1时才会调用此方法，并且我们正在等待ISR中的所有副本完全赶上与此生产请求对应的(本地)
   * leader的偏移量，然后我们才会确认生产请求。

   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        // 保持当前不可变副本列表引用
        val curMaximalIsr = isrState.maximalIsr

        if (isTraceEnabled) {
          def logEndOffsetString: ((Int, Long)) => String = {
            case (brokerId, logEndOffset) => s"broker $brokerId: $logEndOffset"
          }

          val curInSyncReplicaObjects = (curMaximalIsr - localBrokerId).flatMap(getReplica)
          val replicaInfo = curInSyncReplicaObjects.map(replica => (replica.brokerId, replica.logEndOffset))
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffset)
          val (ackedReplicas, awaitingReplicas) = (replicaInfo + localLogInfo).partition {
            _._2 >= requiredOffset
          }

          trace(s"Progress awaiting ISR acks for offset $requiredOffset: " +
            s"acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }

        val minIsr = leaderLog.config.minInSyncReplicas
        if (leaderLog.highWatermark >= requiredOffset) {
          /*
           * 如果ISR中没有足够的副本，则可以将主题配置为不接受消息。在这种情况下，请求已经在本地追加，然后在ISR缩小之前添加到shrunk
           */
          if (minIsr <= curMaximalIsr.size)
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_OR_FOLLOWER)
    }
  }

  /**
   * 检查并可能增加分区的高水位;
   * 1.触发该功能。分区ISR更改
   * 2。任何副本的LEO更改HW由所有同步或被认为是赶上的副本中最小的日志端偏移量确定。
   * 这样，如果一个副本被认为是赶上的，但是它的日志端偏移量小于HW，我们将等待这个副本赶上HW，然后再推进HW。
   * 当ISR只包括leader副本而follower试图追赶时，这有助于解决这种情况。如果我们在推进HW时不等待follower,
   * follower的日志端偏移量可能会一直落后于HW(由leader的日志端偏移量决定)，因此永远不会被添加到ISR中。
   * 随着AlterIsr的加入，我们在推进HW时也会考虑将新添加的副本作为ISR的一部分。这些副本还没有被控制器提交到ISR，
   * 所以我们可以恢复到以前提交的ISR。然而，向ISR中添加额外的副本使其更具限制性，因此更安全。我们称这个集合为“最大”ISR。
   * 注意这里不需要获取leaderIsrUpdate锁，因为这个私有API的所有调用者都获得了该锁
   * @return 如果HW是递增的，则为true，否则为false。
   */
  private def maybeIncrementLeaderHW(leaderLog: Log, curTime: Long = time.milliseconds): Boolean = {
    // maybeIncrementLeaderHW is in the hot path, the following code is written to
    // maybeIncrementLeaderHW处于热路径中，则编写以下代码以避免不必要的收集生成
    var newHighWatermark = leaderLog.logEndOffsetMetadata
    remoteReplicasMap.values.foreach { replica =>
      // 注意这里我们使用的是“最大值”，见上面的解释
      if (replica.logEndOffsetMetadata.messageOffset < newHighWatermark.messageOffset &&
        (curTime - replica.lastCaughtUpTimeMs <= replicaLagTimeMaxMs || isrState.maximalIsr.contains(replica.brokerId))) {
        newHighWatermark = replica.logEndOffsetMetadata
      }
    }

    leaderLog.maybeIncrementHighWatermark(newHighWatermark) match {
      case Some(oldHighWatermark) =>
        debug(s"High watermark updated from $oldHighWatermark to $newHighWatermark")
        true

      case None =>
        def logEndOffsetString: ((Int, LogOffsetMetadata)) => String = {
          case (brokerId, logEndOffsetMetadata) => s"replica $brokerId: $logEndOffsetMetadata"
        }

        if (isTraceEnabled) {
          val replicaInfo = remoteReplicas.map(replica => (replica.brokerId, replica.logEndOffsetMetadata)).toSet
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffsetMetadata)
          trace(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old value. " +
            s"All current LEOs are ${(replicaInfo + localLogInfo).map(logEndOffsetString)}")
        }
        false
    }
  }

  /**
   * 低水位偏移值，仅在本地副本是分区leader时计算。leader broker仅使用该值来决定DeleteRecordsRequest是否满足。
   * 当leader broker接收到FetchRequest或DeleteRecordsRequest时，Low watermark会增加。
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeader)
      throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")

    // 当DeleteRecordsRequest未完成时，可能会多次调用lowWatermarkIfLeader，注意避免在此代码中生成不必要的集合
    var lowWaterMark = localLogOrException.logStartOffset
    remoteReplicas.foreach { replica =>
      if (metadataCache.hasAliveBroker(replica.brokerId) && replica.logStartOffset < lowWaterMark) {
        lowWaterMark = replica.logStartOffset
      }
    }

    futureLog match {
      case Some(partitionFutureLog) =>
        Math.min(lowWaterMark, partitionFutureLog.logStartOffset)
      case None =>
        lowWaterMark
    }
  }

  /**
   * 尝试完成所有挂起的请求。它应该在不持有leaderIsrUpdateLock的情况下被调用。
   */
  private def tryCompleteDelayedRequests(): Unit = delayedOperations.checkAndCompleteAll()

  def maybeShrinkIsr(): Unit = {
    val needsIsrUpdate = !isrState.isInflight && inReadLock(leaderIsrUpdateLock) {
      needsShrinkIsr()
    }
    val leaderHWIncremented = needsIsrUpdate && inWriteLock(leaderIsrUpdateLock) {
      leaderLogIfLocal.exists { leaderLog =>
        val outOfSyncReplicaIds = getOutOfSyncReplicas(replicaLagTimeMaxMs)
        if (outOfSyncReplicaIds.nonEmpty) {
          val outOfSyncReplicaLog = outOfSyncReplicaIds.map { replicaId =>
            val logEndOffsetMessage = getReplica(replicaId)
              .map(_.logEndOffset.toString)
              .getOrElse("unknown")
            s"(brokerId: $replicaId, endOffset: $logEndOffsetMessage)"
          }.mkString(" ")
          val newIsrLog = (isrState.isr -- outOfSyncReplicaIds).mkString(",")
          info(s"Shrinking ISR from ${isrState.isr.mkString(",")} to $newIsrLog. " +
            s"Leader: (highWatermark: ${leaderLog.highWatermark}, " +
            s"endOffset: ${leaderLog.logEndOffset}). " +
            s"Out of sync replicas: $outOfSyncReplicaLog.")

          shrinkIsr(outOfSyncReplicaIds)

          // 我们可能需要增加高水位，因为ISR可以降低到1
          maybeIncrementLeaderHW(leaderLog)
        } else {
          false
        }
      }
    }

    // HW改变后，一些延迟的操作可能会被解除
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  private def needsShrinkIsr(): Boolean = {
    leaderLogIfLocal.exists { _ => getOutOfSyncReplicas(replicaLagTimeMaxMs).nonEmpty }
  }

  private def isFollowerOutOfSync(replicaId: Int,
                                  leaderEndOffset: Long,
                                  currentTimeMs: Long,
                                  maxLagMs: Long): Boolean = {
    getReplica(replicaId).fold(true) { followerReplica =>
      followerReplica.logEndOffset != leaderEndOffset &&
        (currentTimeMs - followerReplica.lastCaughtUpTimeMs) > maxLagMs
    }
  }

  /**
   * 如果follower已经和leader有相同的leo，则不会被认为是不同步的，否则这里会处理两种情况- 1。
   * 卡住的追随者:如果副本的leo在maxLagMs ms内没有更新，那么追随者就卡住了，应该从ISR 2中移除。
   * 缓慢的追随者:如果副本在最后maxLagMs ms内没有读取到leo，那么追随者是滞后的，应该从ISR中删除。
   * 这两种情况都通过检查lastCaughtUpTimeMs来处理，它表示副本被完全捕获的最后时间。如果违反上述任何一个条件，则认为该副本不同步。
   * 如果正在进行ISR更新，我们将在这里返回一个空集
   * */
  def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
    val current = isrState
    if (!current.isInflight) {
      val candidateReplicaIds = current.isr - localBrokerId
      val currentTimeMs = time.milliseconds()
      val leaderEndOffset = localLogOrException.logEndOffset
      candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))
    } else {
      Set.empty
    }
  }

  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    if (isFuture) {
      // 如果请求处理程序线程在收到AlterReplicaLogDirsRequest后试图删除未来的副本，则需要读锁来处理竞争条件。
      inReadLock(leaderIsrUpdateLock) {
        // 注意，如果在调用此方法之前由非non-ReplicaAlterLogDirsThread删除副本，则副本可能未定义
        futureLog.map {
          _.appendAsFollower(records)
        }
      }
    } else {
      // 当ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()用未来副本替换追随者副本时，
      // 需要锁来防止追随者副本被更新。
      futureLogLock.synchronized {
        Some(localLogOrException.appendAsFollower(records))
      }
    }
  }

  def appendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    try {
      doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        val log = if (isFuture) futureLocalLogOrException else localLogOrException
        val logEndOffset = log.logEndOffset
        if (logEndOffset == log.logStartOffset &&
          e.firstOffset < logEndOffset && e.lastOffset >= logEndOffset) {
          // 如果leader(或当前副本)上的日志开始偏移量由于删除记录请求而落在批处理的中间，
          // 并且follower试图从leader获取其第一个偏移量，则可能发生这种情况。我们在这里处理这种情况，而不是Logappend()，
          // 因为我们需要删除以日志开始偏移量开始的段，并创建一个具有较早偏移量(批处理的基本偏移量)的新段，这将向后移动recoveryPoint，
          // 因此我们需要在追加之前检查点新的恢复点
          val replicaName = if (isFuture) "future replica" else "follower"
          info(s"Unexpected offset in append to $topicPartition. First offset ${e.firstOffset} is less than log start offset ${log.logStartOffset}." +
            s" Since this is the first record to be appended to the $replicaName's log, will start the log from offset ${e.firstOffset}.")
          truncateFullyAndStartAt(e.firstOffset, isFuture)
          doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
        } else
          throw e
    }
  }

  def appendRecordsToLeader(records: MemoryRecords, origin: AppendOrigin, requiredAcks: Int,
                            requestLocal: RequestLocal): LogAppendInfo = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal match {
        case Some(leaderLog) =>
          val minIsr = leaderLog.config.minInSyncReplicas
          val inSyncSize = isrState.isr.size

          // 如果没有足够的同步副本来保证安全，请避免写信给leader
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException(s"The size of the current ISR ${isrState.isr} " +
              s"is insufficient to satisfy the min.isr requirement of $minIsr for partition $topicPartition")
          }

          val info = leaderLog.appendAsLeader(records, leaderEpoch = this.leaderEpoch, origin,
            interBrokerProtocolVersion, requestLocal)

          // 我们可能需要增加高水位，因为ISR可以降低到1
          (info, maybeIncrementLeaderHW(leaderLog))

        case None =>
          throw new NotLeaderOrFollowerException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    info.copy(leaderHwChange = if (leaderHWIncremented) LeaderHwChange.Increased else LeaderHwChange.Same)
  }

  def readRecords(lastFetchedEpoch: Optional[Integer],
                  fetchOffset: Long,
                  currentLeaderEpoch: Optional[Integer],
                  maxBytes: Int,
                  fetchIsolation: FetchIsolation,
                  fetchOnlyFromLeader: Boolean,
                  minOneMessage: Boolean): LogReadInfo = inReadLock(leaderIsrUpdateLock) {
    // 决定是否只从leader取回
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    // 注意，我们在读取之前使用了日志端偏移量。这确保了fetch之后的任何追加都不会阻止follower进入同步。
    val initialHighWatermark = localLog.highWatermark
    val initialLogStartOffset = localLog.logStartOffset
    val initialLogEndOffset = localLog.logEndOffset
    val initialLastStableOffset = localLog.lastStableOffset

    lastFetchedEpoch.ifPresent { fetchEpoch =>
      val epochEndOffset = lastOffsetForLeaderEpoch(currentLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)
      val error = Errors.forCode(epochEndOffset.errorCode)
      if (error != Errors.NONE) {
        throw error.exception()
      }

      if (epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH) {
        throw new OffsetOutOfRangeException("Could not determine the end offset of the last fetched epoch " +
          s"$lastFetchedEpoch from the request")
      }

      // 如果取偏移量小于log start，不管epoch是否发散，使用OffsetOutOfRangeException都会失败
      if (fetchOffset < initialLogStartOffset) {
        throw new OffsetOutOfRangeException(s"Received request for offset $fetchOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $initialLogStartOffset to $initialLogEndOffset.")
      }

      if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchOffset) {
        val emptyFetchData = FetchDataInfo(
          fetchOffsetMetadata = LogOffsetMetadata(fetchOffset),
          records = MemoryRecords.EMPTY,
          firstEntryIncomplete = false,
          abortedTransactions = None
        )

        val divergingEpoch = new FetchResponseData.EpochEndOffset()
          .setEpoch(epochEndOffset.leaderEpoch)
          .setEndOffset(epochEndOffset.endOffset)

        return LogReadInfo(
          fetchedData = emptyFetchData,
          divergingEpoch = Some(divergingEpoch),
          highWatermark = initialHighWatermark,
          logStartOffset = initialLogStartOffset,
          logEndOffset = initialLogEndOffset,
          lastStableOffset = initialLastStableOffset)
      }
    }

    val fetchedData = localLog.read(fetchOffset, maxBytes, fetchIsolation, minOneMessage)
    LogReadInfo(
      fetchedData = fetchedData,
      divergingEpoch = None,
      highWatermark = initialHighWatermark,
      logStartOffset = initialLogStartOffset,
      logEndOffset = initialLogEndOffset,
      lastStableOffset = initialLastStableOffset)
  }

  def fetchOffsetForTimestamp(timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = inReadLock(leaderIsrUpdateLock) {
    // 决定是否只从leader取回
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    val lastFetchableOffset = isolationLevel match {
      case Some(IsolationLevel.READ_COMMITTED) => localLog.lastStableOffset
      case Some(IsolationLevel.READ_UNCOMMITTED) => localLog.highWatermark
      case None => localLog.logEndOffset
    }

    val epochLogString = if (currentLeaderEpoch.isPresent) {
      s"epoch ${currentLeaderEpoch.get}"
    } else {
      "unknown epoch"
    }

    // 只有当我们收到客户端请求(定义了isolationLevel)并且开始偏移量落后于高水位时才考虑抛出错误
    val maybeOffsetsError: Option[ApiException] = leaderEpochStartOffsetOpt
      .filter(epochStart => isolationLevel.isDefined && epochStart > localLog.highWatermark)
      .map(epochStart => Errors.OFFSET_NOT_AVAILABLE.exception(s"Failed to fetch offsets for " +
        s"partition $topicPartition with leader $epochLogString as this partition's " +
        s"high watermark (${localLog.highWatermark}) is lagging behind the " +
        s"start offset from the beginning of this epoch ($epochStart)."))

    def getOffsetByTimestamp: Option[TimestampAndOffset] = {
      logManager.getLog(topicPartition).flatMap(log => log.fetchOffsetByTimestamp(timestamp))
    }

    // 如果我们在leader选举后处于滞后HW状态，抛出OffsetNotAvailable用于“最新”偏移量或时间戳查找超出最后可获取偏移量。
    timestamp match {
      case ListOffsetsRequest.LATEST_TIMESTAMP =>
        maybeOffsetsError.map(e => throw e)
          .orElse(Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch))))
      case ListOffsetsRequest.EARLIEST_TIMESTAMP =>
        getOffsetByTimestamp
      case _ =>
        getOffsetByTimestamp.filter(timestampAndOffset => timestampAndOffset.offset < lastFetchableOffset)
          .orElse(maybeOffsetsError.map(e => throw e))
    }
  }

  def activeProducerState: DescribeProducersResponseData.PartitionResponse = {
    val producerState = new DescribeProducersResponseData.PartitionResponse()
      .setPartitionIndex(topicPartition.partition())

    log.map(_.activeProducers) match {
      case Some(producers) =>
        producerState
          .setErrorCode(Errors.NONE.code)
          .setActiveProducers(producers.asJava)
      case None =>
        producerState
          .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
    }

    producerState
  }

  def fetchOffsetSnapshot(currentLeaderEpoch: Optional[Integer],
                          fetchOnlyFromLeader: Boolean): LogOffsetSnapshot = inReadLock(leaderIsrUpdateLock) {
    // 决定是否只从leader取回
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)
    localLog.fetchOffsetSnapshot
  }

  def legacyFetchOffsetsForTimestamp(timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = inReadLock(leaderIsrUpdateLock) {
    val localLog = localLogWithEpochOrException(Optional.empty(), fetchOnlyFromLeader)
    val allOffsets = localLog.legacyFetchOffsetsBefore(timestamp, maxNumOffsets)

    if (!isFromConsumer) {
      allOffsets
    } else {
      val hw = localLog.highWatermark
      if (allOffsets.exists(_ > hw))
        hw +: allOffsets.dropWhile(_ > hw)
      else
        allOffsets
    }
  }

  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal.map(_.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * 如果
   * 1)offset <= highWatermark
   * 并且
   * 2)它是leader replica，则更新logStartOffset和low watermark。该功能可以触发日志段删除和日志滚动。
   *
   * 返回 分区低水位。
   */
  def deleteRecordsOnLeader(offset: Long): LogDeleteRecordsResult = inReadLock(leaderIsrUpdateLock) {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        if (!leaderLog.config.delete)
          throw new PolicyViolationException(s"Records of partition $topicPartition can not be deleted due to the configured policy")

        val convertedOffset = if (offset == DeleteRecordsRequest.HIGH_WATERMARK)
          leaderLog.highWatermark
        else
          offset

        if (convertedOffset < 0)
          throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

        leaderLog.maybeIncrementLogStartOffset(convertedOffset, ClientRecordDeletion)
        LogDeleteRecordsResult(
          requestedOffset = convertedOffset,
          lowWatermark = lowWatermarkIfLeader)
      case None =>
        throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
    }
  }

  /**
   * 将此分区的本地日志截断到指定的偏移量，并将恢复点检查点设置为该偏移量
   *
   * @param offset   用于截断的偏移量
   * @param isFuture 如果应该对该分区的未来日志执行截断，则为True
   */
  def truncateTo(offset: Long, isFuture: Boolean): Unit = {
    // 当ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()用未来副本替换追随者副本时，
    // 需要读锁来防止追随者副本被截断。
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
   * 删除该分区的本地日志中的所有数据，并在新的偏移量处开始日志
   *
   * @param newOffset 开始日志的新偏移量
   * @param isFuture  如果应该对该分区的未来日志执行截断，则为True
   */
  def truncateFullyAndStartAt(newOffset: Long, isFuture: Boolean): Unit = {
    // 当ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()用未来副本替换追随者副本时，
    // 需要读锁来防止追随者副本被截断。
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture)
    }
  }

  /**
   * 查找小于或等于请求epoch的最大epoch的(独占)最后偏移量。
   *
   * @param currentLeaderEpoch  The expected epoch of the current leader (if known)
   * @param leaderEpoch         Requested leader epoch
   * @param fetchOnlyFromLeader 是否只需要领导者的服务
   * @return 请求的领导纪元和这个领导纪元的结束偏移量，或者如果请求的领导纪元未知，领导纪元小于请求的领导纪元和这个领导纪元的结束偏移量。
   *         leader epoch的结束偏移量定义为大于leader epoch的第一个leader epoch的开始偏移量，如果leader epoch是最新的
   *         leader epoch，则为日志结束偏移量。
   */
  def lastOffsetForLeaderEpoch(currentLeaderEpoch: Optional[Integer],
                               leaderEpoch: Int,
                               fetchOnlyFromLeader: Boolean): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      val localLogOrError = getLocalLog(currentLeaderEpoch, fetchOnlyFromLeader)
      localLogOrError match {
        case Left(localLog) =>
          localLog.endOffsetForEpoch(leaderEpoch) match {
            case Some(epochAndOffset) => new EpochEndOffset()
              .setPartition(partitionId)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(epochAndOffset.leaderEpoch)
              .setEndOffset(epochAndOffset.offset)
            case None => new EpochEndOffset()
              .setPartition(partitionId)
              .setErrorCode(Errors.NONE.code)
          }
        case Right(error) => new EpochEndOffset()
          .setPartition(partitionId)
          .setErrorCode(error.code)
      }
    }
  }

  private[cluster] def expandIsr(newInSyncReplica: Int): Unit = {
    // 这是从持有ISR写锁的maybeExpandIsr调用的
    if (!isrState.isInflight) {
      //当扩展ISR时，我们可以放心地假设新的副本将进入ISR，因为这使我们在推进HW时处于更受约束的状态。
      sendAlterIsrRequest(PendingExpandIsr(isrState.isr, newInSyncReplica))
    } else {
      trace(s"ISR update in-flight, not adding new in-sync replica $newInSyncReplica")
    }
  }

  private[cluster] def shrinkIsr(outOfSyncReplicas: Set[Int]): Unit = {
    // This is called from maybeShrinkIsr which holds the ISR write lock
    if (!isrState.isInflight) {
      // 当收缩ISR时，我们不能假设更新会成功，因为这可能会错误地推进HW。我们在这里更新pendingInSyncReplicaIds
      // 只是为了防止任何进一步的ISR更新发生，直到我们得到下一个LeaderAndIsr
      sendAlterIsrRequest(PendingShrinkIsr(isrState.isr, outOfSyncReplicas))
    } else {
      trace(s"ISR update in-flight, not removing out-of-sync replicas $outOfSyncReplicas")
    }
  }

  private def sendAlterIsrRequest(proposedIsrState: IsrState): Unit = {
    val isrToSend: Set[Int] = proposedIsrState match {
      case PendingExpandIsr(isr, newInSyncReplicaId) => isr + newInSyncReplicaId
      case PendingShrinkIsr(isr, outOfSyncReplicaIds) => isr -- outOfSyncReplicaIds
      case state =>
        isrChangeListener.markFailed()
        throw new IllegalStateException(s"Invalid state $state for ISR change for partition $topicPartition")
    }

    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, isrToSend.toList, zkVersion)
    val alterIsrItem = AlterIsrItem(topicPartition, newLeaderAndIsr, handleAlterIsrResponse(proposedIsrState), controllerEpoch)

    val oldState = isrState
    isrState = proposedIsrState

    if (!alterIsrManager.submit(alterIsrItem)) {
      // 如果ISR管理器不接受我们的更新，我们需要恢复提议的状态。如果ISR状态由控制器更新(通过zk模式下的
      // LeaderAndIsr或KRaft模式下的ChangePartitionRecord)，但我们仍有一个AlterIsr请求，则可能发生这种情况。
      isrState = oldState
      isrChangeListener.markFailed()
      warn(s"Failed to enqueue ISR change state $newLeaderAndIsr for partition $topicPartition")
    } else {
      debug(s"Enqueued ISR change to state $newLeaderAndIsr after transition to $proposedIsrState")
    }
  }

  /**
   * 在AlterIsr响应的主体中对每个分区调用此方法。对于不可重试的错误，我们干脆放弃。
   * 这就剩下了[Partition.isrState]处于飞行状态(挂起收缩或挂起扩展)。由于我们的错误是不可重试的，所以我们可以保持这种状态，
   * 直到我们看到来自UpdateMetadata或LeaderAndIsr的新元数据
   */
  private def handleAlterIsrResponse(proposedIsrState: IsrState)(result: Either[Errors, LeaderAndIsr]): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      if (isrState != proposedIsrState) {
        // 这意味着在我们得到AlterIsr响应之前，isrState已经通过leader选举或其他机制进行了更新。我们不知道控制器上到底发生了什么，
        // 但我们知道这个响应是过时的，所以我们忽略它。
        debug(s"Ignoring failed ISR update to $proposedIsrState since we have already updated state to $isrState")
        return
      }

      result match {
        case Left(error: Errors) =>
          isrChangeListener.markFailed()
          error match {
            case Errors.UNKNOWN_TOPIC_OR_PARTITION =>
              debug(s"Failed to update ISR to $proposedIsrState since it doesn't know about this topic or partition. Giving up.")
            case Errors.FENCED_LEADER_EPOCH =>
              debug(s"Failed to update ISR to $proposedIsrState since we sent an old leader epoch. Giving up.")
            case Errors.INVALID_UPDATE_VERSION =>
              debug(s"Failed to update ISR to $proposedIsrState due to invalid version. Giving up.")
            case _ =>
              warn(s"Failed to update ISR to $proposedIsrState due to unexpected $error. Retrying.")
              sendAlterIsrRequest(proposedIsrState)
          }
        case Right(leaderAndIsr: LeaderAndIsr) =>
          // 成功从控制器，还需要检查一些东西
          if (leaderAndIsr.leaderEpoch != leaderEpoch) {
            debug(s"Ignoring new ISR ${leaderAndIsr} since we have a stale leader epoch $leaderEpoch.")
            isrChangeListener.markFailed()
          } else if (leaderAndIsr.zkVersion < zkVersion) {
            debug(s"Ignoring new ISR ${leaderAndIsr} since we have a newer version $zkVersion.")
            isrChangeListener.markFailed()
          } else {
            // 这是两种状态之一:
            // 1)领导状态和r状态。zkVersion > zkVersion:控制器更新到新版本，并带有proposedIsrState。
            // 2) leaderAndIsr。zkVersion == zkVersion:建议状态和实际状态一致，没有进行更新。在这两种情况下，
            // 我们都希望从Pending状态转移到Committed状态，以确保新的更新得到处理。

            isrState = CommittedIsr(leaderAndIsr.isr.toSet)
            zkVersion = leaderAndIsr.zkVersion
            info(s"ISR updated to ${isrState.isr.mkString(",")} and version updated to [$zkVersion]")
            proposedIsrState match {
              case PendingExpandIsr(_, _) => isrChangeListener.markExpand()
              case PendingShrinkIsr(_, _) => isrChangeListener.markShrink()
              case _ => // nothing to do, shouldn't get here
            }
          }
      }
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; Replicas: " + assignmentState.replicas.mkString(","))
    partitionString.append("; ISR: " + isrState.isr.mkString(","))
    assignmentState match {
      case OngoingReassignmentState(adding, removing, _) =>
        partitionString.append("; AddingReplicas: " + adding.mkString(","))
        partitionString.append("; RemovingReplicas: " + removing.mkString(","))
      case _ =>
    }
    partitionString.toString
  }
}
