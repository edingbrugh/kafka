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

import java.io.File
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.common.RecordValidationException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchMetadata => SFetchMetadata}
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.{DescribeLogDirsResponseData, DescribeProducersResponseData, FetchResponseData, LeaderAndIsrResponseData}
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica.{ClientMetadata, _}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.image.{LocalReplicaChanges, MetadataImage, TopicsDelta}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.OptionConverters._

/*
 * 日志追加操作的结果元数据
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/**
 * 对日志进行读操作的结果元数据
 * @param info @FetchDataInfo returned by the @Log read
 * @param divergingEpoch Optional epoch and end offset which indicates the largest epoch such
 *                       that subsequent records are known to diverge on the follower/consumer
 * @param highWatermark high watermark of the local replica
 * @param leaderLogStartOffset The log start offset of the leader at the time of the read
 * @param leaderLogEndOffset The log end offset of the leader at the time of the read
 * @param followerLogStartOffset The log start offset of the follower taken from the Fetch request
 * @param fetchTimeMs The time the fetch was received
 * @param lastStableOffset Current LSO or None if the result has an exception
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param exception Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         lastStableOffset: Option[Long],
                         preferredReadReplica: Option[Int] = None,
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def toFetchPartitionData(isReassignmentFetch: Boolean): FetchPartitionData = FetchPartitionData(
    this.error,
    this.highWatermark,
    this.leaderLogStartOffset,
    this.info.records,
    this.divergingEpoch,
    this.lastStableOffset,
    this.info.abortedTransactions,
    this.preferredReadReplica,
    isReassignmentFetch)

  def withEmptyFetchInfo: LogReadResult =
    copy(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY))

  override def toString = {
    "LogReadResult(" +
      s"info=$info, " +
      s"divergingEpoch=$divergingEpoch, " +
      s"highWatermark=$highWatermark, " +
      s"leaderLogStartOffset=$leaderLogStartOffset, " +
      s"leaderLogEndOffset=$leaderLogEndOffset, " +
      s"followerLogStartOffset=$followerLogStartOffset, " +
      s"fetchTimeMs=$fetchTimeMs, " +
      s"preferredReadReplica=$preferredReadReplica, " +
      s"lastStableOffset=$lastStableOffset, " +
      s"error=$error" +
      ")"
  }

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[FetchResponseData.AbortedTransaction]],
                              preferredReadReplica: Option[Int],
                              isReassignmentFetch: Boolean)

/**
 * 属性来表示托管分区的状态。当代理接收到来自控制器的LeaderAndIsr请求或来自Quorum控制器的元数据日志记录，表明代理应该是分区的领导者或追随者时，
 * 我们创建一个具体的(活动的)分区实例

 */
sealed trait HostedPartition

object HostedPartition {
  /**
   * 此代理在本地没有此分区的任何状态
   */
  final object None extends HostedPartition

  /**
   * 这个代理托管分区，并且它是在线的。
   */
  final case class Online(partition: Partition) extends HostedPartition

  /**
   * 该代理托管分区，但它位于脱机日志目录中。
   */
  final object Offline extends HostedPartition
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     val zkClient: Option[KafkaZkClient],
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     quotaManagers: QuotaManagers,
                     val brokerTopicStats: BrokerTopicStats,
                     val metadataCache: MetadataCache,
                     logDirFailureChannel: LogDirFailureChannel,
                     val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                     val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                     val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                     val delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                     threadNamePrefix: Option[String],
                     val alterIsrManager: AlterIsrManager) extends Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           zkClient: Option[KafkaZkClient],
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           alterIsrManager: AlterIsrManager,
           threadNamePrefix: Option[String] = None) = {
    this(config, metrics, time, zkClient, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedElectLeader](
        purgatoryName = "ElectLeader", brokerId = config.brokerId),
      threadNamePrefix, alterIsrManager)
  }

  /* 控制器的纪元最后一次改变了领导者 */
  @volatile private[server] var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  protected val localBrokerId = config.brokerId
  protected val allPartitions = new Pool[TopicPartition, HostedPartition](
    valueFactory = Some(tp => HostedPartition.Online(Partition(tp, time, this)))
  )
  protected val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  private[server] val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile private[server] var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  protected val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  private var logDirFailureHandler: LogDirFailureHandler = null

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  private[server] val replicaSelectorOpt: Option[ReplicaSelector] = createReplicaSelector()

  newGauge("LeaderCount", () => leaderPartitionsIterator.size)
  private[kafka] val partitionCount = newGauge("PartitionCount", () => allPartitions.size)
  newGauge("OfflineReplicaCount", () => offlinePartitionCount)
  newGauge("UnderReplicatedPartitions", () => underReplicatedPartitionCount)
  newGauge("UnderMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  newGauge("AtMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isAtMinIsr))
  newGauge("ReassigningPartitions", () => reassigningPartitionsCount)

  def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

  val isrExpandRate: Meter = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  // 当ReplicaAlterDirThread用未来的副本替换完当前副本时，它将从分区状态映射中删除该分区。但是，即使分区状态映射为空，它也不会关闭自己。
  // 因此，我们需要定期调用shutdownIdleReplicaAlterDirThread()来关闭空闲的ReplicaAlterDirThread
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  def hasDelayedElectionOperations: Boolean = delayedElectLeaderPurgatory.numDelayed != 0

  def tryCompleteElection(key: DelayedOperationKey): Unit = {
    val completed = delayedElectLeaderPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d ElectLeader.".format(key.keyLabel, completed))
  }

  def startup(): Unit = {
    // 启动ISR过期线程一个follower可以落后leader最长配置。replicaLagTimeMaxMs x 1.5，然后从ISR中删除
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", shutdownIdleReplicaAlterLogDirsThread _, period = 10000L, unit = TimeUnit.MILLISECONDS)


    // 如果IBP < 1.0，控制器将发送不包含isNew字段的LeaderAndIsrRequest V0。在这种情况下，如果日志目录失败，
    // 接收请求的代理无法确定创建分区是否安全。因此，如果IBP < 1.0，我们选择在任何日志目录失败时停止代理
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasNonOfflinePartition = allPartitions.values.exists {
      case online: HostedPartition.Online => topic == online.partition.topic
      case HostedPartition.None | HostedPartition.Offline => false
    }
    if (!topicHasNonOfflinePartition)
      brokerTopicStats.removeMetrics(topic)
  }

  protected def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit = {
    val topicPartitionOperationKey = TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
  }

  def stopReplicas(correlationId: Int,
                   controllerId: Int,
                   controllerEpoch: Int,
                   partitionStates: Map[TopicPartition, StopReplicaPartitionState]
                  ): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(s"Handling StopReplica request correlationId $correlationId from controller " +
        s"$controllerId for ${partitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        partitionStates.forKeyValue { (topicPartition, partitionState) =>
          stateChangeLogger.trace(s"Received StopReplica request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch $controllerEpoch for partition $topicPartition")
        }

      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      if (controllerEpoch < this.controllerEpoch) {
        stateChangeLogger.warn(s"Ignoring StopReplica request from " +
          s"controller $controllerId with correlation id $correlationId " +
          s"since its controller epoch $controllerEpoch is old. " +
          s"Latest known controller epoch is ${this.controllerEpoch}")
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        this.controllerEpoch = controllerEpoch

        val stoppedPartitions = mutable.Map.empty[TopicPartition, Boolean]
        partitionStates.forKeyValue { (topicPartition, partitionState) =>
          val deletePartition = partitionState.deletePartition()

          getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)

            case HostedPartition.Online(partition) =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              // 当一个主题被删除时，leader epoch不会增加。为了避免这种情况，使用一个哨兵值(EpochDuringDelete)覆盖任何以前的纪元。
              // 当旧版本的StopReplica请求不包含领导epoch时，使用哨兵值(NoEpoch)并绕过epoch验证。
              if (requestLeaderEpoch == LeaderAndIsr.EpochDuringDelete ||
                  requestLeaderEpoch == LeaderAndIsr.NoEpoch ||
                  requestLeaderEpoch > currentLeaderEpoch) {
                stoppedPartitions += topicPartition -> deletePartition
                // 假设一切都会顺利进行。如果发生错误，它将被覆盖。
                responseMap.put(topicPartition, Errors.NONE)
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              } else {
                stateChangeLogger.info(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              }

            case HostedPartition.None =>
              // 删除日志和相应的文件夹，以防副本管理器不再保存它们。这可能发生在主题被删除时，而代理关闭并恢复。
              stoppedPartitions += topicPartition -> deletePartition
              responseMap.put(topicPartition, Errors.NONE)
          }
        }

        stopPartitions(stoppedPartitions).foreach { case (topicPartition, e) =>
          if (e.isInstanceOf[KafkaStorageException]) {
              stateChangeLogger.error(s"Ignoring StopReplica request (delete=true) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
          } else {
            stateChangeLogger.error(s"Ignoring StopReplica request (delete=true) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition due to an unexpected " +
                s"${e.getClass.getName} exception: ${e.getMessage}")
          }
          responseMap.put(topicPartition, Errors.forException(e))
        }
        (responseMap, Errors.NONE)
      }
    }
  }

  /**
   * 停止给定的分区。
   *
   * @param partitionsToStop    从主题分区到布尔值的映射，该布尔值指示是否应该删除该分区。
   *
   * @return                    从分区到发生异常的映射。如果没有错误发生，映射将为空。
   */
  protected def stopPartitions(
    partitionsToStop: Map[TopicPartition, Boolean]
  ): Map[TopicPartition, Throwable] = {
    // 首先停止所有分区的获取器。
    val partitions = partitionsToStop.keySet
    replicaFetcherManager.removeFetcherForPartitions(partitions)
    replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)

    // 其次，从分区映射中删除已删除的分区。Fetchers依赖于ReplicaManager来获取Partition的信息，因此必须先停止它们。
    val partitionsToDelete = mutable.Set.empty[TopicPartition]
    partitionsToStop.forKeyValue { (topicPartition, shouldDelete) =>
      if (shouldDelete) {
        getPartition(topicPartition) match {
          case hostedPartition: HostedPartition.Online =>
            if (allPartitions.remove(topicPartition, hostedPartition)) {
              maybeRemoveTopicMetrics(topicPartition.topic)
              // 此处不删除日志。它们稍后将在单个批处理中删除。这样做是为了避免每次删除都要检查检查点。
              hostedPartition.partition.delete()
            }

          case _ =>
        }
        partitionsToDelete += topicPartition
      }
      // 如果我们是领导者，我们可能还有一些操作等待完成。我们强制完成以防止它们超时。
      completeDelayedFetchOrProduceRequests(topicPartition)
    }

    // 第三，删除日志和检查点。
    val errorMap = new mutable.HashMap[TopicPartition, Throwable]()
    if (partitionsToDelete.nonEmpty) {
      // 删除日志和检查点。
      logManager.asyncDelete(partitionsToDelete, (tp, e) => errorMap.put(tp, e))
    }
    errorMap
  }

  def getPartition(topicPartition: TopicPartition): HostedPartition = {
    allPartitions.keys.foreach( v => println("======key========== " + v + "========value=========" + allPartitions.get(v)))
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartition.None)
  }

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }


  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
    partition
  }

  def onlinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) => Some(partition)
      case _ => None
    }
  }

  // 所有非脱机分区上的迭代器。这是一个弱一致迭代器;在构造迭代器之后脱机的分区仍然可以由该迭代器返回。
  private def onlinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case _ => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.iterator.count(_ == HostedPartition.Offline)
  }

  def getPartitionOrException(topicPartition: TopicPartition): Partition = {
    getPartitionOrError(topicPartition) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  def getPartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        // 主题是存在的，但是这个代理不再是它的副本，所以我们返回NOT_LEADER_OR_FOLLOWER，这将强制客户端刷新元数据以找到新的位置。
        // 例如，在分区重新分配期间，如果在删除本地副本之后将来自客户机的农产品请求发送给代理，则可能发生这种情况。
        Left(Errors.NOT_LEADER_OR_FOLLOWER)

      case HostedPartition.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  def localLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition).localLogOrException
  }

  def futureLocalLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition).futureLocalLogOrException
  }

  def futureLogExists(topicPartition: TopicPartition): Boolean = {
    getPartitionOrException(topicPartition).futureLog.isDefined
  }

  def localLog(topicPartition: TopicPartition): Option[Log] = {
    onlinePartition(topicPartition).flatMap(_.log)
  }

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    localLog(topicPartition).map(_.parentDir)
  }

  /**
   * TODO: move this action queue to handle thread so we can simplify concurrency handling
   */
  private val actionQueue = new ActionQueue

  def tryCompleteActions(): Unit = actionQueue.tryCompleteActions()

  /**
   * 向分区的主副本附加消息，并等待它们被复制到其他副本;当超时或满足所需的ack时，将触发回调函数;如果回调函数本身已经在某个对象上同步，
   * 则传递该对象以避免死锁。请注意，所有挂起的延迟检查操作都存储在队列中。ReplicaManager.appendRecords()的所有调用者都应该调用ActionQueue。
   * 在不持有任何冲突锁的情况下，对所有受影响的分区执行tryCompleteActions。
   */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    origin: AppendOrigin,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Lock] = None,
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => (),
                    requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds

      // todo 1
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed, origin,
        entriesPerPartition, requiredAcks, requestLocal)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition -> ProducePartitionStatus(
          result.info.lastOffset + 1, // required offset
          new PartitionResponse(
            result.error,
            result.info.firstOffset.map(_.messageOffset).getOrElse(-1),
            result.info.logAppendTime,
            result.info.logStartOffset,
            result.info.recordErrors.asJava,
            result.info.errorMessage
          )
        ) // 响应状态
      }

      actionQueue.add {
        () =>
          localProduceResults.foreach {
            case (topicPartition, result) =>
              val requestKey = TopicPartitionOperationKey(topicPartition)
              result.info.leaderHwChange match {
                case LeaderHwChange.Increased =>
                  // HW改变后，一些延迟的操作可能会被解除
                  delayedProducePurgatory.checkAndComplete(requestKey)
                  delayedFetchPurgatory.checkAndComplete(requestKey)
                  delayedDeleteRecordsPurgatory.checkAndComplete(requestKey)
                case LeaderHwChange.Same =>
                  // 由于日志结束偏移量已经更新，可能会取消阻塞一些追随者获取请求
                  delayedFetchPurgatory.checkAndComplete(requestKey)
                case LeaderHwChange.None =>
                  // nothing
              }
          }
      }

      recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })

      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // 创建延迟生产操作
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // 创建一个(主题、分区)对列表，用作这个延迟生成操作的键
        val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

        // 尝试立即完成请求，否则将其放入炼狱，这是因为在创建延迟的生成操作时，可能会有新的请求到达，从而使该操作可完成。
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // 我们可以立即做出反应
        val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
        responseCallback(produceResponseStatus)
      }
    } else {
      // 如果需要。ack在可接受范围之外，客户端有问题，只是返回一个错误，不处理请求
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(
          Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.map(_.messageOffset).getOrElse(-1),
          RecordBatch.NO_TIMESTAMP,
          LogAppendInfo.UnknownLogAppendInfo.logStartOffset
        )
      }
      responseCallback(responseStatus)
    }
  }

  /**
   * 删除分区的leader副本上的记录，并等待删除记录操作被传播到其他副本;当所有活动副本的logStartOffset达到指定的偏移量时，回调函数将被触发
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // 拒绝内部主题上的删除记录操作
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // 如果存在满足以下要求的主题分区，我们需要放置一个延迟的DeleteRecordsRequest，并等待删除记录操作完成
  // 1. 该分区的删除记录操作成功
  // 2. 该分区的低水位小于指定的偏移量
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * 对于映射中指定的每一对分区和日志目录，如果分区已经在此代理上创建，则将其日志文件移动到指定的日志目录。
   * 否则，将对记录在内存中，以便稍后代理接收到分区的LeaderAndIsrRequest时在指定的日志目录中创建分区。
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* 如果主题名称异常长，则不支持更改日志目录。
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (Log.logFutureDirName(topicPartition).size > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              // 如果destinationDir与现有的目标日志目录不同，则停止当前的复制移动
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }
            case HostedPartition.Offline =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartition.None => // Do nothing
          }

          // 如果这个分区的日志还没有创建:
          // 1) 在内存中记录目标日志目录，以便在代理稍后收到此分区的LeaderAndIsrRequest时在此日志目录中创建分区。
          // 2) 在AlterReplicaLogDirsResponse中响应此分区的NotLeaderOrFollowerException
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // 如果给定分区不存在副本，则抛出NotLeaderOrFollowerException
          val partition = getPartitionOrException(topicPartition)
          partition.localLogOrException

          // 如果destinationLDir与副本的当前日志目录不同:
          // - 如果不存在脱机日志目录，则在destinationDir(如果不存在)中创建未来日志，
          // 并启动ReplicaAlterDirThread将该分区的数据从当前日志移动到未来日志中
          // - 否则，返回KafkaStorageException。当存在脱机日志目录时，我们不创建未来日志，这样我们就可以避免在多个日志目录中为同一个分区创建未来日志。
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)
            val initialFetchState = InitialFetchState(BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.forException(e))
          case e: NotLeaderOrFollowerException =>
            // 为保持兼容性，为ALTER_REPLICA_LOG_DIRS保留REPLICA_NOT_AVAILABLE异常
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.REPLICA_NOT_AVAILABLE)
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * 获取指定分区列表的LogDirInfo。每个LogDirInfo为给定的日志目录指定以下信息:
   * 1) 日志目录错误，如日志是在线还是离线
   * 2) 给定日志目录中每个分区当前和未来日志的大小和延迟。只包含查询分区的日志。在实现KIP-113之后，代理上可能会有将来的日志(将来将取代分区的当前日志)。
   */
  def describeLogDirs(partitions: Set[TopicPartition]): List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.parentDir)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val topicInfos = logs.groupBy(_.topicPartition.topic).map{case (topic, logs) =>
              new DescribeLogDirsResponseData.DescribeLogDirsTopic().setName(topic).setPartitions(
                logs.filter { log =>
                  partitions.contains(log.topicPartition)
                }.map { log =>
                  new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                    .setPartitionSize(log.size)
                    .setPartitionIndex(log.topicPartition.partition)
                    .setOffsetLag(getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture))
                    .setIsFutureKey(log.isFuture)
                }.toList.asJava)
            }.toList.asJava

            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code).setTopics(topicInfos)
          case None =>
            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code)
        }

      } catch {
        case e: KafkaStorageException =>
          warn("Unable to describe replica dirs for %s".format(absolutePath), e)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.forException(t).code)
      }
    }.toList
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    localLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // 如果副本未创建或处于脱机状态，则返回-1L表示LEO延迟不可用
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset,
          new DeleteRecordsPartitionResult()
            .setLowWatermark(result.lowWatermark)
            .setErrorCode(result.error.code)
            .setPartitionIndex(topicPartition.partition))
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // 创建延迟删除记录操作
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // 创建一个(主题、分区)对列表，作为延迟删除记录操作的键
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

      // 尝试立即完成请求，否则将其放入炼狱，这是因为当创建延迟的删除记录操作时，可能会有新的请求到达，从而使该操作可完成。
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // 我们可以立即做出反应
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // 如果下列所有条件都为真，则需要放置延迟的生产请求并等待复制完成
  //
  // 1. required acks = -1
  // 2. 有数据要追加
  // 3. 至少有一个分区追加成功(错误少于分区)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * 将消息追加到本地副本日志
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short,
                               requestLocal: RequestLocal): Map[TopicPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled
    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = onlinePartition(topicPartition).map(_.logStartOffset).getOrElse(-1L)
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      error(s"Error processing append operation on partition $topicPartition", t)

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // 如果不允许，拒绝添加到内部主题
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {

          // 获取写入的topicPartition
          val partition = getPartitionOrException(topicPartition)

          // TODO 写入日志的地方，tmd终于找到了
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks, requestLocal)
          val numAppendedMessages = info.numMessages

          // TODO 更新成功追加的字节和消息的统计数据为bytesInRate和messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
              s"${info.firstOffset.getOrElse(-1)} and ending at offset ${info.lastOffset}")

          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: 对于已知的异常，不增加失败的产品请求度量 它应该指示broker在处理生成请求时意外失败
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(
              logStartOffset, recordErrors, rve.invalidException.getMessage), Some(rve.invalidException)))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
        }
      }
    }
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = {
    val partition = getPartitionOrException(topicPartition)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader)
  }

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = {
    val partition = getPartitionOrException(topicPartition)
    partition.legacyFetchOffsetsForTimestamp(timestamp, maxNumOffsets, isFromConsumer, fetchOnlyFromLeader)
  }

  /**
   * 从副本中获取消息，并等待足够的数据获取和返回;当超时或所需的获取信息被满足时，回调函数将被触发。消费者可以从任何副本中获取，
   * 但追随者只能从leader中获取。
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchMaxBytes: Int,
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,
                    isolationLevel: IsolationLevel,
                    clientMetadata: Option[ClientMetadata]): Unit = {
    val isFromFollower = Request.isValidBrokerId(replicaId)
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    val fetchIsolation = if (!isFromConsumer)
      FetchLogEnd
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted
    else
      FetchHighWatermark

    // 如果请求来自follower或旧版本的客户端(没有ClientMetadata)，则限制对leader的抓取
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)
    def readFromLog(): Seq[(TopicPartition, LogReadResult)] = {
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        fetchIsolation = fetchIsolation,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        clientMetadata = clientMetadata)
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      else result
    }

    val logReadResults = readFromLog()

    // 检查这个获取请求是否可以立即被满足
    var bytesReadable: Long = 0
    var errorReadingData = false
    var hasDivergingEpoch = false
    val logReadResultMap = new mutable.HashMap[TopicPartition, LogReadResult]
    logReadResults.foreach { case (topicPartition, logReadResult) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      if (logReadResult.divergingEpoch.nonEmpty)
        hasDivergingEpoch = true
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      logReadResultMap.put(topicPartition, logReadResult)
    }

    // 如果                    1) 获取请求不想等待，立即响应
    //                        2) 获取请求不需要任何数据
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //                        5) we found a diverging epoch
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData || hasDivergingEpoch) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = isFromFollower && isAddingReplica(tp, replicaId)
        tp -> result.toFetchPartitionData(isReassignmentFetch)
      }
      responseCallback(fetchPartitionData)
    } else {
      // 从读结果构造取结果
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicPartition, FetchPartitionStatus)]
      fetchInfos.foreach { case (topicPartition, partitionData) =>
        logReadResultMap.get(topicPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }
      val fetchMetadata: SFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit,
        fetchOnlyFromLeader, fetchIsolation, isFromFollower, replicaId, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, clientMetadata,
        responseCallback)

      // 创建一个(主题、分区)对的列表，作为这个延迟获取操作的键
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }

      // 尽量立即完成请求，否则将其投入炼狱;这是因为在创建延迟的获取操作时，可能会有新的请求到达，从而使该操作可以完成。
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * 以给定的偏移量从多个主题分区读取，最多maxSize字节
   */
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       fetchIsolation: FetchIsolation,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       clientMetadata: Option[ClientMetadata]): Seq[(TopicPartition, LogReadResult)] = {
    val traceEnabled = isTraceEnabled

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      try {
        if (traceEnabled)
          trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
            s"remaining response limit $limitBytes" +
            (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        val partition = getPartitionOrException(tp)
        val fetchTimeMs = time.milliseconds

        // 如果我们是leader，确定首选的read-replica
        val preferredReadReplica = clientMetadata.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorOpt.foreach { selector =>
            debug(s"Replica selector ${selector.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for $clientMetadata")
          }
          // 如果设置了优先读-副本，请跳过该读操作
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = offsetSnapshot.highWatermark.messageOffset,
            leaderLogStartOffset = offsetSnapshot.logStartOffset,
            leaderLogEndOffset = offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = -1L,
            lastStableOffset = Some(offsetSnapshot.lastStableOffset.messageOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        } else {
          // 先尝试read，这告诉我们是否需要这个分区的所有adjustdfetchsize
          val readInfo: LogReadInfo = partition.readRecords(
            lastFetchedEpoch = fetchInfo.lastFetchedEpoch,
            fetchOffset = fetchInfo.fetchOffset,
            currentLeaderEpoch = fetchInfo.currentLeaderEpoch,
            maxBytes = adjustedMaxBytes,
            fetchIsolation = fetchIsolation,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            minOneMessage = minOneMessage)

          val fetchDataInfo = if (shouldLeaderThrottle(quota, partition, replicaId)) {
            // 如果正在对分区进行节流，只需返回一个空集。
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else if (!hardMaxBytesLimit && readInfo.fetchedData.firstEntryIncomplete) {
            // 对于FetchRequest版本3，我们将不完整的消息集替换为空的消息集，因为消费者可以在这种情况下取得进展，并且不需要报告' RecordTooLargeException '。
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else {
            readInfo.fetchedData
          }

          LogReadResult(info = fetchDataInfo,
            divergingEpoch = readInfo.divergingEpoch,
            highWatermark = readInfo.highWatermark,
            leaderLogStartOffset = readInfo.logStartOffset,
            leaderLogEndOffset = readInfo.logEndOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = fetchTimeMs,
            lastStableOffset = Some(readInfo.lastStableOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        }
      } catch {
        // NOTE: 对于已知异常，失败的获取请求指标不会增加，因为它应该指示代理在处理获取请求时出现意外失败
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderOrFollowerException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = Request.describeReplicaId(replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // 一旦我们从非空分区读取数据，我们就不再忽略请求和分区级别的大小限制
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
    * 使用已配置的 [[ReplicaSelector]], 为给定的分区确定首选读副本客户端元数据、请求的偏移量和当前副本集。如果首选读副本是leader，则返回None
    */
  def findPreferredReadReplica(partition: Partition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    partition.leaderReplicaIdOpt.flatMap { leaderReplicaId =>
      // 不要查找通过正常复制获取追随者的首选项
      if (Request.isValidBrokerId(replicaId))
        None
      else {
        replicaSelectorOpt.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition,
            new ListenerName(clientMetadata.listenerName))
          val replicaInfos = partition.remoteReplicas
            // 排除没有请求偏移量的副本(无论它们是否在ISR中)
            .filter(replica => replica.logEndOffset >= fetchOffset && replica.logStartOffset <= fetchOffset)
            .map(replica => new DefaultReplicaView(
              replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
              replica.logEndOffset,
              currentTimeMs - replica.lastCaughtUpTimeMs))

          val leaderReplica = new DefaultReplicaView(
            replicaEndpoints.getOrElse(leaderReplicaId, Node.noNode()),
            partition.localLogOrException.logEndOffset, 0L)
          val replicaInfoSet = mutable.Set[ReplicaView]() ++= replicaInfos += leaderReplica

          val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
          replicaSelector.select(partition.topicPartition, clientMetadata, partitionInfo).asScala.collect {
            // 尽管副本选择器可以返回leader，但我们不想将它与FetchResponse一起发送出去，所以我们在这里排除了它
            case selected if !selected.endpoint.isEmpty && selected != leaderReplica => selected.endpoint.id
          }
        }
      }
    }
  }

  /**
   *  为了避免ISR震荡，我们只在leader上节流一个副本，如果它在节流的副本列表中，超过配额并且副本不同步。
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean = {
    val isReplicaInSync = partition.inSyncReplicaIds.contains(replicaId)
    !isReplicaInSync && quota.isThrottled(partition.topicPartition) && quota.isQuotaExceeded
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localLog(topicPartition).map(_.config)

  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.recordVersion.value)

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if (updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = s"Received update metadata request with correlation id $correlationId " +
          s"from an old controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}. " +
          s"Latest known controller epoch is $controllerEpoch"
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateChangeLogger.messageWithPrefix(stateControllerEpochErrorMessage))
      } else {
        val zkMetadataCache = metadataCache.asInstanceOf[ZkMetadataCache]
        val deletedPartitions = zkMetadataCache.updateMetadata(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    val startMs = time.milliseconds()
    replicaStateChangeLock synchronized {
      val controllerId = leaderAndIsrRequest.controllerId
      val requestPartitionStates = leaderAndIsrRequest.partitionStates.asScala
      stateChangeLogger.info(s"Handling LeaderAndIsr request correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        requestPartitionStates.foreach { partitionState =>
          stateChangeLogger.trace(s"Received LeaderAndIsr request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch ${leaderAndIsrRequest.controllerEpoch}")
        }
      val topicIds = leaderAndIsrRequest.topicIds()
      def topicIdFromRequest(topicName: String): Option[Uuid] = {
        val topicId = topicIds.get(topicName)
        // 如果无效的主题ID返回None
        if (topicId == null || topicId == Uuid.ZERO_UUID)
          None
        else
          Some(topicId)
      }

      val response = {
        if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
          stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
            s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
            s"Latest known controller epoch is $controllerEpoch")
          leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
        } else {
          val responseMap = new mutable.HashMap[TopicPartition, Errors]
          controllerEpoch = leaderAndIsrRequest.controllerEpoch

          val partitionStates = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()

          // 如果分区还不存在，首先创建分区
          requestPartitionStates.foreach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            val partitionOpt = getPartition(topicPartition) match {
              case HostedPartition.Offline =>
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                  "partition is in an offline log directory")
                responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
                None

              case HostedPartition.Online(partition) =>
                Some(partition)

              case HostedPartition.None =>
                val partition = Partition(topicPartition, time, this)
                allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
                Some(partition)
            }

            // 接下来检查主题ID和分区的leader epoch
            partitionOpt.foreach { partition =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              val requestTopicId = topicIdFromRequest(topicPartition.topic)
              val logTopicId = partition.topicId

              if (!hasConsistentTopicId(requestTopicId, logTopicId)) {
                stateChangeLogger.error(s"Topic ID in memory: ${logTopicId.get} does not" +
                  s" match the topic ID for partition $topicPartition received: " +
                  s"${requestTopicId.get}.")
                responseMap.put(topicPartition, Errors.INCONSISTENT_TOPIC_ID)
              } else if (requestLeaderEpoch > currentLeaderEpoch) {
                // 如果leader epoch有效，则记录做出leader决策的控制器的epoch。这在更新isr以在zookeeper路径中维护决策者控制器的epoch时非常有用
                if (partitionState.replicas.contains(localBrokerId))
                  partitionStates.put(partition, partitionState)
                else {
                  stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                    s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                    s"in assigned replica list ${partitionState.replicas.asScala.mkString(",")}")
                  responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
                }
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
              } else {
                val error = requestTopicId match {
                  case Some(topicId) if logTopicId.isEmpty =>
                    // 控制器可以发送LeaderAndIsr来升级到使用主题id而不影响epoch。如果我们有一个匹配的epoch，我们期望日志被定义。
                    val log = localLogOrException(partition.topicPartition)
                    log.assignTopicId(topicId)
                    stateChangeLogger.info(s"Updating log for $topicPartition to assign topic ID " +
                      s"$topicId from LeaderAndIsr request from controller $controllerId with correlation " +
                      s"id $correlationId epoch $controllerEpoch")
                    Errors.NONE
                  case _ =>
                    stateChangeLogger.info(s"Ignoring LeaderAndIsr request from " +
                      s"controller $controllerId with correlation id $correlationId " +
                      s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                      s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                    Errors.STALE_CONTROLLER_EPOCH
                }
                responseMap.put(topicPartition, error)
              }
            }
          }

          val partitionsToBeLeader = partitionStates.filter { case (_, partitionState) =>
            partitionState.leader == localBrokerId
          }
          val partitionsToBeFollower = partitionStates.filter { case (k, _) => !partitionsToBeLeader.contains(k) }

          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
            makeLeaders(controllerId, controllerEpoch, partitionsToBeLeader, correlationId, responseMap,
              highWatermarkCheckpoints, topicIdFromRequest)
          else
            Set.empty[Partition]
          val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
            makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
              highWatermarkCheckpoints, topicIdFromRequest)
          else
            Set.empty[Partition]

          val followerTopicSet = partitionsBecomeFollower.map(_.topic).toSet
          updateLeaderAndFollowerMetrics(followerTopicSet)

          leaderAndIsrRequest.partitionStates.forEach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            /*
           * 如果存在离线日志目录，则可能在getOrCreateReplica()由于KafkaStorageException导致本地副本创建失败之前，
           * 已经通过getOrCreatePartition()创建了Partition对象。在本例中为ReplicaManager。allPartitions将这个
           * 主题分区映射到一个空的Partition对象。我们需要将这个主题分区映射到OfflinePartition。
           */
            if (localLog(topicPartition).isEmpty)
              markPartitionOffline(topicPartition)
          }

          // 我们在第一个highwatermark之后初始化highwatermark线程。通过避免奇怪的竞争条件，这确保所有分区在开始检查点之前都已完全填充
          startHighWatermarkCheckPointThread()

          maybeAddLogDirFetchers(partitionStates.keySet, highWatermarkCheckpoints, topicIdFromRequest)

          replicaFetcherManager.shutdownIdleFetcherThreads()
          replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
          onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)

          val data = new LeaderAndIsrResponseData().setErrorCode(Errors.NONE.code)
          if (leaderAndIsrRequest.version < 5) {
            responseMap.forKeyValue { (tp, error) =>
              data.partitionErrors.add(new LeaderAndIsrPartitionError()
                .setTopicName(tp.topic)
                .setPartitionIndex(tp.partition)
                .setErrorCode(error.code))
            }
          } else {
            responseMap.forKeyValue { (tp, error) =>
              val topicId = topicIds.get(tp.topic)
              var topic = data.topics.find(topicId)
              if (topic == null) {
                topic = new LeaderAndIsrTopicError().setTopicId(topicId)
                data.topics.add(topic)
              }
              topic.partitionErrors.add(new LeaderAndIsrPartitionError()
                .setPartitionIndex(tp.partition)
                .setErrorCode(error.code))
            }
          }
          new LeaderAndIsrResponse(data, leaderAndIsrRequest.version)
        }
      }
      val endMs = time.milliseconds()
      val elapsedMs = endMs - startMs
      stateChangeLogger.info(s"Finished LeaderAndIsr request in ${elapsedMs}ms correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      response
    }
  }

  /**
   * 检查leaderanddisr请求中提供的主题ID是否与日志中的主题ID一致。
   *
   * 如果请求具有无效的主题ID (null或零)，那么我们假定不支持主题ID。主题ID不一致，因此返回true。如果日志不存在或尚未设置主题ID，
   * 则logTopicIdOpt为None。在这两种情况下，ID都不一致，因此返回true。
   *
   * @param requestTopicIdOpt the topic ID from the LeaderAndIsr request if it exists
   * @param logTopicIdOpt the topic ID in the log if the log and the topic ID exist
   * @return true if the request topic id is consistent, false otherwise
   */
  private def hasConsistentTopicId(requestTopicIdOpt: Option[Uuid], logTopicIdOpt: Option[Uuid]): Boolean = {
    requestTopicIdOpt match {
      case None => true
      case Some(requestTopicId) => logTopicIdOpt.isEmpty || logTopicIdOpt.contains(requestTopicId)
    }
  }

  /**
   * KAFKA-8392
   * 对于代理不再是领导者的主题分区，删除与这些主题相关的指标。注意，这意味着代理不再是上述主题分区的副本或领导者
   */
  protected def updateLeaderAndFollowerMetrics(newFollowerTopics: Set[String]): Unit = {
    val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
    newFollowerTopics.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)

    // 删除不是主题关注者的代理的指标
    leaderTopicSet.diff(newFollowerTopics).foreach(brokerTopicStats.removeOldFollowerMetrics)
  }

  protected def maybeAddLogDirFetchers(partitions: Set[Partition],
                                       offsetCheckpoints: OffsetCheckpoints,
                                       topicIds: String => Option[Uuid]): Unit = {
    val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
    for (partition <- partitions) {
      val topicPartition = partition.topicPartition
      if (logManager.getLog(topicPartition, isFuture = true).isDefined) {
        partition.log.foreach { log =>
          val leader = BrokerEndPoint(config.brokerId, "localhost", -1)

          // 将将来的副本日志添加到分区映射中
          partition.createLogIfNotExists(
            isNew = false,
            isFutureReplica = true,
            offsetCheckpoints,
            topicIds(partition.topic))

          // 暂停清理正在移动的分区，并启动ReplicaAlterDirThread将副本从源目录移动到目标目录
          logManager.abortAndPauseCleaning(topicPartition)

          futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(leader,
            partition.getLeaderEpoch, log.highWatermark))
        }
      }
    }

    if (futureReplicasAndInitialOffset.nonEmpty)
      replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)
  }

  /*
   * 通过以下方式使当前broker成为给定分区集的leader:
   *
   * 1. 停止这些分区的获取程序
   * 2. 更新缓存中的分区元数据
   * 3. 将这些分区添加到leader分区集
   *
   * 如果在这个函数中抛出了一个意外的错误，它将被传播到kafkAapi，在kafkaApi中，错误消息将被设置在每个分区上，
   * 因为我们不知道是哪个分区引起的。否则，返回由于此方法而成为leader的分区集
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          controllerEpoch: Int,
                          partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors],
                          highWatermarkCheckpoints: OffsetCheckpoints,
                          topicIds: String => Option[Uuid]): Set[Partition] = {
    val traceEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.keys.foreach { partition =>
      if (traceEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
          s"controller $controllerId epoch $controllerEpoch starting the become-leader transition for " +
          s"partition ${partition.topicPartition}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeLeaders = mutable.Set[Partition]()

    try {
      // 首先停止所有分区的获取器
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $controllerEpoch as part of the become-leader transition for " +
        s"${partitionStates.size} partitions")
      // 更新分区信息以成为leader
      partitionStates.forKeyValue { (partition, partitionState) =>
        try {
          // todo 2 创建路径
          if (partition.makeLeader(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName)))
            partitionsToMakeLeaders += partition
          else
            stateChangeLogger.info(s"Skipped the become-leader state change after marking its " +
              s"partition as leader with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} (last update controller epoch ${partitionState.controllerEpoch}) " +
              s"since it is already the leader for the partition.")
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionStates.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition}", e)
        }
        // 重新抛出异常，使其在kafkaApi中被捕获
        throw e
    }

    if (traceEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-leader transition for partition ${partition.topicPartition}")
      }

    partitionsToMakeLeaders
  }

  /*
   * 通过以下方式使当前代理成为给定分区集的follower:
   *
   * 1. 从leader分区集中删除这些分区。
   * 2. 将副本标记为追随者，这样就不能从生产者客户端添加更多数据。
   * 3. 停止这些分区的读取器，这样副本读取器线程就不能再添加数据了。
   * 4. 截断这些分区的日志和检查点偏移量。
   * 5. 在purgatory中完成生产和取回任务
   * 6. 如果broker没有关闭，将fetcher添加到新的leader中。
   *
   * 执行这些步骤的顺序确保在检查点偏移之前，转换中的副本不会再接收任何消息，从而保证检查点之前的所有消息都被刷新到磁盘
   *
   * 如果在这个函数中抛出了一个意外的错误，它将被传播到kafkaApi，在kafkaApi中，错误消息将被设置在每个分区上，因为我们不知道是哪个分区引起的。
   * 否则，返回由于该方法而成为follower的分区集
   */
  private def makeFollowers(controllerId: Int,
                            controllerEpoch: Int,
                            partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors],
                            highWatermarkCheckpoints: OffsetCheckpoints,
                            topicIds: String => Option[Uuid]) : Set[Partition] = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.forKeyValue { (partition, partitionState) =>
      if (traceLoggingEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionState.leader}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
    try {
      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionStates.forKeyValue { (partition, partitionState) =>
        val newLeaderBrokerId = partitionState.leader
        try {
          if (metadataCache.hasAliveBroker(newLeaderBrokerId)) {
            // 仅在leader可用时更改分区状态
            if (partition.makeFollower(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName)))
              partitionsToMakeFollower += partition
            else
              stateChangeLogger.info(s"Skipped the become-follower state change after marking its partition as " +
                s"follower with correlation id $correlationId from controller $controllerId epoch $controllerEpoch " +
                s"for partition ${partition.topicPartition} (last update " +
                s"controller epoch ${partitionState.controllerEpoch}) " +
                s"since the new leader $newLeaderBrokerId is the same as the old leader")
          } else {
            // 领导代理应该始终存在于元数据缓存中。如果没有，我们应该记录错误消息并中止此分区的转换过程
            stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) " +
              s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
            // 即使leader不可用，也要创建本地副本。这是确保我们在检查点文件中包含分区的高水位所必需的(参见KAFKA-1647)
            partition.createLogIfNotExists(isNew = partitionState.isNew, isFutureReplica = false,
              highWatermarkCheckpoints, topicIds(partitionState.topicName))
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower for partition $partition with leader " +
              s"$newLeaderBrokerId in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      // 为了正确初始化获取位置，必须先停止获取器。
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
        s"epoch $controllerEpoch with correlation id $correlationId for ${partitionsToMakeFollower.size} partitions")

      partitionsToMakeFollower.foreach { partition =>
        completeDelayedFetchOrProduceRequests(partition.topicPartition)
      }

      if (isShuttingDown.get()) {
        if (traceLoggingEnabled) {
          partitionsToMakeFollower.foreach { partition =>
            stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
              s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} with leader ${partitionStates(partition).leader} " +
              "since it is shutting down")
          }
        }
      } else {
        // 我们不需要再次检查leader是否存在，因为这已经在流程开始时完成了
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
          val leaderNode = partition.leaderReplicaIdOpt.flatMap(leaderId => metadataCache.
            getAliveBrokerNode(leaderId, config.interBrokerListenerName)).getOrElse(Node.noNode())
          val leader = new BrokerEndPoint(leaderNode.id(), leaderNode.host(), leaderNode.port())
          val log = partition.localLogOrException
          val fetchOffset = initialFetchOffset(log)
          partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
        }.toMap

        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch", e)
        // 重新抛出异常，使其在kafkaApi中被捕获
        throw e
    }

    if (traceLoggingEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).leader}")
      }

    partitionsToMakeFollower
  }

  /**
   * 从IBP 2.7开始，我们在请求中发送最新的获取epoch，如果在响应中返回一个偏离的epoch，则截断，避免了对单独的OffsetForLeaderEpoch请求的需要。
   */
  protected def initialFetchOffset(log: Log): Long = {
    if (ApiVersion.isTruncationOnFetchSupported(config.interBrokerProtocolVersion) && log.latestEpoch.nonEmpty)
      log.logEndOffset
    else
      log.highWatermark
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // 收缩非离线分区的isr
    allPartitions.keys.foreach { topicPartition =>
      onlinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  /**
   * 根据最后一次获取请求更新follower在leader上的获取状态，并更新' readResult '。如果跟随者副本没有被识别为分配的副本之一，
   * 则不要更新' readResult '，以便日志起始偏移量和高水位与fetch响应中的记录一致。日志起始偏移量和高水位不仅会因为这个获取请求而改变，
   * 例如，滚动新的日志段和删除旧的日志段可能会使日志起始偏移量比获取记录中的最后一个偏移量更远。追随者将在下一个获取响应中获得更新的领导者状态。
   * 如果follower具有发散的epoch，或者如果读取失败并出现任何错误，则不更新follower的读取状态。
   */
  private def updateFollowerFetchState(followerId: Int,
                                       readResults: Seq[(TopicPartition, LogReadResult)]): Seq[(TopicPartition, LogReadResult)] = {
    readResults.map { case (topicPartition, readResult) =>
      val updatedReadResult = if (readResult.error != Errors.NONE) {
        debug(s"Skipping update of fetch state for follower $followerId since the " +
          s"log read returned error ${readResult.error}")
        readResult
      } else if (readResult.divergingEpoch.nonEmpty) {
        debug(s"Skipping update of fetch state for follower $followerId since the " +
          s"log read returned diverging epoch ${readResult.divergingEpoch}")
        readResult
      } else {
        onlinePartition(topicPartition) match {
          case Some(partition) =>
            if (partition.updateFollowerFetchState(followerId,
              followerFetchOffsetMetadata = readResult.info.fetchOffsetMetadata,
              followerStartOffset = readResult.followerLogStartOffset,
              followerFetchTimeMs = readResult.fetchTimeMs,
              leaderEndOffset = readResult.leaderLogEndOffset)) {
              readResult
            } else {
              warn(s"Leader $localBrokerId failed to record follower $followerId's position " +
                s"${readResult.info.fetchOffsetMetadata.messageOffset}, and last sent HW since the replica " +
                s"is not recognized to be one of the assigned replicas ${partition.assignmentState.replicas.mkString(",")} " +
                s"for partition $topicPartition. Empty records will be returned for this partition.")
              readResult.withEmptyFetchInfo
            }
          case None =>
            warn(s"While recording the replica LEO, the partition $topicPartition hasn't been created.")
            readResult
        }
      }
      topicPartition -> updatedReadResult
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    onlinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    onlinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // 将所有分区的highwatermark值刷新到highwatermark文件中
  def checkpointHighWatermarks(): Unit = {
    def putHw(logDirToCheckpoints: mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]],
              log: Log): Unit = {
      val checkpoints = logDirToCheckpoints.getOrElseUpdate(log.parentDir,
        new mutable.AnyRefMap[TopicPartition, Long]())
      checkpoints.put(log.topicPartition, log.highWatermark)
    }

    val logDirToHws = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]](
      allPartitions.size)
    onlinePartitionsIterator.foreach { partition =>
      partition.log.foreach(putHw(logDirToHws, _))
      partition.futureLog.foreach(putHw(logDirToHws, _))
    }

    for ((logDir, hws) <- logDirToHws) {
      try highWatermarkCheckpoints.get(logDir).foreach(_.write(hws))
      catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $logDir", e)
      }
    }
  }

  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(tp, HostedPartition.Offline)
    Partition.removeMetrics(tp)
  }

  /**
   * 副本的日志目录失败处理程序
   *
   * @param dir                     日志目录的绝对路径
   * @param sendZkNotification      检查我们是否需要向zookeeper节点发送通知(需要进行单元测试)
   */
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    warn(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = onlinePartitionsIterator.filter { partition =>
        partition.log.exists { _.parentDir == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = onlinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.parentDir == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)

    if (sendZkNotification)
      if (zkClient.isEmpty) {
        warn("Unable to propagate log dir failure via Zookeeper in KRaft mode")
      } else {
        zkClient.get.propagateLogDirEvent(localBrokerId)
      }
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
    removeMetric("AtMinIsrPartitionCount")
    removeMetric("ReassigningPartitions")
  }

  // 只有在进行单元测试时才需要检查高水位
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedElectLeaderPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorOpt.foreach(_.close)
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  protected def createReplicaSelector(): Option[ReplicaSelector] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = CoreUtils.createObject[ReplicaSelector](className)
      tmpReplicaSelector.configure(config.originals())
      tmpReplicaSelector
    }
  }

  def lastOffsetForLeaderEpoch(
    requestedEpochInfo: Seq[OffsetForLeaderTopic]
  ): Seq[OffsetForLeaderTopicResult] = {
    requestedEpochInfo.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        val tp = new TopicPartition(offsetForLeaderTopic.topic, offsetForLeaderPartition.partition)
        getPartition(tp) match {
          case HostedPartition.Online(partition) =>
            val currentLeaderEpochOpt =
              if (offsetForLeaderPartition.currentLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
                Optional.empty[Integer]
              else
                Optional.of[Integer](offsetForLeaderPartition.currentLeaderEpoch)

            partition.lastOffsetForLeaderEpoch(
              currentLeaderEpochOpt,
              offsetForLeaderPartition.leaderEpoch,
              fetchOnlyFromLeader = true)

          case HostedPartition.Offline =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)

          case HostedPartition.None if metadataCache.contains(tp) =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)

          case HostedPartition.None =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        }
      }

      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }
  }

  def electLeaders(
    controller: KafkaController,
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    responseCallback: Map[TopicPartition, ApiError] => Unit,
    requestTimeout: Int
  ): Unit = {

    val deadline = time.milliseconds() + requestTimeout

    def electionCallback(results: Map[TopicPartition, Either[ApiError, Int]]): Unit = {
      val expectedLeaders = mutable.Map.empty[TopicPartition, Int]
      val failures = mutable.Map.empty[TopicPartition, ApiError]
      results.foreach {
        case (partition, Right(leader)) => expectedLeaders += partition -> leader
        case (partition, Left(error)) => failures += partition -> error
      }
      if (expectedLeaders.nonEmpty) {
        val watchKeys = expectedLeaders.iterator.map {
          case (tp, _) => TopicPartitionOperationKey(tp)
        }.toBuffer

        delayedElectLeaderPurgatory.tryCompleteElseWatch(
          new DelayedElectLeader(
            math.max(0, deadline - time.milliseconds()),
            expectedLeaders,
            failures,
            this,
            responseCallback
          ),
          watchKeys
        )
      } else {
          // 实际上没有选择分区，因此请立即返回
          responseCallback(failures)
      }
    }

    controller.electLeaders(partitions, electionType, electionCallback)
  }

  def activeProducerState(requestPartition: TopicPartition): DescribeProducersResponseData.PartitionResponse = {
    getPartitionOrError(requestPartition) match {
      case Left(error) => new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(requestPartition.partition)
        .setErrorCode(error.code)
      case Right(partition) => partition.activeProducerState
    }
  }

  private[kafka] def getOrCreatePartition(tp: TopicPartition,
                                          delta: TopicsDelta,
                                          topicId: Uuid): Option[(Partition, Boolean)] = {
    getPartition(tp) match {
      case HostedPartition.Offline =>
        stateChangeLogger.warn(s"Unable to bring up new local leader ${tp} " +
          s"with topic id ${topicId} because it resides in an offline log " +
          "directory.")
        None

      case HostedPartition.Online(partition) => {
        if (partition.topicId.exists(_ != topicId)) {
          // Note: Partition#topicId will be None here if the Log object for this partition
          // has not been created.
          throw new IllegalStateException(s"Topic ${tp} exists, but its ID is " +
            s"${partition.topicId.get}, not ${topicId} as expected")
        }
        Some(partition, false)
      }

      case HostedPartition.None =>
        if (delta.image().topicsById().containsKey(topicId)) {
          stateChangeLogger.error(s"Expected partition ${tp} with topic id " +
            s"${topicId} to exist, but it was missing. Creating...")
        } else {
          stateChangeLogger.info(s"Creating new partition ${tp} with topic id " +
            s"${topicId}.")
        }
        // 这是一个我们还不知道的分区，所以创建它并在线标记它
        val partition = Partition(tp, time, this)
        allPartitions.put(tp, HostedPartition.Online(partition))
        Some(partition, true)
    }
  }

  /**
   * 应用KRaft主题更改delta。
   *
   * @param newImage        The new metadata image.
   * @param delta           The delta to apply.
   */
  def applyDelta(newImage: MetadataImage, delta: TopicsDelta): Unit = {
    // 在获取锁之前，计算本地更改
    val localChanges = delta.localChanges(config.nodeId)

    replicaStateChangeLock.synchronized {
      // 在获取锁之前，计算本地更改创建与我们在这里删除的分区名称相同的新分区。
      if (!localChanges.deletes.isEmpty) {
        val deletes = localChanges.deletes.asScala.map(tp => (tp, true)).toMap
        stateChangeLogger.info(s"Deleting ${deletes.size} partition(s).")
        stopPartitions(deletes).foreach { case (topicPartition, e) =>
          if (e.isInstanceOf[KafkaStorageException]) {
            stateChangeLogger.error(s"Unable to delete replica ${topicPartition} because " +
              "the local replica for the partition is in an offline log directory")
          } else {
            stateChangeLogger.error(s"Unable to delete replica ${topicPartition} because " +
              s"we got an unexpected ${e.getClass.getName} exception: ${e.getMessage}")
          }
        }
      }

      // 处理分区，我们现在是领导者或追随者。
      if (!localChanges.leaders.isEmpty || !localChanges.followers.isEmpty) {
        val lazyOffsetCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        val changedPartitions = new mutable.HashSet[Partition]
        if (!localChanges.leaders.isEmpty) {
          applyLocalLeadersDelta(changedPartitions, delta, lazyOffsetCheckpoints, localChanges.leaders.asScala)
        }
        if (!localChanges.followers.isEmpty) {
          applyLocalFollowersDelta(changedPartitions, newImage, delta, lazyOffsetCheckpoints, localChanges.followers.asScala)
        }
        maybeAddLogDirFetchers(changedPartitions, lazyOffsetCheckpoints,
          name => Option(newImage.topics().getTopic(name)).map(_.id()))

        def markPartitionOfflineIfNeeded(tp: TopicPartition): Unit = {
          /*
           * 如果存在离线日志目录，则可能在getOrCreateReplica()由于KafkaStorageException导致本地副本创建失败之前，
           * 已经通过getOrCreatePartition()创建了Partition对象。在本例中为ReplicaManager。allPartitions
           * 将这个主题分区映射到一个空的Partition对象。我们需要将这个主题分区映射到OfflinePartition。
           */
          if (localLog(tp).isEmpty)
            markPartitionOffline(tp)
        }
        localChanges.leaders.keySet.forEach(markPartitionOfflineIfNeeded)
        localChanges.followers.keySet.forEach(markPartitionOfflineIfNeeded)

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
      }
    }
  }

  private def applyLocalLeadersDelta(
    changedPartitions: mutable.Set[Partition],
    delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    newLocalLeaders: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${newLocalLeaders.size} partition(s) to " +
      "local leaders.")
    replicaFetcherManager.removeFetcherForPartitions(newLocalLeaders.keySet)
    newLocalLeaders.forKeyValue { case (tp, info) =>
      getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
        try {
          val state = info.partition.toLeaderAndIsrPartitionState(tp, isNew)
          if (!partition.makeLeader(state, offsetCheckpoints, Some(info.topicId))) {
            stateChangeLogger.info("Skipped the become-leader state change for " +
              s"${tp} with topic id ${info.topicId} because this partition is " +
              "already a local leader.")
          }
          changedPartitions.add(partition)
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.info(s"Skipped the become-leader state change for ${tp} " +
              s"with topic id ${info.topicId} due to disk error ${e}")
            val dirOpt = getLogDir(tp)
            error(s"Error while making broker the leader for partition ${tp} in dir " +
              s"${dirOpt}", e)
        }
      }
    }
  }

  private def applyLocalFollowersDelta(
    changedPartitions: mutable.Set[Partition],
    newImage: MetadataImage,
    delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    newLocalFollowers: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${newLocalFollowers.size} partition(s) to " +
      "local followers.")
    val shuttingDown = isShuttingDown.get()
    val partitionsToMakeFollower = new mutable.HashMap[TopicPartition, Partition]
    val newFollowerTopicSet = new mutable.HashSet[String]
    newLocalFollowers.forKeyValue { case (tp, info) =>
      getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
        try {
          newFollowerTopicSet.add(tp.topic)

          if (shuttingDown) {
            stateChangeLogger.trace(s"Unable to start fetching ${tp} with topic " +
              s"ID ${info.topicId} because the replica manager is shutting down.")
          } else {
            val leader = info.partition.leader
            if (newImage.cluster.broker(leader) == null) {
              stateChangeLogger.trace(s"Unable to start fetching $tp with topic ID ${info.topicId} " +
                s"from leader $leader because it is not alive.")

              // 即使leader不可用，也要创建本地副本。这是确保我们在检查点文件中包含分区的高水位所必需的(参见KAFKA-1647)。
              partition.createLogIfNotExists(isNew, false, offsetCheckpoints, Some(info.topicId))
            } else {
              val state = info.partition.toLeaderAndIsrPartitionState(tp, isNew)
              if (partition.makeFollower(state, offsetCheckpoints, Some(info.topicId))) {
                partitionsToMakeFollower.put(tp, partition)
              } else {
                stateChangeLogger.info("Skipped the become-follower state change after marking its " +
                  s"partition as follower for partition $tp with id ${info.topicId} and partition state $state.")
              }
            }
          }
          changedPartitions.add(partition)
        } catch {
          case e: Throwable => stateChangeLogger.error(s"Unable to start fetching ${tp} " +
              s"with topic ID ${info.topicId} due to ${e.getClass.getSimpleName}", e)
            replicaFetcherManager.addFailedPartition(tp)
        }
      }
    }

    // 为了正确初始化获取位置，必须先停止获取器。
    replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.keySet)
    stateChangeLogger.info(s"Stopped fetchers as part of become-follower for ${partitionsToMakeFollower.size} partitions")

    val listenerName = config.interBrokerListenerName.value
    val partitionAndOffsets = new mutable.HashMap[TopicPartition, InitialFetchState]
    partitionsToMakeFollower.forKeyValue { (topicPartition, partition) =>
      val node = partition.leaderReplicaIdOpt
        .flatMap(leaderId => Option(newImage.cluster.broker(leaderId)))
        .flatMap(_.node(listenerName).asScala)
        .getOrElse(Node.noNode)
      val log = partition.localLogOrException
      partitionAndOffsets.put(topicPartition, InitialFetchState(
        new BrokerEndPoint(node.id, node.host, node.port),
        partition.getLeaderEpoch,
        initialFetchOffset(log)
      ))
    }

    replicaFetcherManager.addFetcherForPartitions(partitionAndOffsets)
    stateChangeLogger.info(s"Started fetchers as part of become-follower for ${partitionsToMakeFollower.size} partitions")

    partitionsToMakeFollower.keySet.foreach(completeDelayedFetchOrProduceRequests)

    updateLeaderAndFollowerMetrics(newFollowerTopicSet)
  }

  def deleteStrayReplicas(topicPartitions: Iterable[TopicPartition]): Unit = {
    stopPartitions(topicPartitions.map(tp => tp -> true).toMap).forKeyValue { (topicPartition, exception) =>
      exception match {
        case e: KafkaStorageException =>
          stateChangeLogger.error(s"Unable to delete stray replica $topicPartition because " +
            s"the local replica for the partition is in an offline log directory: ${e.getMessage}.")
        case e: Throwable =>
          stateChangeLogger.error(s"Unable to delete stray replica $topicPartition because " +
            s"we got an unexpected ${e.getClass.getName} exception: ${e.getMessage}", e)
      }
    }
  }
}
