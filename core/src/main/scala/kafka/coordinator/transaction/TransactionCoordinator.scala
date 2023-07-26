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
package kafka.coordinator.transaction

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import kafka.server.{KafkaConfig, MetadataCache, ReplicaManager, RequestLocal}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.{DescribeTransactionsResponseData, ListTransactionsResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{LogContext, ProducerIdAndEpoch, Time}

object TransactionCoordinator {

  def apply(config: KafkaConfig,
            replicaManager: ReplicaManager,
            scheduler: Scheduler,
            createProducerIdGenerator: () => ProducerIdManager,
            metrics: Metrics,
            metadataCache: MetadataCache,
            time: Time): TransactionCoordinator = {

    val txnConfig = TransactionConfig(config.transactionalIdExpirationMs,
      config.transactionMaxTimeoutMs,
      config.transactionTopicPartitions,
      config.transactionTopicReplicationFactor,
      config.transactionTopicSegmentBytes,
      config.transactionsLoadBufferSize,
      config.transactionTopicMinISR,
      config.transactionAbortTimedOutTransactionCleanupIntervalMs,
      config.transactionRemoveExpiredTransactionalIdCleanupIntervalMs,
      config.requestTimeoutMs)

    val txnStateManager = new TransactionStateManager(config.brokerId, scheduler, replicaManager, txnConfig,
      time, metrics)

    val logContext = new LogContext(s"[TransactionCoordinator id=${config.brokerId}] ")
    val txnMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnStateManager,
      time, logContext)

    new TransactionCoordinator(config.brokerId, txnConfig, scheduler, createProducerIdGenerator, txnStateManager, txnMarkerChannelManager,
      time, logContext)
  }

  private def initTransactionError(error: Errors): InitProducerIdResult = {
    InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TxnTransitMetadata): InitProducerIdResult = {
    InitProducerIdResult(txnMetadata.producerId, txnMetadata.producerEpoch, Errors.NONE)
  }
}

/**
 * 事务协调器处理由生产者发送的消息事务，并与代理通信以更新正在进行的事务状态。
 *
 * 每个Kafka服务器实例化一个事务协调器，负责一组生产者。具有特定事务id的生产者被分配给相应的协调者;
 * 没有特定事务id的生产者可以与随机代理作为其协调者进行对话。
 */
class TransactionCoordinator(brokerId: Int,
                             txnConfig: TransactionConfig,
                             scheduler: Scheduler,
                             createProducerIdManager: () => ProducerIdManager,
                             txnManager: TransactionStateManager,
                             txnMarkerChannelManager: TransactionMarkerChannelManager,
                             time: Time,
                             logContext: LogContext) extends Logging {
  this.logIdent = logContext.logPrefix

  import TransactionCoordinator._

  type InitProducerIdCallback = InitProducerIdResult => Unit
  type AddPartitionsCallback = Errors => Unit
  type EndTxnCallback = Errors => Unit
  type ApiResult[T] = Either[Errors, T]

  /* 协调器的活动标志 */
  private val isActive = new AtomicBoolean(false)

  val producerIdManager = createProducerIdManager()

  def handleInitProducerId(transactionalId: String,
                           transactionTimeoutMs: Int,
                           expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch],
                           responseCallback: InitProducerIdCallback,
                           requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {

    if (transactionalId == null) {
      // 如果事务id为空，则总是盲目地接受请求，并从producerId管理器返回一个新的producerId
      val producerId = producerIdManager.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
    } else if (transactionalId.isEmpty) {
      // 如果事务id为空，则作为无效请求返回错误。这是为了使TransactionCoordinator的行为与生产者客户端保持一致
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
      // 检查transactionTimeoutMs不大于代理配置的最大允许值
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    } else {
      val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).flatMap {
        case None =>
          val producerId = producerIdManager.generateProducerId()
          val createdMetadata = new TransactionMetadata(transactionalId = transactionalId,
            producerId = producerId,
            lastProducerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            txnTimeoutMs = transactionTimeoutMs,
            state = Empty,
            topicPartitions = collection.mutable.Set.empty[TopicPartition],
            txnLastUpdateTimestamp = time.milliseconds())
          txnManager.putTransactionStateIfNotExists(createdMetadata)

        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
      }

      val result: ApiResult[(Int, TxnTransitMetadata)] = coordinatorEpochAndMetadata.flatMap {
        existingEpochAndMetadata =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata

          txnMetadata.inLock {
            prepareInitProducerIdTransit(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata,
              expectedProducerIdAndEpoch)
          }
      }

      result match {
        case Left(error) =>
          responseCallback(initTransactionError(error))

        case Right((coordinatorEpoch, newMetadata)) =>
          if (newMetadata.txnState == PrepareEpochFence) {
            // 中止正在进行的事务，然后返回CONCURRENT_TRANSACTIONS让客户端等待并重试
            def sendRetriableErrorCallback(error: Errors): Unit = {
              if (error != Errors.NONE) {
                responseCallback(initTransactionError(error))
              } else {
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
              }
            }

            endTransaction(transactionalId,
              newMetadata.producerId,
              newMetadata.producerEpoch,
              TransactionResult.ABORT,
              isFromClient = false,
              sendRetriableErrorCallback,
              requestLocal)
          } else {
            def sendPidResponseCallback(error: Errors): Unit = {
              if (error == Errors.NONE) {
                info(s"Initialized transactionalId $transactionalId with producerId ${newMetadata.producerId} and producer " +
                  s"epoch ${newMetadata.producerEpoch} on partition " +
                  s"${Topic.TRANSACTION_STATE_TOPIC_NAME}-${txnManager.partitionFor(transactionalId)}")
                responseCallback(initTransactionMetadata(newMetadata))
              } else {
                info(s"Returning $error error code to client for $transactionalId's InitProducerId request")
                responseCallback(initTransactionError(error))
              }
            }

            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
              sendPidResponseCallback, requestLocal = requestLocal)
          }
      }
    }
  }

  private def prepareInitProducerIdTransit(transactionalId: String,
                                           transactionTimeoutMs: Int,
                                           coordinatorEpoch: Int,
                                           txnMetadata: TransactionMetadata,
                                           expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch]): ApiResult[(Int, TxnTransitMetadata)] = {

    def isValidProducerId(producerIdAndEpoch: ProducerIdAndEpoch): Boolean = {
      // 如果请求中提供了生产者ID和epoch，除非下列情况之一为真，否则关闭生产者:
      //   1. 生产者epoch等于-1，这意味着元数据刚刚创建。这是生产者从UNKNOWN_PRODUCER_ID错误中恢复的情况，并且返回新生成的生产者ID是安全的.
      //   2. 预期的生产者ID与当前元数据中的ID匹配(当我们尝试增加epoch时将检查epoch)
      //   3. 预期的生产者ID与前一个匹配，并且预期的epoch耗尽，在这种情况下，这可能是在生产者从未收到响应的有效epoch碰撞之后重试
      txnMetadata.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH ||
        producerIdAndEpoch.producerId == txnMetadata.producerId ||
        (producerIdAndEpoch.producerId == txnMetadata.lastProducerId && TransactionMetadata.isEpochExhausted(producerIdAndEpoch.epoch))
    }

    if (txnMetadata.pendingTransitionInProgress) {
      // 返回一个可检索的异常，让客户端退出并重试
      Left(Errors.CONCURRENT_TRANSACTIONS)
    }
    else if (!expectedProducerIdAndEpoch.forall(isValidProducerId)) {
      Left(Errors.PRODUCER_FENCED)
    } else {
      // 调用者应该已经在txnMetadata上同步了
      txnMetadata.state match {
        case PrepareAbort | PrepareCommit =>
          // 回复客户端，让客户端退回并重试
          Left(Errors.CONCURRENT_TRANSACTIONS)

        case CompleteAbort | CompleteCommit | Empty =>
          val transitMetadataResult =
            // 如果epoch耗尽，并且预期的epoch(如果提供了)与之匹配，则生成一个新的生产者ID
            if (txnMetadata.isProducerEpochExhausted &&
                expectedProducerIdAndEpoch.forall(_.epoch == txnMetadata.producerEpoch)) {
              val newProducerId = producerIdManager.generateProducerId()
              Right(txnMetadata.prepareProducerIdRotation(newProducerId, transactionTimeoutMs, time.milliseconds(),
                expectedProducerIdAndEpoch.isDefined))
            } else {
              txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, expectedProducerIdAndEpoch.map(_.epoch),
                time.milliseconds())
            }

          transitMetadataResult match {
            case Right(transitMetadata) => Right((coordinatorEpoch, transitMetadata))
            case Left(err) => Left(err)
          }

        case Ongoing =>
          // 指示先中止当前正在进行的TXN。注意，这个epoch永远不会返回给用户。我们将中止正在进行的事务，
          // 并将CONCURRENT_TRANSACTIONS返回给客户端。这将强制客户端重试，这将确保第二次碰撞epoch。特别是，
          // 如果隔离当前生产者耗尽了当前生产者id的可用epoch，那么当客户端重试时，我们将生成一个新的生产者id。
          Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())

        case Dead | PrepareEpochFence =>
          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
            s"This is illegal as we should never have transitioned to this state."
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)
      }
    }
  }

  def handleListTransactions(
    filteredProducerIds: Set[Long],
    filteredStates: Set[String]
  ): ListTransactionsResponseData = {
    if (!isActive.get()) {
      new ListTransactionsResponseData().setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
    } else {
      txnManager.listTransactionStates(filteredProducerIds, filteredStates)
    }
  }

  def handleDescribeTransactions(
    transactionalId: String
  ): DescribeTransactionsResponseData.TransactionState = {
    if (transactionalId == null) {
      throw new IllegalArgumentException("Invalid null transactionalId")
    }

    val transactionState = new DescribeTransactionsResponseData.TransactionState()
      .setTransactionalId(transactionalId)

    if (!isActive.get()) {
      transactionState.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
    } else if (transactionalId.isEmpty) {
      transactionState.setErrorCode(Errors.INVALID_REQUEST.code)
    } else {
      txnManager.getTransactionState(transactionalId) match {
        case Left(error) =>
          transactionState.setErrorCode(error.code)
        case Right(None) =>
          transactionState.setErrorCode(Errors.TRANSACTIONAL_ID_NOT_FOUND.code)
        case Right(Some(coordinatorEpochAndMetadata)) =>
          val txnMetadata = coordinatorEpochAndMetadata.transactionMetadata
          txnMetadata.inLock {
            if (txnMetadata.state == Dead) {
              // The transaction state is being expired, so ignore it
              transactionState.setErrorCode(Errors.TRANSACTIONAL_ID_NOT_FOUND.code)
            } else {
              txnMetadata.topicPartitions.foreach { topicPartition =>
                var topicData = transactionState.topics.find(topicPartition.topic)
                if (topicData == null) {
                  topicData = new DescribeTransactionsResponseData.TopicData()
                    .setTopic(topicPartition.topic)
                  transactionState.topics.add(topicData)
                }
                topicData.partitions.add(topicPartition.partition)
              }

              transactionState
                .setErrorCode(Errors.NONE.code)
                .setProducerId(txnMetadata.producerId)
                .setProducerEpoch(txnMetadata.producerEpoch)
                .setTransactionState(txnMetadata.state.name)
                .setTransactionTimeoutMs(txnMetadata.txnTimeoutMs)
                .setTransactionStartTimeMs(txnMetadata.txnStartTimestamp)
            }
          }
      }
    }
  }

  def handleAddPartitionsToTransaction(transactionalId: String,
                                       producerId: Long,
                                       producerEpoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: AddPartitionsCallback,
                                       requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      debug(s"Returning ${Errors.INVALID_REQUEST} error code to client for $transactionalId's AddPartitions request")
      responseCallback(Errors.INVALID_REQUEST)
    } else {
      // 尝试更新事务元数据，并将更新后的元数据附加到TXN日志中;如果没有这样的元数据，将其视为无效的producerId映射错误。
      val result: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
        case None => Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndMetadata) =>
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // 生成带有添加分区的新事务元数据
          txnMetadata.inLock {
            if (txnMetadata.producerId != producerId) {
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.producerEpoch != producerEpoch) {
              Left(Errors.PRODUCER_FENCED)
            } else if (txnMetadata.pendingTransitionInProgress) {
              // 返回一个可检索的异常，让客户端退出并重试
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == Ongoing && partitions.subsetOf(txnMetadata.topicPartitions)) {
              // 这是一个优化:如果分区已经在元数据中，立即回复OK
              Left(Errors.NONE)
            } else {
              Right(coordinatorEpoch, txnMetadata.prepareAddPartitions(partitions.toSet, time.milliseconds()))
            }
          }
      }

      result match {
        case Left(err) =>
          debug(s"Returning $err error code to client for $transactionalId's AddPartitions request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
            responseCallback, requestLocal = requestLocal)
      }
    }
  }

  /**
   * 从给定分区加载状态，并开始处理映射到该分区的组的请求。
   *
   * @param txnTopicPartitionId 我们现在领导的分裂
   * @param coordinatorEpoch 来自收到的leaderanddisr请求的分区协调器(或leader) epoch
   */
  def onElection(txnTopicPartitionId: Int, coordinatorEpoch: Int): Unit = {
    info(s"Elected as the txn coordinator for partition $txnTopicPartitionId at epoch $coordinatorEpoch")
    // 在迁移期间执行的操作必须能够适应我们在卸载阶段看到的任何先前的错误或我们留下的部分状态。确保在继续加载该分区之前删除了
    // 该分区的所有相关状态。
    txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)

    // 现在加载分区。
    txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch,
      txnMarkerChannelManager.addTxnMarkersToSend)
  }

  /**
   * 在放弃领导后，清除给定分区的协调器缓存。
   *
   * @param txnTopicPartitionId The partition that we are no longer leading
   * @param coordinatorEpoch 分区协调器(或leader) epoch，如果我们在接收到来自控制器的StopReplica请求后退出，该epoch可能不存在
   */
  def onResignation(txnTopicPartitionId: Int, coordinatorEpoch: Option[Int]): Unit = {
    info(s"Resigned as the txn coordinator for partition $txnTopicPartitionId at epoch $coordinatorEpoch")
    coordinatorEpoch match {
      case Some(epoch) =>
        txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId, epoch)
      case None =>
        txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId)
    }
    txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }

  private def logInvalidStateTransitionAndReturnError(transactionalId: String,
                                                      transactionState: TransactionState,
                                                      transactionResult: TransactionResult) = {
    debug(s"TransactionalId: $transactionalId's state is $transactionState, but received transaction " +
      s"marker result to send: $transactionResult")
    Left(Errors.INVALID_TXN_STATE)
  }

  def handleEndTransaction(transactionalId: String,
                           producerId: Long,
                           producerEpoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback,
                           requestLocal: RequestLocal = RequestLocal.NoCaching): Unit = {
    endTransaction(transactionalId,
      producerId,
      producerEpoch,
      txnMarkerResult,
      isFromClient = true,
      responseCallback,
      requestLocal)
  }

  private def endTransaction(transactionalId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             txnMarkerResult: TransactionResult,
                             isFromClient: Boolean,
                             responseCallback: EndTxnCallback,
                             requestLocal: RequestLocal): Unit = {
    var isEpochFence = false
    if (transactionalId == null || transactionalId.isEmpty)
      responseCallback(Errors.INVALID_REQUEST)
    else {
      val preAppendResult: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
        case None =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch

          txnMetadata.inLock {
            if (txnMetadata.producerId != producerId)
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            // 在客户端请求中强制执行严格的平等，因为它们不应该碰到制作人时代。
            else if ((isFromClient && producerEpoch != txnMetadata.producerEpoch) || producerEpoch < txnMetadata.producerEpoch)
              Left(Errors.PRODUCER_FENCED)
            else if (txnMetadata.pendingTransitionInProgress && txnMetadata.pendingState.get != PrepareEpochFence)
              Left(Errors.CONCURRENT_TRANSACTIONS)
            else txnMetadata.state match {
              case Ongoing =>
                val nextState = if (txnMarkerResult == TransactionResult.COMMIT)
                  PrepareCommit
                else
                  PrepareAbort

                if (nextState == PrepareAbort && txnMetadata.pendingState.contains(PrepareEpochFence)) {
                  // 我们应该清除挂起状态，以便为转换到PrepareAbort让路，并在我们即将追加的事务元数据中撞击epoch。
                  isEpochFence = true
                  txnMetadata.pendingState = None
                  txnMetadata.producerEpoch = producerEpoch
                  txnMetadata.lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH
                }

                Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))
              case CompleteCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case CompleteAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Empty =>
                logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Dead | PrepareEpochFence =>
                val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                  s"This is illegal as we should never have transitioned to this state."
                fatal(errorMsg)
                throw new IllegalStateException(errorMsg)

            }
          }
      }

      preAppendResult match {
        case Left(err) =>
          debug(s"Aborting append of $txnMarkerResult to transaction log with coordinator and returning $err error to client for $transactionalId's EndTransaction request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          def sendTxnMarkersCallback(error: Errors): Unit = {
            if (error == Errors.NONE) {
              val preSendResult: ApiResult[(TransactionMetadata, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
                case None =>
                  val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                    s"no metadata in the cache; this is not expected"
                  fatal(errorMsg)
                  throw new IllegalStateException(errorMsg)

                case Some(epochAndMetadata) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                    val txnMetadata = epochAndMetadata.transactionMetadata
                    txnMetadata.inLock {
                      if (txnMetadata.producerId != producerId)
                        Left(Errors.INVALID_PRODUCER_ID_MAPPING)
                      else if (txnMetadata.producerEpoch != producerEpoch)
                        Left(Errors.PRODUCER_FENCED)
                      else if (txnMetadata.pendingTransitionInProgress)
                        Left(Errors.CONCURRENT_TRANSACTIONS)
                      else txnMetadata.state match {
                        case Empty| Ongoing | CompleteCommit | CompleteAbort =>
                          logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                        case PrepareCommit =>
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case PrepareAbort =>
                          if (txnMarkerResult != TransactionResult.ABORT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case Dead | PrepareEpochFence =>
                          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                            s"This is illegal as we should never have transitioned to this state."
                          fatal(errorMsg)
                          throw new IllegalStateException(errorMsg)

                      }
                    }
                  } else {
                    debug(s"The transaction coordinator epoch has changed to ${epochAndMetadata.coordinatorEpoch} after $txnMarkerResult was " +
                      s"successfully appended to the log for $transactionalId with old epoch $coordinatorEpoch")
                    Left(Errors.NOT_COORDINATOR)
                  }
              }

              preSendResult match {
                case Left(err) =>
                  info(s"Aborting sending of transaction markers after appended $txnMarkerResult to transaction log and returning $err error to client for $transactionalId's EndTransaction request")
                  responseCallback(err)

                case Right((txnMetadata, newPreSendMetadata)) =>
                  // 如果日志追加成功，我们可以立即响应客户机并继续写入TXN标记
                  responseCallback(Errors.NONE)

                  txnMarkerChannelManager.addTxnMarkersToSend(coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else {
              info(s"Aborting sending of transaction markers and returning $error error to client for $transactionalId's EndTransaction request of $txnMarkerResult, " +
                s"since appending $newMetadata to transaction log with coordinator epoch $coordinatorEpoch failed")

              if (isEpochFence) {
                txnManager.getTransactionState(transactionalId).foreach {
                  case None =>
                    warn(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                      s"no metadata in the cache; this is not expected")

                  case Some(epochAndMetadata) =>
                    if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                      // This was attempted epoch fence that failed, so mark this state on the metadata
                      epochAndMetadata.transactionMetadata.hasFailedEpochFence = true
                      warn(s"The coordinator failed to write an epoch fence transition for producer $transactionalId to the transaction log " +
                        s"with error $error. The epoch was increased to ${newMetadata.producerEpoch} but not returned to the client")
                    }
                }
              }

              responseCallback(error)
            }
          }

          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
            sendTxnMarkersCallback, requestLocal = requestLocal)
      }
    }
  }

  def transactionTopicConfigs: Properties = txnManager.transactionTopicConfigs

  def partitionFor(transactionalId: String): Int = txnManager.partitionFor(transactionalId)

  private def onEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)(error: Errors): Unit = {
    error match {
      case Errors.NONE =>
        info("Completed rollback of ongoing transaction for transactionalId " +
          s"${txnIdAndPidEpoch.transactionalId} due to timeout")

      case error@(Errors.INVALID_PRODUCER_ID_MAPPING |
                  Errors.PRODUCER_FENCED |
                  Errors.CONCURRENT_TRANSACTIONS) =>
        debug(s"Rollback of ongoing transaction for transactionalId ${txnIdAndPidEpoch.transactionalId} " +
          s"has been cancelled due to error $error")

      case error =>
        warn(s"Rollback of ongoing transaction for transactionalId ${txnIdAndPidEpoch.transactionalId} " +
          s"failed due to error $error")
    }
  }

  private[transaction] def abortTimedOutTransactions(onComplete: TransactionalIdAndProducerIdEpoch => EndTxnCallback): Unit = {

    txnManager.timedOutTransactions().foreach { txnIdAndPidEpoch =>
      txnManager.getTransactionState(txnIdAndPidEpoch.transactionalId).foreach {
        case None =>
          error(s"Could not find transaction metadata when trying to timeout transaction for $txnIdAndPidEpoch")

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val transitMetadataOpt = txnMetadata.inLock {
            if (txnMetadata.producerId != txnIdAndPidEpoch.producerId) {
              error(s"Found incorrect producerId when expiring transactionalId: ${txnIdAndPidEpoch.transactionalId}. " +
                s"Expected producerId: ${txnIdAndPidEpoch.producerId}. Found producerId: " +
                s"${txnMetadata.producerId}")
              None
            } else if (txnMetadata.pendingTransitionInProgress) {
              debug(s"Skipping abort of timed out transaction $txnIdAndPidEpoch since there is a " +
                "pending state transition")
              None
            } else {
              Some(txnMetadata.prepareFenceProducerEpoch())
            }
          }

          transitMetadataOpt.foreach { txnTransitMetadata =>
            endTransaction(txnMetadata.transactionalId,
              txnTransitMetadata.producerId,
              txnTransitMetadata.producerEpoch,
              TransactionResult.ABORT,
              isFromClient = false,
              onComplete(txnIdAndPidEpoch),
              RequestLocal.NoCaching)
          }
      }
    }
  }

  /**
   * 启动逻辑在服务器启动时同时执行。
   */
  def startup(retrieveTransactionTopicPartitionCount: () => Int, enableTransactionalIdExpiration: Boolean = true): Unit = {
    info("Starting up.")
    scheduler.startup()
    scheduler.schedule("transaction-abort",
      () => abortTimedOutTransactions(onEndTransactionComplete),
      txnConfig.abortTimedOutTransactionsIntervalMs,
      txnConfig.abortTimedOutTransactionsIntervalMs
    )
    txnManager.startup(retrieveTransactionTopicPartitionCount, enableTransactionalIdExpiration)
    txnMarkerChannelManager.start()
    isActive.set(true)

    info("Startup complete.")
  }

  /**
   * 在服务器关闭的同时执行关机逻辑。动作的顺序应该从启动过程中颠倒过来。
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    scheduler.shutdown()
    producerIdManager.shutdown()
    txnManager.shutdown()
    txnMarkerChannelManager.shutdown()
    info("Shutdown complete.")
  }
}

case class InitProducerIdResult(producerId: Long, producerEpoch: Short, error: Errors)
