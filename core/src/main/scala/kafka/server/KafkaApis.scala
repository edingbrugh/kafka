
package kafka.server

import kafka.admin.AdminUtils
import kafka.api.{ApiVersion, ElectLeadersRequestOps, KAFKA_0_11_0_IV0, KAFKA_2_3_IV0}
import kafka.common.OffsetAndMetadata
import kafka.controller.ReplicaAssignment
import kafka.coordinator.group._
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.log.AppendOrigin
import kafka.message.ZStdCompressionCodec
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.server.metadata.ConfigRepository
import kafka.utils.Implicits._
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.internals.{FatalExitError, Topic}
import org.apache.kafka.common.message.AlterConfigsResponseData.AlterConfigsResourceResponse
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.{ReassignablePartitionResponse, ReassignableTopicResponse}
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.{CreatableTopicResult, CreatableTopicResultCollection}
import org.apache.kafka.common.message.DeleteGroupsResponseData.{DeletableGroupResult, DeletableGroupResultCollection}
import org.apache.kafka.common.message.DeleteRecordsResponseData.{DeleteRecordsPartitionResult, DeleteRecordsTopicResult}
import org.apache.kafka.common.message.DeleteTopicsResponseData.{DeletableTopicResult, DeletableTopicResultCollection}
import org.apache.kafka.common.message.ElectLeadersResponseData.{PartitionResult, ReplicaElectionResult}
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult, OffsetForLeaderTopicResultCollection}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ListenerName, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{ProducerIdAndEpoch, Time}
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.server.authorizer._

import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Optional}
import scala.annotation.nowarn
import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * todo 处理各种Kafka请求的逻辑
 */
class KafkaApis(val requestChannel: RequestChannel,
                val metadataSupport: MetadataSupport,
                val replicaManager: ReplicaManager,
                val groupCoordinator: GroupCoordinator,
                val txnCoordinator: TransactionCoordinator,
                val autoTopicCreationManager: AutoTopicCreationManager,
                val brokerId: Int,
                val config: KafkaConfig,
                val configRepository: ConfigRepository,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer],
                val quotas: QuotaManagers,
                val fetchManager: FetchManager,
                brokerTopicStats: BrokerTopicStats,
                val clusterId: String,
                time: Time,
                val tokenManager: DelegationTokenManager,
                val apiVersionManager: ApiVersionManager) extends ApiRequestHandler with Logging {

  type FetchResponseStats = Map[TopicPartition, RecordConversionStats]
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  val configHelper = new ConfigHelper(metadataCache, config, configRepository)
  val authHelper = new AuthHelper(authorizer)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)
  val aclApis = new AclApis(authHelper, authorizer, requestHelper, "broker", config)

  def close(): Unit = {
    aclApis.close()
    info("Shutdown complete.")
  }

  private def isForwardingEnabled(request: RequestChannel.Request): Boolean = {
    metadataSupport.forwardingManager.isDefined && request.context.principalSerde.isPresent
  }

  private def maybeForwardToController(
                                        request: RequestChannel.Request,
                                        handler: RequestChannel.Request => Unit
                                      ): Unit = {
    def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
      responseOpt match {
        case Some(response) => requestHelper.sendForwardedResponse(request, response)
        case None =>
          info(s"The client connection will be closed due to controller responded " +
            s"unsupported version exception during $request forwarding. " +
            s"This could happen when the controller changed after the connection was established.")
          requestChannel.closeConnection(request, Collections.emptyMap())
      }
    }

    metadataSupport.maybeForward(request, handler, responseCallback)
  }

  private def forwardToControllerOrFail(
                                         request: RequestChannel.Request
                                       ): Unit = {
    def errorHandler(request: RequestChannel.Request): Unit = {
      throw new IllegalStateException(s"Unable to forward $request to the controller")
    }

    maybeForwardToController(request, errorHandler)
  }

  /**
   * 处理所有请求和到正确api的多路复用的顶级方法
   */
  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

      if (!apiVersionManager.isApiEnabled(request.header.apiKey)) {
        // 套接字服务器将拒绝在此范围内未公开的api，并在将它们发送给请求处理程序之前关闭连接，因此在实践中不应该使用此路径
        throw new IllegalStateException(s"API ${request.header.apiKey} is not enabled")
      }

      request.header.apiKey match {
        case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
        case ApiKeys.UPDATE_METADATA => handleUpdateMetadataRequest(request, requestLocal)
        case ApiKeys.CONTROLLED_SHUTDOWN => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request, requestLocal)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request, requestLocal)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request, requestLocal)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.CREATE_TOPICS => maybeForwardToController(request, handleCreateTopicsRequest)
        case ApiKeys.DELETE_TOPICS => maybeForwardToController(request, handleDeleteTopicsRequest)
        case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request, requestLocal)
        case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionToTxnRequest(request, requestLocal)
        case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request, requestLocal)
        case ApiKeys.END_TXN => handleEndTxnRequest(request, requestLocal)
        case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request, requestLocal)
        case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request, requestLocal)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => maybeForwardToController(request, handleCreateAcls)
        case ApiKeys.DELETE_ACLS => maybeForwardToController(request, handleDeleteAcls)
        case ApiKeys.ALTER_CONFIGS => maybeForwardToController(request, handleAlterConfigsRequest)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.ALTER_REPLICA_LOG_DIRS => handleAlterReplicaLogDirsRequest(request)
        case ApiKeys.DESCRIBE_LOG_DIRS => handleDescribeLogDirsRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.CREATE_PARTITIONS => maybeForwardToController(request, handleCreatePartitionsRequest)
        case ApiKeys.CREATE_DELEGATION_TOKEN => maybeForwardToController(request, handleCreateTokenRequest)
        case ApiKeys.RENEW_DELEGATION_TOKEN => maybeForwardToController(request, handleRenewTokenRequest)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => maybeForwardToController(request, handleExpireTokenRequest)
        case ApiKeys.DESCRIBE_DELEGATION_TOKEN => handleDescribeTokensRequest(request)
        case ApiKeys.DELETE_GROUPS => handleDeleteGroupsRequest(request, requestLocal)
        case ApiKeys.ELECT_LEADERS => handleElectReplicaLeader(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => maybeForwardToController(request, handleIncrementalAlterConfigsRequest)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => maybeForwardToController(request, handleAlterPartitionReassignmentsRequest)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => maybeForwardToController(request, handleListPartitionReassignmentsRequest)
        case ApiKeys.OFFSET_DELETE => handleOffsetDeleteRequest(request, requestLocal)
        case ApiKeys.DESCRIBE_CLIENT_QUOTAS => handleDescribeClientQuotasRequest(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => maybeForwardToController(request, handleAlterClientQuotasRequest)
        case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => handleDescribeUserScramCredentialsRequest(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => maybeForwardToController(request, handleAlterUserScramCredentialsRequest)
        case ApiKeys.ALTER_ISR => handleAlterIsrRequest(request)
        case ApiKeys.UPDATE_FEATURES => maybeForwardToController(request, handleUpdateFeatures)
        case ApiKeys.ENVELOPE => handleEnvelope(request, requestLocal)
        case ApiKeys.DESCRIBE_CLUSTER => handleDescribeCluster(request)
        case ApiKeys.DESCRIBE_PRODUCERS => handleDescribeProducersRequest(request)
        case ApiKeys.DESCRIBE_TRANSACTIONS => handleDescribeTransactionsRequest(request)
        case ApiKeys.LIST_TRANSACTIONS => handleListTransactionsRequest(request)
        case ApiKeys.ALLOCATE_PRODUCER_IDS => handleAllocateProducerIdsRequest(request)
        case ApiKeys.DESCRIBE_QUORUM => forwardToControllerOrFail(request)
        case _ => throw new IllegalStateException(s"No handler for request api key ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable =>
        error(s"Unexpected error handling request ${request.requestDesc(true)} " +
          s"with context ${request.context}", e)
        requestHelper.handleError(request, e)
    } finally {
      // 尝试完成延迟的动作。为了避免冲突锁定，完成延迟请求的操作保存在队列中。我们添加了在KafkaApis.handle()结束时检查
      // ReplicaManager队列的逻辑，以及某些延迟操作(例如DelayedJoin)的过期线程。
      replicaManager.tryCompleteActions()
      // 本地完成时间可以在处理请求时设置。只有在未设置时才记录。
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    // 这里不能使用ensureTopicExists检查，因为控制器会将其作为通知发送给所有代理，这样它们就会停止为客户端提供删除主题的数据
    val correlationId = request.header.correlationId
    val leaderAndIsrRequest = request.body[LeaderAndIsrRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (isBrokerEpochStale(zkSupport, leaderAndIsrRequest.brokerEpoch)) {
      // 当代理非常快速地重新启动时，该代理可能会接收到针对其上一代的请求，因此代理应该跳过过时的请求。
      info("Received LeaderAndIsr request with broker epoch " +
        s"${leaderAndIsrRequest.brokerEpoch} smaller than the current broker epoch ${zkSupport.controller.brokerEpoch}")
      requestHelper.sendResponseExemptThrottle(request, leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_BROKER_EPOCH.exception))
    } else {

      // todo 1 创建路径
      val response = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest,
        RequestHandlerHelper.onLeadershipChange(groupCoordinator, txnCoordinator, _, _))
      requestHelper.sendResponseExemptThrottle(request, response)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    // 这里不能使用ensureTopicExists检查，因为控制器会将其作为通知发送给所有代理，这样它们就会停止为客户端提供删除主题的数据
    val stopReplicaRequest = request.body[StopReplicaRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (isBrokerEpochStale(zkSupport, stopReplicaRequest.brokerEpoch)) {
      // 当代理非常快速地重新启动时，该代理可能会接收到针对其上一代的请求，因此代理应该跳过过时的请求。
      info("Received StopReplica request with broker epoch " +
        s"${stopReplicaRequest.brokerEpoch} smaller than the current broker epoch ${zkSupport.controller.brokerEpoch}")
      requestHelper.sendResponseExemptThrottle(request, new StopReplicaResponse(
        new StopReplicaResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code)))
    } else {
      val partitionStates = stopReplicaRequest.partitionStates().asScala
      val (result, error) = replicaManager.stopReplicas(
        request.context.correlationId,
        stopReplicaRequest.controllerId,
        stopReplicaRequest.controllerEpoch,
        partitionStates)
      // 清空协调器缓存，以防我们是领头的。在重新分配的情况下，我们不能依赖于LeaderAndIsr API，因为它只发送给活动副本。
      result.forKeyValue { (topicPartition, error) =>
        if (error == Errors.NONE) {
          val partitionState = partitionStates(topicPartition)
          if (topicPartition.topic == GROUP_METADATA_TOPIC_NAME
            && partitionState.deletePartition) {
            val leaderEpoch = if (partitionState.leaderEpoch >= 0)
              Some(partitionState.leaderEpoch)
            else
              None
            groupCoordinator.onResignation(topicPartition.partition, leaderEpoch)
          } else if (topicPartition.topic == TRANSACTION_STATE_TOPIC_NAME
            && partitionState.deletePartition) {
            val leaderEpoch = if (partitionState.leaderEpoch >= 0)
              Some(partitionState.leaderEpoch)
            else
              None
            txnCoordinator.onResignation(topicPartition.partition, coordinatorEpoch = leaderEpoch)
          }
        }
      }

      def toStopReplicaPartition(tp: TopicPartition, error: Errors) =
        new StopReplicaResponseData.StopReplicaPartitionError()
          .setTopicName(tp.topic)
          .setPartitionIndex(tp.partition)
          .setErrorCode(error.code)

      requestHelper.sendResponseExemptThrottle(request, new StopReplicaResponse(new StopReplicaResponseData()
        .setErrorCode(error.code)
        .setPartitionErrors(result.map {
          case (tp, error) => toStopReplicaPartition(tp, error)
        }.toBuffer.asJava)))
    }

    CoreUtils.swallow(replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads(), this)
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    val correlationId = request.header.correlationId
    val updateMetadataRequest = request.body[UpdateMetadataRequest]

    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    if (isBrokerEpochStale(zkSupport, updateMetadataRequest.brokerEpoch)) {
      // 当代理非常快速地重新启动时，该代理可能会接收到针对其上一代的请求，因此代理应该跳过过时的请求。
      info("Received update metadata request with broker epoch " +
        s"${updateMetadataRequest.brokerEpoch} smaller than the current broker epoch ${zkSupport.controller.brokerEpoch}")
      requestHelper.sendResponseExemptThrottle(request,
        new UpdateMetadataResponse(new UpdateMetadataResponseData().setErrorCode(Errors.STALE_BROKER_EPOCH.code)))
    } else {
      val deletedPartitions = replicaManager.maybeUpdateMetadataCache(correlationId, updateMetadataRequest)
      if (deletedPartitions.nonEmpty)
        groupCoordinator.handleDeletedPartitions(deletedPartitions, requestLocal)

      if (zkSupport.adminManager.hasDelayedTopicOperations) {
        updateMetadataRequest.partitionStates.forEach { partitionState =>
          zkSupport.adminManager.tryCompleteDelayedTopicOperations(partitionState.topicName)
        }
      }

      quotas.clientQuotaCallback.foreach { callback =>
        if (callback.updateClusterMetadata(metadataCache.getClusterMetadata(clusterId, request.context.listenerName))) {
          quotas.fetch.updateQuotaMetricConfigs()
          quotas.produce.updateQuotaMetricConfigs()
          quotas.request.updateQuotaMetricConfigs()
          quotas.controllerMutation.updateQuotaMetricConfigs()
        }
      }
      if (replicaManager.hasDelayedElectionOperations) {
        updateMetadataRequest.partitionStates.forEach { partitionState =>
          val tp = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
          replicaManager.tryCompleteElection(TopicPartitionOperationKey(tp))
        }
      }
      requestHelper.sendResponseExemptThrottle(request, new UpdateMetadataResponse(
        new UpdateMetadataResponseData().setErrorCode(Errors.NONE.code)))
    }
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    // 这里不能使用ensureTopicExists检查，因为控制器会将其作为通知发送给所有代理，这样它们就会停止为客户端提供删除主题的数据
    val controlledShutdownRequest = request.body[ControlledShutdownRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    def controlledShutdownCallback(controlledShutdownResult: Try[Set[TopicPartition]]): Unit = {
      val response = controlledShutdownResult match {
        case Success(partitionsRemaining) =>
          ControlledShutdownResponse.prepareResponse(Errors.NONE, partitionsRemaining.asJava)

        case Failure(throwable) =>
          controlledShutdownRequest.getErrorResponse(throwable)
      }
      requestHelper.sendResponseExemptThrottle(request, response)
    }

    zkSupport.controller.controlledShutdown(controlledShutdownRequest.data.brokerId, controlledShutdownRequest.data.brokerEpoch, controlledShutdownCallback)
  }

  /**
   * 处理偏移提交请求
   */
  def handleOffsetCommitRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val header = request.header
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
    val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()

    // 发送偏移量提交响应的回调函数
    def sendResponseCallback(commitStatus: Map[TopicPartition, Errors]): Unit = {
      val combinedCommitStatus = commitStatus ++ unauthorizedTopicErrors ++ nonExistingTopicErrors
      if (isDebugEnabled)
        combinedCommitStatus.forKeyValue { (topicPartition, error) =>
          if (error != Errors.NONE) {
            debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
              s"on partition $topicPartition failed due to ${error.exceptionName}")
          }
        }
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new OffsetCommitResponse(requestThrottleMs, combinedCommitStatus.asJava))
    }

    // 如果未授权给组，则拒绝请求
    if (!authHelper.authorize(request.context, READ, GROUP, offsetCommitRequest.data.groupId)) {
      val error = Errors.GROUP_AUTHORIZATION_FAILED
      val responseTopicList = OffsetCommitRequest.getErrorResponseTopics(
        offsetCommitRequest.data.topics,
        error)

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => new OffsetCommitResponse(
        new OffsetCommitResponseData()
          .setTopics(responseTopicList)
          .setThrottleTimeMs(requestThrottleMs)
      ))
    } else if (offsetCommitRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // 只在IBP >= 2.3时启用静态成员关系，因为在我们确定所有代理都支持静态成员逻辑之前，代理使用静态成员逻辑是不安全的。
      // 如果静态组是由旧的协调器加载的，它将丢弃group.instance.id字段，因此静态成员可能意外地变成“动态”，从而导致错误的状态。
      val errorMap = new mutable.HashMap[TopicPartition, Errors]
      for (topicData <- offsetCommitRequest.data.topics.asScala) {
        for (partitionData <- topicData.partitions.asScala) {
          val topicPartition = new TopicPartition(topicData.name, partitionData.partitionIndex)
          errorMap += topicPartition -> Errors.UNSUPPORTED_VERSION
        }
      }
      sendResponseCallback(errorMap.toMap)
    } else {
      val authorizedTopicRequestInfoBldr = immutable.Map.newBuilder[TopicPartition, OffsetCommitRequestData.OffsetCommitRequestPartition]

      val topics = offsetCommitRequest.data.topics.asScala
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, topics)(_.name)
      for (topicData <- topics) {
        for (partitionData <- topicData.partitions.asScala) {
          val topicPartition = new TopicPartition(topicData.name, partitionData.partitionIndex)
          if (!authorizedTopics.contains(topicData.name))
            unauthorizedTopicErrors += (topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED)
          else if (!metadataCache.contains(topicPartition))
            nonExistingTopicErrors += (topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            authorizedTopicRequestInfoBldr += (topicPartition -> partitionData)
        }
      }

      val authorizedTopicRequestInfo = authorizedTopicRequestInfoBldr.result()

      if (authorizedTopicRequestInfo.isEmpty)
        sendResponseCallback(Map.empty)
      else if (header.apiVersion == 0) {
        // 对于版本0总是存储偏移量到ZK
        val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.unsupported("Version 0 offset commit requests"))
        val responseInfo = authorizedTopicRequestInfo.map {
          case (topicPartition, partitionData) =>
            try {
              if (partitionData.committedMetadata() != null
                && partitionData.committedMetadata().length > config.offsetMetadataMaxSize)
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
              else {
                zkSupport.zkClient.setOrCreateConsumerOffset(
                  offsetCommitRequest.data.groupId,
                  topicPartition,
                  partitionData.committedOffset)
                (topicPartition, Errors.NONE)
              }
            } catch {
              case e: Throwable => (topicPartition, Errors.forException(e))
            }
        }
        sendResponseCallback(responseInfo)
      } else {
        // 对于版本1及以后的版本，在偏移量管理器中存储偏移量

        // "default" 过期时间戳现在是+保留(如果v2可能会覆盖保留)v1和v2的过期时间戳计算方式不同。
        //   - 如果没有提供明确的提交时间戳，我们将其视为与v5相同。
        //   - 如果提供了v1和显式保留时间，我们将基于此计算过期时间戳
        //   - 如果是v2/v3/v4(没有明确的提交时间戳)，我们将其视为与v5相同。
        //   - 对于v5及更高版本，没有每个分区过期时间戳，因此该字段不再有效
        val currentTimestamp = time.milliseconds
        val partitionData = authorizedTopicRequestInfo.map { case (k, partitionData) =>
          val metadata = if (partitionData.committedMetadata == null)
            OffsetAndMetadata.NoMetadata
          else
            partitionData.committedMetadata

          val leaderEpochOpt = if (partitionData.committedLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
            Optional.empty[Integer]
          else
            Optional.of[Integer](partitionData.committedLeaderEpoch)

          k -> new OffsetAndMetadata(
            offset = partitionData.committedOffset,
            leaderEpoch = leaderEpochOpt,
            metadata = metadata,
            commitTimestamp = partitionData.commitTimestamp match {
              case OffsetCommitRequest.DEFAULT_TIMESTAMP => currentTimestamp
              case customTimestamp => customTimestamp
            },
            expireTimestamp = offsetCommitRequest.data.retentionTimeMs match {
              case OffsetCommitRequest.DEFAULT_RETENTION_TIME => None
              case retentionTime => Some(currentTimestamp + retentionTime)
            }
          )
        }

        // 调用协调器来处理提交偏移量
        groupCoordinator.handleCommitOffsets(
          offsetCommitRequest.data.groupId,
          offsetCommitRequest.data.memberId,
          Option(offsetCommitRequest.data.groupInstanceId),
          offsetCommitRequest.data.generationId,
          partitionData,
          sendResponseCallback,
          requestLocal)
      }
    }
  }

  /**
   * 处理生产者信息
   */
  def handleProduceRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val produceRequest = request.body[ProduceRequest]
    val requestSize = request.sizeInBytes

    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      val isAuthorizedTransactional = produceRequest.transactionalId != null &&
        authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, produceRequest.transactionalId)
      if (!isAuthorizedTransactional) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    }

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()
    // 缓存结果以避免冗余的授权调用
    val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC,
      produceRequest.data().topicData().asScala)(_.name())

    produceRequest.data.topicData.forEach(topic => topic.partitionData.forEach { partition =>
      val topicPartition = new TopicPartition(topic.name, partition.index)
      // 这个调用者假定类型是MemoryRecords，在当前序列化中也是如此。我们强制转换类型是为了避免对代码基造成较大的更改。
      val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        try {
          ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)
          authorizedRequestInfo += (topicPartition -> memoryRecords)
        } catch {
          case e: ApiException =>
            invalidRequestResponses += topicPartition -> new PartitionResponse(Errors.forException(e))
        }
    })

    // producerResponse的构造能够接受自动生成的协议数据，
    // 所以KafkaApisHandleProduceRequest应该应用自动生成的协议来避免额外的转换。
    @nowarn("cat=deprecation")
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))
        }
      }

      // 记录带宽和请求配额特定的值，如果违反了任何配额，则通过静音通道进行节流。如果违反了两个配额，
      // 则使用两个配额之间的最大节流时间。请注意，如果acks == 0，则不强制执行请求配额。
      val timeMs = time.milliseconds()
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request, requestSize, timeMs)
      val requestThrottleTimeMs =
        if (produceRequest.acks == 0) 0
        else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
      val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
      if (maxThrottleTimeMs > 0) {
        request.apiThrottleTimeMs = maxThrottleTimeMs
        if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
          requestHelper.throttle(quotas.produce, request, bandwidthThrottleTimeMs)
        } else {
          requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
        }
      }

      // 立即发送响应。在节流的情况下，信道已经被静音。
      if (produceRequest.acks == 0) {
        // 如果生产者request.required.acks = 0，则不需要操作;但是，如果在处理请求时出现任何错误，
        // 由于生产者不期望响应，服务器将关闭套接字服务器，以便生产者客户端知道发生了错误并刷新其元数据
        if (errorInResponse) {
          val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
            topicPartition -> status.error.exceptionName
          }.mkString(", ")
          info(
            s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
              s"from client id ${request.header.clientId} with ack=0\n" +
              s"Topic and partition to exceptions: $exceptionsSummary"
          )
          requestChannel.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          // 请注意，尽管请求限制对于acks == 0是免除的，但是由于带宽配额违反，通道可能会被限制。
          requestHelper.sendNoOpResponseExemptThrottle(request)
        }
      } else {
        requestChannel.sendResponse(request, new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs), None)
      }
    }

    def processingStatsCallback(processingStats: FetchResponseStats): Unit = {
      processingStats.forKeyValue { (tp, info) =>
        updateRecordConversionStats(request, tp, info)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId

      // 调用副本管理器将消息附加到副本
      replicaManager.appendRecords(
        timeout = produceRequest.timeout.toLong,
        requiredAcks = produceRequest.acks,
        internalTopicsAllowed = internalTopicsAllowed,
        origin = AppendOrigin.Client,
        entriesPerPartition = authorizedRequestInfo,
        requestLocal = requestLocal,
        responseCallback = sendResponseCallback,
        recordConversionStatsCallback = processingStatsCallback)
      // 如果请求被放入purgatory，它将有一个持有的引用，因此不能被垃圾收集;因此，我们在这里清除它的数据，以便让GC回收它的内存，因为它已经附加到日志
      produceRequest.clearPartitionRecords()
    }
  }

  /**
   * 处理取回请求
   */
  def handleFetchRequest(request: RequestChannel.Request): Unit = {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val fetchContext = fetchManager.newContext(
      fetchRequest.metadata,
      fetchRequest.fetchData,
      fetchRequest.toForget,
      fetchRequest.isFromFollower)

    val clientMetadata: Option[ClientMetadata] = if (versionId >= 11) {
      // Fetch API版本11添加了首选副本逻辑
      Some(new DefaultClientMetadata(
        fetchRequest.rackId,
        clientId,
        request.context.clientAddress,
        request.context.principal,
        request.context.listenerName.value))
    } else {
      None
    }

    val erroneous = mutable.ArrayBuffer[(TopicPartition, FetchResponseData.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // follower必须在ClusterResource上有ClusterAction才能获取分区数据。
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
        fetchContext.foreachPartition { (topicPartition, data) =>
          if (!metadataCache.contains(topicPartition))
            erroneous += topicPartition -> FetchResponse.partitionResponse(topicPartition.partition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            interesting += (topicPartition -> data)
        }
      } else {
        fetchContext.foreachPartition { (part, _) =>
          erroneous += part -> FetchResponse.partitionResponse(part.partition, Errors.TOPIC_AUTHORIZATION_FAILED)
        }
      }
    } else {
      // 普通的Kafka消费者需要读取每个分区的READ权限。
      val partitionDatas = new mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]
      fetchContext.foreachPartition { (topicPartition, partitionData) =>
        partitionDatas += topicPartition -> partitionData
      }
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, partitionDatas)(_._1.topic)
      partitionDatas.foreach { case (topicPartition, data) =>
        if (!authorizedTopics.contains(topicPartition.topic))
          erroneous += topicPartition -> FetchResponse.partitionResponse(topicPartition.partition, Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicPartition))
          erroneous += topicPartition -> FetchResponse.partitionResponse(topicPartition.partition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += (topicPartition -> data)
      }
    }

    def maybeDownConvertStorageError(error: Errors): Errors = {
      // 如果消费者发送FetchRequest V5或更早的版本，客户端库不能保证识别KafkaStorageException的错误代码。在这种情况下，
      // 客户端库会将KafkaStorageException转换为不可检索的UnknownServerException。如果FetchRequest版本<= 5，
      // 我们可以通过在响应中将KafkaStorageException转换为NotLeaderOrFollowerException来确保消费者将更新元数据并重试
      if (error == Errors.KAFKA_STORAGE_ERROR && versionId <= 5) {
        Errors.NOT_LEADER_OR_FOLLOWER
      } else {
        error
      }
    }

    def maybeConvertFetchedData(tp: TopicPartition,
                                partitionData: FetchResponseData.PartitionData): FetchResponseData.PartitionData = {
      val logConfig = replicaManager.getLogConfig(tp)

      if (logConfig.exists(_.compressionType == ZStdCompressionCodec.name) && versionId < 10) {
        trace(s"Fetching messages is disabled for ZStandard compressed partition $tp. Sending unsupported version response to $clientId.")
        FetchResponse.partitionResponse(tp.partition, Errors.UNSUPPORTED_COMPRESSION_TYPE)
      } else {
        // 当磁盘上的magic值大于获取请求版本所支持的值时，需要对获取的记录进行向下转换。如果代理间协议版本为' 3.0 '或更高版本，
        // 则日志配置消息格式版本始终为' 3.0 '(即魔术值为' v2 ')。因此，如果fetch版本为3或更低，我们总是通过下转换路径
        // (在极少数情况下，下转换可能不需要，但不值得为它们优化)。如果代理间协议版本低于“3.0”，
        // 我们将依赖日志配置消息格式版本作为磁盘魔法值的代理，以维持Kafka 0.10.0中最初引入的长期行为。一个重要的含义是，
        // 在生成单个消息后降级消息格式版本是不安全的(无论获取版本如何，代理都将返回没有降级转换的消息)。
        val unconvertedRecords = FetchResponse.recordsOrFail(partitionData)
        val downConvertMagic =
          logConfig.map(_.recordVersion.value).flatMap { magic =>
            if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1)
              Some(RecordBatch.MAGIC_VALUE_V0)
            else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3)
              Some(RecordBatch.MAGIC_VALUE_V1)
            else
              None
          }

        downConvertMagic match {
          case Some(magic) =>
            // 对于来自客户端的获取请求，检查是否为特定分区禁用了向下转换
            if (!fetchRequest.isFromFollower && !logConfig.forall(_.messageDownConversionEnable)) {
              trace(s"Conversion to message format ${downConvertMagic.get} is disabled for partition $tp. Sending unsupported version response to $clientId.")
              FetchResponse.partitionResponse(tp.partition, Errors.UNSUPPORTED_VERSION)
            } else {
              try {
                trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
                // 由于下转换非常占用内存，我们希望尽可能地延迟下转换。使用KIP-283，我们能够以分块方式惰性向下转换。延迟的、
                // 分块的向下转换总是保证至少有一批消息被向下转换并发送到客户端。
                new FetchResponseData.PartitionData()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
                  .setHighWatermark(partitionData.highWatermark)
                  .setLastStableOffset(partitionData.lastStableOffset)
                  .setLogStartOffset(partitionData.logStartOffset)
                  .setAbortedTransactions(partitionData.abortedTransactions)
                  .setRecords(new LazyDownConversionRecords(tp, unconvertedRecords, magic, fetchContext.getFetchOffset(tp).get, time))
                  .setPreferredReadReplica(partitionData.preferredReadReplica())
              } catch {
                case e: UnsupportedCompressionTypeException =>
                  trace("Received unsupported compression type error during down-conversion", e)
                  FetchResponse.partitionResponse(tp.partition, Errors.UNSUPPORTED_COMPRESSION_TYPE)
              }
            }
          case None =>
            new FetchResponseData.PartitionData()
              .setPartitionIndex(tp.partition)
              .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
              .setHighWatermark(partitionData.highWatermark)
              .setLastStableOffset(partitionData.lastStableOffset)
              .setLogStartOffset(partitionData.logStartOffset)
              .setAbortedTransactions(partitionData.abortedTransactions)
              .setRecords(unconvertedRecords)
              .setPreferredReadReplica(partitionData.preferredReadReplica)
              .setDivergingEpoch(partitionData.divergingEpoch)
        }
      }
    }

    // 处理获取响应的回调，在节流之前调用
    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
      val reassigningPartitions = mutable.Set[TopicPartition]()
      responsePartitionData.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        if (data.isReassignmentFetch) reassigningPartitions.add(tp)
        val partitionData = new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.partition)
          .setErrorCode(maybeDownConvertStorageError(data.error).code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
          .setPreferredReadReplica(data.preferredReadReplica.getOrElse(FetchResponse.INVALID_PREFERRED_REPLICA_ID))
        data.divergingEpoch.foreach(partitionData.setDivergingEpoch)
        partitions.put(tp, partitionData)
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      var unconvertedFetchResponse: FetchResponse = null

      def createResponse(throttleTimeMs: Int): FetchResponse = {
        // 如果需要，向下转换每个分区的消息
        val convertedData = new util.LinkedHashMap[TopicPartition, FetchResponseData.PartitionData]
        unconvertedFetchResponse.responseData.forEach { (tp, unconvertedPartitionData) =>
          val error = Errors.forCode(unconvertedPartitionData.errorCode)
          if (error != Errors.NONE)
            debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
              s"on partition $tp failed due to ${error.exceptionName}")
          convertedData.put(tp, maybeConvertFetchedData(tp, unconvertedPartitionData))
        }

        // 准备从转换后的数据获取响应
        val response = FetchResponse.of(unconvertedFetchResponse.error, throttleTimeMs, unconvertedFetchResponse.sessionId, convertedData)
        // 仅在发送响应时记录字节输出度量
        response.responseData.forEach { (tp, data) =>
          brokerTopicStats.updateBytesOut(tp.topic, fetchRequest.isFromFollower,
            reassigningPartitions.contains(tp), FetchResponse.recordsSize(data))
        }
        response
      }

      def updateConversionStats(send: Send): Unit = {
        send match {
          case send: MultiRecordsSend if send.recordConversionStats != null =>
            send.recordConversionStats.asScala.toMap.foreach {
              case (tp, stats) => updateRecordConversionStats(request, tp, stats)
            }
          case _ =>
        }
      }

      if (fetchRequest.isFromFollower) {
        // 我们已经根据配额进行了评估，准备好了。现在就录下来。
        unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
        val responseSize = KafkaApis.sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        trace(s"Sending Fetch response with partitions.size=${unconvertedFetchResponse.responseData.size}, " +
          s"metadata=${unconvertedFetchResponse.sessionId}")
        requestHelper.sendResponseExemptThrottle(request, createResponse(0), Some(updateConversionStats))
      } else {
        // 用于确定节流时间的取值大小在任何向下转换之前计算。这可能与实际响应大小略有不同。但是由于向下转换会导致数据被加载到内存中，
        // 所以我们应该只在不打算节流时才这样做。记录带宽和特定于请求配额的值，并在违反任何配额时通过使通道静音来进行节流。
        // 如果违反了两个配额，则使用两个配额之间的最大节流时间。当节流时，我们取消记录已记录的带宽配额值
        val responseSize = fetchContext.getResponseSize(partitions, versionId)
        val timeMs = time.milliseconds()
        val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
        val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, responseSize, timeMs)

        val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
        if (maxThrottleTimeMs > 0) {
          request.apiThrottleTimeMs = maxThrottleTimeMs
          // 即使我们需要限制请求配额违反，我们也应该“取消记录”已经从获取配额中记录的值，因为我们将返回一个空响应。
          quotas.fetch.unrecordQuotaSensor(request, responseSize, timeMs)
          if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
            requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
          } else {
            requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
          }
          // 如果需要节流，则返回空响应。
          unconvertedFetchResponse = fetchContext.getThrottledResponse(maxThrottleTimeMs)
        } else {
          // 获取实际响应。这将更新获取上下文。
          unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
          trace(s"Sending Fetch response with partitions.size=$responseSize, metadata=${unconvertedFetchResponse.sessionId}")
        }

        // 立即发送响应。
        requestChannel.sendResponse(request, createResponse(maxThrottleTimeMs), Some(updateConversionStats))
      }
    }

    // 如果在最近的配额窗口中没有记录字节，那么将fetchMaxBytes限制为可以在不进行节流的情况下提取的最大字节数。
    // 尝试获取更多字节将导致保证的节流，可能会阻塞消费者的进度
    val maxQuotaWindowBytes = if (fetchRequest.isFromFollower)
      Int.MaxValue
    else
      quotas.fetch.getMaxValueInQuotaWindow(request.session, clientId).toInt

    val fetchMaxBytes = Math.min(Math.min(fetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
    val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)
    if (interesting.isEmpty)
      processResponseCallback(Seq.empty)
    else {
      // 调用副本管理器从本地副本获取消息
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchMinBytes,
        fetchMaxBytes,
        versionId <= 2,
        interesting,
        replicationQuota(fetchRequest),
        processResponseCallback,
        fetchRequest.isolationLevel,
        clientMetadata)
    }
  }

  def replicationQuota(fetchRequest: FetchRequest): ReplicaQuota =
    if (fetchRequest.isFromFollower) quotas.leader else UnboundedQuota

  def handleListOffsetRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion

    val topics = if (version == 0)
      handleListOffsetRequestV0(request)
    else
      handleListOffsetRequestV1AndAbove(request)

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => new ListOffsetsResponse(new ListOffsetsResponseData()
      .setThrottleTimeMs(requestThrottleMs)
      .setTopics(topics.asJava)))
  }

  private def handleListOffsetRequestV0(request: RequestChannel.Request): List[ListOffsetsTopicResponse] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetsRequest]

    val (authorizedRequestInfo, unauthorizedRequestInfo) = authHelper.partitionSeqByAuthorized(request.context,
      DESCRIBE, TOPIC, offsetRequest.topics.asScala.toSeq)(_.name)

    val unauthorizedResponseStatus = unauthorizedRequestInfo.map(topic =>
      new ListOffsetsTopicResponse()
        .setName(topic.name)
        .setPartitions(topic.partitions.asScala.map(partition =>
          new ListOffsetsPartitionResponse()
            .setPartitionIndex(partition.partitionIndex)
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)).asJava)
    )

    val responseTopics = authorizedRequestInfo.map { topic =>
      val responsePartitions = topic.partitions.asScala.map { partition =>
        val topicPartition = new TopicPartition(topic.name, partition.partitionIndex)

        try {
          val offsets = replicaManager.legacyFetchOffsetsForTimestamp(
            topicPartition = topicPartition,
            timestamp = partition.timestamp,
            maxNumOffsets = partition.maxNumOffsets,
            isFromConsumer = offsetRequest.replicaId == ListOffsetsRequest.CONSUMER_REPLICA_ID,
            fetchOnlyFromLeader = offsetRequest.replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID)
          new ListOffsetsPartitionResponse()
            .setPartitionIndex(partition.partitionIndex)
            .setErrorCode(Errors.NONE.code)
            .setOldStyleOffsets(offsets.map(JLong.valueOf).asJava)
        } catch {
          // NOTE: UnknownTopicOrPartitionException and NotLeaderOrFollowerException are special cases since these error messages
          // are typically transient and there is no value in logging the entire stack trace for the same
          case e@(_: UnknownTopicOrPartitionException |
                  _: NotLeaderOrFollowerException |
                  _: KafkaStorageException) =>
            debug("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
              correlationId, clientId, topicPartition, e.getMessage))
            new ListOffsetsPartitionResponse()
              .setPartitionIndex(partition.partitionIndex)
              .setErrorCode(Errors.forException(e).code)
          case e: Throwable =>
            error("Error while responding to offset request", e)
            new ListOffsetsPartitionResponse()
              .setPartitionIndex(partition.partitionIndex)
              .setErrorCode(Errors.forException(e).code)
        }
      }
      new ListOffsetsTopicResponse().setName(topic.name).setPartitions(responsePartitions.asJava)
    }
    (responseTopics ++ unauthorizedResponseStatus).toList
  }

  private def handleListOffsetRequestV1AndAbove(request: RequestChannel.Request): List[ListOffsetsTopicResponse] = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetsRequest]
    val version = request.header.apiVersion

    def buildErrorResponse(e: Errors, partition: ListOffsetsPartition): ListOffsetsPartitionResponse = {
      new ListOffsetsPartitionResponse()
        .setPartitionIndex(partition.partitionIndex)
        .setErrorCode(e.code)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
    }

    val (authorizedRequestInfo, unauthorizedRequestInfo) = authHelper.partitionSeqByAuthorized(request.context,
      DESCRIBE, TOPIC, offsetRequest.topics.asScala.toSeq)(_.name)

    val unauthorizedResponseStatus = unauthorizedRequestInfo.map(topic =>
      new ListOffsetsTopicResponse()
        .setName(topic.name)
        .setPartitions(topic.partitions.asScala.map(partition =>
          buildErrorResponse(Errors.TOPIC_AUTHORIZATION_FAILED, partition)).asJava)
    )

    val responseTopics = authorizedRequestInfo.map { topic =>
      val responsePartitions = topic.partitions.asScala.map { partition =>
        val topicPartition = new TopicPartition(topic.name, partition.partitionIndex)
        if (offsetRequest.duplicatePartitions.contains(topicPartition)) {
          debug(s"OffsetRequest with correlation id $correlationId from client $clientId on partition $topicPartition " +
            s"failed because the partition is duplicated in the request.")
          buildErrorResponse(Errors.INVALID_REQUEST, partition)
        } else {
          try {
            val fetchOnlyFromLeader = offsetRequest.replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID
            val isClientRequest = offsetRequest.replicaId == ListOffsetsRequest.CONSUMER_REPLICA_ID
            val isolationLevelOpt = if (isClientRequest)
              Some(offsetRequest.isolationLevel)
            else
              None

            val foundOpt = replicaManager.fetchOffsetForTimestamp(topicPartition,
              partition.timestamp,
              isolationLevelOpt,
              if (partition.currentLeaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) Optional.empty() else Optional.of(partition.currentLeaderEpoch),
              fetchOnlyFromLeader)

            val response = foundOpt match {
              case Some(found) =>
                val partitionResponse = new ListOffsetsPartitionResponse()
                  .setPartitionIndex(partition.partitionIndex)
                  .setErrorCode(Errors.NONE.code)
                  .setTimestamp(found.timestamp)
                  .setOffset(found.offset)
                if (found.leaderEpoch.isPresent && version >= 4)
                  partitionResponse.setLeaderEpoch(found.leaderEpoch.get)
                partitionResponse
              case None =>
                buildErrorResponse(Errors.NONE, partition)
            }
            response
          } catch {
            // 注意:这些异常是特殊情况，因为这些错误消息通常是暂时的，或者客户端会收到一个明确的异常，并且记录整个堆栈跟踪是没有价值的
            case e@(_: UnknownTopicOrPartitionException |
                    _: NotLeaderOrFollowerException |
                    _: UnknownLeaderEpochException |
                    _: FencedLeaderEpochException |
                    _: KafkaStorageException |
                    _: UnsupportedForMessageFormatException) =>
              debug(s"Offset request with correlation id $correlationId from client $clientId on " +
                s"partition $topicPartition failed due to ${e.getMessage}")
              buildErrorResponse(Errors.forException(e), partition)

            // 只有V5和更新的ListOffset调用应该得到OFFSET_NOT_AVAILABLE
            case e: OffsetNotAvailableException =>
              if (request.header.apiVersion >= 5) {
                buildErrorResponse(Errors.forException(e), partition)
              } else {
                buildErrorResponse(Errors.LEADER_NOT_AVAILABLE, partition)
              }

            case e: Throwable =>
              error("Error while responding to offset request", e)
              buildErrorResponse(Errors.forException(e), partition)
          }
        }
      }
      new ListOffsetsTopicResponse().setName(topic.name).setPartitions(responsePartitions.asJava)
    }
    (responseTopics ++ unauthorizedResponseStatus).toList
  }

  private def metadataResponseTopic(error: Errors,
                                    topic: String,
                                    isInternal: Boolean,
                                    partitionData: util.List[MetadataResponsePartition]): MetadataResponseTopic = {
    new MetadataResponseTopic()
      .setErrorCode(error.code)
      .setName(topic)
      .setIsInternal(isInternal)
      .setPartitions(partitionData)
  }

  private def getTopicMetadata(
                                request: RequestChannel.Request,
                                fetchAllTopics: Boolean,
                                allowAutoTopicCreation: Boolean,
                                topics: Set[String],
                                listenerName: ListenerName,
                                errorUnavailableEndpoints: Boolean,
                                errorUnavailableListeners: Boolean
                              ): Seq[MetadataResponseTopic] = {
    val topicResponses = metadataCache.getTopicMetadata(topics, listenerName,
      errorUnavailableEndpoints, errorUnavailableListeners)

    if (topics.isEmpty || topicResponses.size == topics.size || fetchAllTopics) {
      topicResponses
    } else {
      val nonExistingTopics = topics.diff(topicResponses.map(_.name).toSet)
      val nonExistingTopicResponses = if (allowAutoTopicCreation) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(nonExistingTopics, controllerMutationQuota, Some(request.context))
      } else {
        nonExistingTopics.map { topic =>
          val error = try {
            Topic.validate(topic)
            Errors.UNKNOWN_TOPIC_OR_PARTITION
          } catch {
            case _: InvalidTopicException =>
              Errors.INVALID_TOPIC_EXCEPTION
          }

          metadataResponseTopic(
            error,
            topic,
            Topic.isInternal(topic),
            util.Collections.emptyList()
          )
        }
      }

      topicResponses ++ nonExistingTopicResponses
    }
  }

  def handleTopicMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion

    // 版本10和11不支持主题id。在这些版本中，主题名不能为空。
    if (!metadataRequest.isAllTopics) {
      metadataRequest.data.topics.forEach { topic =>
        if (topic.name == null) {
          throw new InvalidRequestException(s"Topic name can not be null for version ${metadataRequest.version}")
        } else if (topic.topicId != Uuid.ZERO_UUID) {
          throw new InvalidRequestException(s"Topic IDs are not supported in requests for version ${metadataRequest.version}")
        }
      }
    }

    val topics = if (metadataRequest.isAllTopics)
      metadataCache.getAllTopics()
    else
      metadataRequest.topics.asScala.toSet

    val authorizedForDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
      topics, logIfDenied = !metadataRequest.isAllTopics)(identity)
    var (authorizedTopics, unauthorizedForDescribeTopics) = topics.partition(authorizedForDescribeTopics.contains)
    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = authorizedTopics.filterNot(metadataCache.contains(_))
      if (metadataRequest.allowAutoTopicCreation && config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false)) {
          val authorizedForCreateTopics = authHelper.filterByAuthorized(request.context, CREATE, TOPIC,
            nonExistingTopics)(identity)
          unauthorizedForCreateTopics = nonExistingTopics.diff(authorizedForCreateTopics)
          authorizedTopics = authorizedTopics.diff(unauthorizedForCreateTopics)
        }
      }
    }

    val unauthorizedForCreateTopicMetadata = unauthorizedForCreateTopics.map(topic =>
      metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, isInternal(topic), util.Collections.emptyList()))

    // 没有透露未经授权的主题的存在，所以我们甚至没有检查它们是否存在
    val unauthorizedForDescribeTopicMetadata =
    // 在所有主题的情况下，不要包括未经授权描述的主题
      if ((requestVersion == 0 && (metadataRequest.topics == null || metadataRequest.topics.isEmpty)) || metadataRequest.isAllTopics)
        Set.empty[MetadataResponseTopic]
      else
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, false, util.Collections.emptyList()))

    // 在版本0中，当具有副本的代理不可用时，我们返回一个错误，而在更高版本中，我们只是不将代理包含在返回的代理列表中
    val errorUnavailableEndpoints = requestVersion == 0
    // 在版本5及更低的版本中，如果在leader上没有找到匹配的监听器，我们将返回LEADER_NOT_AVAILABLE。从版本6开始，
    // 我们返回LISTENER_NOT_FOUND以启用配置错误诊断。
    val errorUnavailableListeners = requestVersion >= 6

    val allowAutoCreation = config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation && !metadataRequest.isAllTopics
    val topicMetadata = getTopicMetadata(request, metadataRequest.isAllTopics, allowAutoCreation, authorizedTopics,
      request.context.listenerName, errorUnavailableEndpoints, errorUnavailableListeners)

    var clusterAuthorizedOperations = Int.MinValue // Default value in the schema
    if (requestVersion >= 8) {
      // 获取集群授权操作
      if (requestVersion <= 10) {
        if (metadataRequest.data.includeClusterAuthorizedOperations) {
          if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
            clusterAuthorizedOperations = authHelper.authorizedOperations(request, Resource.CLUSTER)
          else
            clusterAuthorizedOperations = 0
        }
      }

      // 获取主题授权操作
      if (metadataRequest.data.includeTopicAuthorizedOperations) {
        def setTopicAuthorizedOperations(topicMetadata: Seq[MetadataResponseTopic]): Unit = {
          topicMetadata.foreach { topicData =>
            topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(request, new Resource(ResourceType.TOPIC, topicData.name)))
          }
        }

        setTopicAuthorizedOperations(topicMetadata)
      }
    }

    val completeTopicMetadata = topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokerNodes(request.context.listenerName)

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.mkString(","), request.header.correlationId, request.header.clientId))

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      MetadataResponse.prepareResponse(
        requestVersion,
        requestThrottleMs,
        brokers.toList.asJava,
        clusterId,
        metadataSupport.controllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID),
        completeTopicMetadata.asJava,
        clusterAuthorizedOperations
      ))
  }

  /**
   * 处理偏移量获取请求
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion
    if (version == 0) {
      // 从ZK读取偏移量
      handleOffsetFetchRequestV0(request)
    } else if (version >= 1 && version <= 7) {
      // reading offsets from Kafka
      handleOffsetFetchRequestBetweenV1AndV7(request)
    } else {
      // 多组的批处理偏移读取从版本8及更高版本开始
      handleOffsetFetchRequestV8AndAbove(request)
    }
  }

  private def handleOffsetFetchRequestV0(request: RequestChannel.Request): Unit = {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val offsetFetchResponse =
      // 如果未授权给组，则拒绝请求
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, offsetFetchRequest.groupId))
          offsetFetchRequest.getErrorResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED)
        else {
          val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.unsupported("Version 0 offset fetch requests"))
          val (authorizedPartitions, unauthorizedPartitions) = partitionByAuthorized(
            offsetFetchRequest.partitions.asScala, request.context)

          // 版本0从ZK读取偏移量
          val authorizedPartitionData = authorizedPartitions.map { topicPartition =>
            try {
              if (!metadataCache.contains(topicPartition))
                (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
              else {
                val payloadOpt = zkSupport.zkClient.getConsumerOffset(offsetFetchRequest.groupId, topicPartition)
                payloadOpt match {
                  case Some(payload) =>
                    (topicPartition, new OffsetFetchResponse.PartitionData(payload.toLong,
                      Optional.empty(), OffsetFetchResponse.NO_METADATA, Errors.NONE))
                  case None =>
                    (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
                }
              }
            } catch {
              case e: Throwable =>
                (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                  Optional.empty(), OffsetFetchResponse.NO_METADATA, Errors.forException(e)))
            }
          }.toMap

          val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNAUTHORIZED_PARTITION).toMap
          new OffsetFetchResponse(requestThrottleMs, Errors.NONE, (authorizedPartitionData ++ unauthorizedPartitionData).asJava)
        }
      trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
      offsetFetchResponse
    }

    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  private def handleOffsetFetchRequestBetweenV1AndV7(request: RequestChannel.Request): Unit = {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]
    val groupId = offsetFetchRequest.groupId()
    val (error, partitionData) = fetchOffsets(groupId, offsetFetchRequest.isAllPartitions,
      offsetFetchRequest.requireStable, offsetFetchRequest.partitions, request.context)

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val offsetFetchResponse =
        if (error != Errors.NONE) {
          offsetFetchRequest.getErrorResponse(requestThrottleMs, error)
        } else {
          new OffsetFetchResponse(requestThrottleMs, Errors.NONE, partitionData.asJava)
        }
      trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
      offsetFetchResponse
    }

    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  private def handleOffsetFetchRequestV8AndAbove(request: RequestChannel.Request): Unit = {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]
    val groupIds = offsetFetchRequest.groupIds().asScala
    val groupToErrorMap = mutable.Map.empty[String, Errors]
    val groupToPartitionData = mutable.Map.empty[String, util.Map[TopicPartition, PartitionData]]
    val groupToTopicPartitions = offsetFetchRequest.groupIdsToPartitions()
    groupIds.foreach(g => {
      val (error, partitionData) = fetchOffsets(g,
        offsetFetchRequest.isAllPartitionsForGroup(g),
        offsetFetchRequest.requireStable(),
        groupToTopicPartitions.get(g), request.context)
      groupToErrorMap += (g -> error)
      groupToPartitionData += (g -> partitionData.asJava)
    })

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val offsetFetchResponse = new OffsetFetchResponse(requestThrottleMs,
        groupToErrorMap.asJava, groupToPartitionData.asJava)
      trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
      offsetFetchResponse
    }

    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  private def fetchOffsets(groupId: String, isAllPartitions: Boolean, requireStable: Boolean,
                           partitions: util.List[TopicPartition], context: RequestContext): (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {
    if (!authHelper.authorize(context, DESCRIBE, GROUP, groupId)) {
      (Errors.GROUP_AUTHORIZATION_FAILED, Map.empty)
    } else {
      if (isAllPartitions) {
        val (error, allPartitionData) = groupCoordinator.handleFetchOffsets(groupId, requireStable)
        if (error != Errors.NONE) {
          (error, allPartitionData)
        } else {
          // clients are not allowed to see offsets for topics that are not authorized for Describe
          val (authorizedPartitionData, _) = authHelper.partitionMapByAuthorized(context,
            DESCRIBE, TOPIC, allPartitionData)(_.topic)
          (Errors.NONE, authorizedPartitionData)
        }
      } else {
        val (authorizedPartitions, unauthorizedPartitions) = partitionByAuthorized(
          partitions.asScala, context)
        val (error, authorizedPartitionData) = groupCoordinator.handleFetchOffsets(groupId,
          requireStable, Some(authorizedPartitions))
        if (error != Errors.NONE) {
          (error, authorizedPartitionData)
        } else {
          val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNAUTHORIZED_PARTITION).toMap
          (Errors.NONE, authorizedPartitionData ++ unauthorizedPartitionData)
        }
      }
    }
  }

  private def partitionByAuthorized(seq: Seq[TopicPartition], context: RequestContext):
  (Seq[TopicPartition], Seq[TopicPartition]) =
    authHelper.partitionSeqByAuthorized(context, DESCRIBE, TOPIC, seq)(_.topic)

  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion
    if (version < 4) {
      handleFindCoordinatorRequestLessThanV4(request)
    } else {
      handleFindCoordinatorRequestV4AndAbove(request)
    }
  }

  private def handleFindCoordinatorRequestV4AndAbove(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val coordinators = findCoordinatorRequest.data.coordinatorKeys.asScala.map { key =>
      val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, key)
      new FindCoordinatorResponseData.Coordinator()
        .setKey(key)
        .setErrorCode(error.code)
        .setHost(node.host)
        .setNodeId(node.id)
        .setPort(node.port)
    }

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val response = new FindCoordinatorResponse(
        new FindCoordinatorResponseData()
          .setCoordinators(coordinators.asJava)
          .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
        .format(response, request.header.correlationId, request.header.clientId))
      response
    }

    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  private def handleFindCoordinatorRequestLessThanV4(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, findCoordinatorRequest.data.key)

    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val responseBody = new FindCoordinatorResponse(
        new FindCoordinatorResponseData()
          .setErrorCode(error.code)
          .setErrorMessage(error.message())
          .setNodeId(node.id)
          .setHost(node.host)
          .setPort(node.port)
          .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      responseBody
    }

    if (error == Errors.NONE) {
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    } else {
      requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  private def getCoordinator(request: RequestChannel.Request, keyType: Byte, key: String): (Errors, Node) = {
    if (keyType == CoordinatorType.GROUP.id &&
      !authHelper.authorize(request.context, DESCRIBE, GROUP, key))
      (Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode)
    else if (keyType == CoordinatorType.TRANSACTION.id &&
      !authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, key))
      (Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, Node.noNode)
    else {
      val (partition, internalTopicName) = CoordinatorType.forId(keyType) match {
        case CoordinatorType.GROUP =>
          (groupCoordinator.partitionFor(key), GROUP_METADATA_TOPIC_NAME)

        case CoordinatorType.TRANSACTION =>
          (txnCoordinator.partitionFor(key), TRANSACTION_STATE_TOPIC_NAME)
      }

      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName), request.context.listenerName)

      if (topicMetadata.headOption.isEmpty) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request)
        autoTopicCreationManager.createTopics(Seq(internalTopicName).toSet, controllerMutationQuota, None)
        (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
      } else {
        if (topicMetadata.head.errorCode != Errors.NONE.code) {
          (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          val coordinatorEndpoint = topicMetadata.head.partitions.asScala
            .find(_.partitionIndex == partition)
            .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
            .flatMap(metadata => metadataCache.
              getAliveBrokerNode(metadata.leaderId, request.context.listenerName))

          coordinatorEndpoint match {
            case Some(endpoint) =>
              (Errors.NONE, endpoint)
            case _ =>
              (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
      }
    }
  }

  def handleDescribeGroupRequest(request: RequestChannel.Request): Unit = {

    def sendResponseCallback(describeGroupsResponseData: DescribeGroupsResponseData): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        describeGroupsResponseData.setThrottleTimeMs(requestThrottleMs)
        new DescribeGroupsResponse(describeGroupsResponseData)
      }

      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    val describeRequest = request.body[DescribeGroupsRequest]
    val describeGroupsResponseData = new DescribeGroupsResponseData()

    describeRequest.data.groups.forEach { groupId =>
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
        describeGroupsResponseData.groups.add(DescribeGroupsResponse.forError(groupId, Errors.GROUP_AUTHORIZATION_FAILED))
      } else {
        val (error, summary) = groupCoordinator.handleDescribeGroup(groupId)
        val members = summary.members.map { member =>
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId(member.memberId)
            .setGroupInstanceId(member.groupInstanceId.orNull)
            .setClientId(member.clientId)
            .setClientHost(member.clientHost)
            .setMemberAssignment(member.assignment)
            .setMemberMetadata(member.metadata)
        }

        val describedGroup = new DescribeGroupsResponseData.DescribedGroup()
          .setErrorCode(error.code)
          .setGroupId(groupId)
          .setGroupState(summary.state)
          .setProtocolType(summary.protocolType)
          .setProtocolData(summary.protocol)
          .setMembers(members.asJava)

        if (request.header.apiVersion >= 3) {
          if (error == Errors.NONE && describeRequest.data.includeAuthorizedOperations) {
            describedGroup.setAuthorizedOperations(authHelper.authorizedOperations(request, new Resource(ResourceType.GROUP, groupId)))
          }
        }

        describeGroupsResponseData.groups.add(describedGroup)
      }
    }

    sendResponseCallback(describeGroupsResponseData)
  }

  def handleListGroupsRequest(request: RequestChannel.Request): Unit = {
    val listGroupsRequest = request.body[ListGroupsRequest]
    val states = if (listGroupsRequest.data.statesFilter == null)
    // 将null数组处理为空
      immutable.Set[String]()
    else
      listGroupsRequest.data.statesFilter.asScala.toSet

    def createResponse(throttleMs: Int, groups: List[GroupOverview], error: Errors): AbstractResponse = {
      new ListGroupsResponse(new ListGroupsResponseData()
        .setErrorCode(error.code)
        .setGroups(groups.map { group =>
          val listedGroup = new ListGroupsResponseData.ListedGroup()
            .setGroupId(group.groupId)
            .setProtocolType(group.protocolType)
            .setGroupState(group.state.toString)
          listedGroup
        }.asJava)
        .setThrottleTimeMs(throttleMs)
      )
    }

    val (error, groups) = groupCoordinator.handleListGroups(states)
    if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
    // 使用描述集群访问返回所有组。我们保留这个替代方案是为了向后兼容。
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        createResponse(requestThrottleMs, groups, error))
    else {
      val filteredGroups = groups.filter(group => authHelper.authorize(request.context, DESCRIBE, GROUP, group.groupId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        createResponse(requestThrottleMs, filteredGroups, error))
    }
  }

  def handleJoinGroupRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val joinGroupRequest = request.body[JoinGroupRequest]

    // 发送join-group响应的回调
    def sendResponseCallback(joinResult: JoinGroupResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val protocolName = if (request.context.apiVersion() >= 7)
          joinResult.protocolName.orNull
        else
          joinResult.protocolName.getOrElse(GroupCoordinator.NoProtocol)

        val responseBody = new JoinGroupResponse(
          new JoinGroupResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(joinResult.error.code)
            .setGenerationId(joinResult.generationId)
            .setProtocolType(joinResult.protocolType.orNull)
            .setProtocolName(protocolName)
            .setLeader(joinResult.leaderId)
            .setMemberId(joinResult.memberId)
            .setMembers(joinResult.members.asJava)
        )

        trace("Sending join group response %s for correlation id %d to client %s."
          .format(responseBody, request.header.correlationId, request.header.clientId))
        responseBody
      }

      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    if (joinGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // 只在IBP >= 2.3时启用静态成员关系，因为在我们确定所有代理都支持静态成员逻辑之前，代理使用静态成员逻辑是不安全的。
      // 如果静态组是由旧的协调器加载的，它将丢弃group.instance.id字段，因此静态成员可能意外地变成“动态”，从而导致错误的状态。
      sendResponseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNSUPPORTED_VERSION))
    } else if (!authHelper.authorize(request.context, READ, GROUP, joinGroupRequest.data.groupId)) {
      sendResponseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_AUTHORIZATION_FAILED))
    } else {
      val groupInstanceId = Option(joinGroupRequest.data.groupInstanceId)

      // 只有joinGroupRequest version >= 4且配置groupInstanceId为unknown时，才会返回MEMBER_ID_REQUIRED错误。
      val requireKnownMemberId = joinGroupRequest.version >= 4 && groupInstanceId.isEmpty

      // 让协调器处理join-group
      val protocols = joinGroupRequest.data.protocols.valuesList.asScala.map(protocol =>
        (protocol.name, protocol.metadata)).toList

      groupCoordinator.handleJoinGroup(
        joinGroupRequest.data.groupId,
        joinGroupRequest.data.memberId,
        groupInstanceId,
        requireKnownMemberId,
        request.header.clientId,
        request.context.clientAddress.toString,
        joinGroupRequest.data.rebalanceTimeoutMs,
        joinGroupRequest.data.sessionTimeoutMs,
        joinGroupRequest.data.protocolType,
        protocols,
        sendResponseCallback,
        requestLocal)
    }
  }

  def handleSyncGroupRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val syncGroupRequest = request.body[SyncGroupRequest]

    def sendResponseCallback(syncGroupResult: SyncGroupResult): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new SyncGroupResponse(
          new SyncGroupResponseData()
            .setErrorCode(syncGroupResult.error.code)
            .setProtocolType(syncGroupResult.protocolType.orNull)
            .setProtocolName(syncGroupResult.protocolName.orNull)
            .setAssignment(syncGroupResult.memberAssignment)
            .setThrottleTimeMs(requestThrottleMs)
        ))
    }

    if (syncGroupRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // 只在IBP >= 2.3时启用静态成员关系，因为在我们确定所有代理都支持静态成员逻辑之前，代理使用静态成员逻辑是不安全的。
      // 如果静态组是由旧的协调器加载的，它将丢弃group.instance.id字段，因此静态成员可能意外地变成“动态”，从而导致错误的状态。
      sendResponseCallback(SyncGroupResult(Errors.UNSUPPORTED_VERSION))
    } else if (!syncGroupRequest.areMandatoryProtocolTypeAndNamePresent()) {
      // 从版本5开始，ProtocolType和ProtocolName字段是必须的。
      sendResponseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
    } else if (!authHelper.authorize(request.context, READ, GROUP, syncGroupRequest.data.groupId)) {
      sendResponseCallback(SyncGroupResult(Errors.GROUP_AUTHORIZATION_FAILED))
    } else {
      val assignmentMap = immutable.Map.newBuilder[String, Array[Byte]]
      syncGroupRequest.data.assignments.forEach { assignment =>
        assignmentMap += (assignment.memberId -> assignment.assignment)
      }

      groupCoordinator.handleSyncGroup(
        syncGroupRequest.data.groupId,
        syncGroupRequest.data.generationId,
        syncGroupRequest.data.memberId,
        Option(syncGroupRequest.data.protocolType),
        Option(syncGroupRequest.data.protocolName),
        Option(syncGroupRequest.data.groupInstanceId),
        assignmentMap.result(),
        sendResponseCallback,
        requestLocal
      )
    }
  }

  def handleDeleteGroupsRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    val groups = deleteGroupsRequest.data.groupsNames.asScala.distinct

    val (authorizedGroups, unauthorizedGroups) = authHelper.partitionSeqByAuthorized(request.context, DELETE, GROUP,
      groups)(identity)

    val groupDeletionResult = groupCoordinator.handleDeleteGroups(authorizedGroups.toSet, requestLocal) ++
      unauthorizedGroups.map(_ -> Errors.GROUP_AUTHORIZATION_FAILED)

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
      val deletionCollections = new DeletableGroupResultCollection()
      groupDeletionResult.forKeyValue { (groupId, error) =>
        deletionCollections.add(new DeletableGroupResult()
          .setGroupId(groupId)
          .setErrorCode(error.code)
        )
      }

      new DeleteGroupsResponse(new DeleteGroupsResponseData()
        .setResults(deletionCollections)
        .setThrottleTimeMs(requestThrottleMs)
      )
    })
  }

  def handleHeartbeatRequest(request: RequestChannel.Request): Unit = {
    val heartbeatRequest = request.body[HeartbeatRequest]

    // 发送心跳响应的回调
    def sendResponseCallback(error: Errors): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val response = new HeartbeatResponse(
          new HeartbeatResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(error.code))
        trace("Sending heartbeat response %s for correlation id %d to client %s."
          .format(response, request.header.correlationId, request.header.clientId))
        response
      }

      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    if (heartbeatRequest.data.groupInstanceId != null && config.interBrokerProtocolVersion < KAFKA_2_3_IV0) {
      // 只在IBP >= 2.3时启用静态成员关系，因为在我们确定所有代理都支持静态成员逻辑之前，代理使用静态成员逻辑是不安全的。
      // 如果静态组是由旧的协调器加载的，它将丢弃group.instance.id字段，因此静态成员可能意外地变成“动态”，从而导致错误的状态。
      sendResponseCallback(Errors.UNSUPPORTED_VERSION)
    } else if (!authHelper.authorize(request.context, READ, GROUP, heartbeatRequest.data.groupId)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new HeartbeatResponse(
          new HeartbeatResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)))
    } else {
      // let the coordinator to handle heartbeat
      groupCoordinator.handleHeartbeat(
        heartbeatRequest.data.groupId,
        heartbeatRequest.data.memberId,
        Option(heartbeatRequest.data.groupInstanceId),
        heartbeatRequest.data.generationId,
        sendResponseCallback)
    }
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request): Unit = {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    val members = leaveGroupRequest.members.asScala.toList

    if (!authHelper.authorize(request.context, READ, GROUP, leaveGroupRequest.data.groupId)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        new LeaveGroupResponse(new LeaveGroupResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
        )
      })
    } else {
      def sendResponseCallback(leaveGroupResult: LeaveGroupResult): Unit = {
        val memberResponses = leaveGroupResult.memberResponses.map(
          leaveGroupResult =>
            new MemberResponse()
              .setErrorCode(leaveGroupResult.error.code)
              .setMemberId(leaveGroupResult.memberId)
              .setGroupInstanceId(leaveGroupResult.groupInstanceId.orNull)
        )

        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          new LeaveGroupResponse(
            memberResponses.asJava,
            leaveGroupResult.topLevelError,
            requestThrottleMs,
            leaveGroupRequest.version)
        }

        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      groupCoordinator.handleLeaveGroup(
        leaveGroupRequest.data.groupId,
        members,
        sendResponseCallback)
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
  }

  def handleApiVersionsRequest(request: RequestChannel.Request): Unit = {
    // 请注意，无论当前的身份验证状态如何，代理都会返回其支持的apikey和版本的完整列表(例如，
    // 在SASL侦听器上进行SASL身份验证之前，请注意在SSL握手完成之前，SSL侦听器上不可能发生Kafka协议请求)。
    // 如果认为这会泄漏有关代理版本的信息，则解决方法是使用SSL和客户端身份验证，这是在连接的早期阶段执行的，
    // 其中ApiVersionRequest不可用。

    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      } else {
        apiVersionManager.apiVersionResponse(requestThrottleMs)
      }
    }

    requestHelper.sendResponseMaybeThrottle(request, createResponseCallback)
  }

  def handleCreateTopicsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 6)

    def sendResponseCallback(results: CreatableTopicResultCollection): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseData = new CreateTopicsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(results)
        val responseBody = new CreateTopicsResponse(responseData)
        trace(s"Sending create topics response $responseData for correlation id " +
          s"${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }

      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, createResponse)
    }

    val createTopicsRequest = request.body[CreateTopicsRequest]
    val results = new CreatableTopicResultCollection(createTopicsRequest.data.topics.size)
    if (!zkSupport.controller.isActive) {
      createTopicsRequest.data.topics.forEach { topic =>
        results.add(new CreatableTopicResult().setName(topic.name)
          .setErrorCode(Errors.NOT_CONTROLLER.code))
      }
      sendResponseCallback(results)
    } else {
      createTopicsRequest.data.topics.forEach { topic =>
        results.add(new CreatableTopicResult().setName(topic.name))
      }
      val hasClusterAuthorization = authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME,
        logIfDenied = false)
      val topics = createTopicsRequest.data.topics.asScala.map(_.name)
      val authorizedTopics =
        if (hasClusterAuthorization) topics.toSet
        else authHelper.filterByAuthorized(request.context, CREATE, TOPIC, topics)(identity)
      val authorizedForDescribeConfigs = authHelper.filterByAuthorized(request.context, DESCRIBE_CONFIGS, TOPIC,
        topics, logIfDenied = false)(identity).map(name => name -> results.find(name)).toMap

      results.forEach { topic =>
        if (results.findAll(topic.name).size > 1) {
          topic.setErrorCode(Errors.INVALID_REQUEST.code)
          topic.setErrorMessage("Found multiple entries for this topic.")
        } else if (!authorizedTopics.contains(topic.name)) {
          topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          topic.setErrorMessage("Authorization failed.")
        }
        if (!authorizedForDescribeConfigs.contains(topic.name)) {
          topic.setTopicConfigErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        }
      }
      val toCreate = mutable.Map[String, CreatableTopic]()
      createTopicsRequest.data.topics.forEach { topic =>
        if (results.find(topic.name).errorCode == Errors.NONE.code) {
          toCreate += topic.name -> topic
        }
      }

      def handleCreateTopicsResults(errors: Map[String, ApiError]): Unit = {
        errors.foreach { case (topicName, error) =>
          val result = results.find(topicName)
          result.setErrorCode(error.error.code)
            .setErrorMessage(error.message)
          //如果Create失败，则重置响应中的任何配置
          if (error != ApiError.NONE) {
            result.setConfigs(List.empty.asJava)
              .setNumPartitions(-1)
              .setReplicationFactor(-1)
              .setTopicConfigErrorCode(Errors.NONE.code)
          }
        }
        sendResponseCallback(results)
      }

      zkSupport.adminManager.createTopics(
        createTopicsRequest.data.timeoutMs,
        createTopicsRequest.data.validateOnly,
        toCreate,
        authorizedForDescribeConfigs,
        controllerMutationQuota,
        handleCreateTopicsResults)
    }
  }

  def handleCreatePartitionsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val createPartitionsRequest = request.body[CreatePartitionsRequest]
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 3)

    def sendResponseCallback(results: Map[String, ApiError]): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val createPartitionsResults = results.map {
          case (topic, error) => new CreatePartitionsTopicResult()
            .setName(topic)
            .setErrorCode(error.error.code)
            .setErrorMessage(error.message)
        }.toSeq
        val responseBody = new CreatePartitionsResponse(new CreatePartitionsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setResults(createPartitionsResults.asJava))
        trace(s"Sending create partitions response $responseBody for correlation id ${request.header.correlationId} to " +
          s"client ${request.header.clientId}.")
        responseBody
      }

      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, createResponse)
    }

    if (!zkSupport.controller.isActive) {
      val result = createPartitionsRequest.data.topics.asScala.map { topic =>
        (topic.name, new ApiError(Errors.NOT_CONTROLLER, null))
      }.toMap
      sendResponseCallback(result)
    } else {
      // 向响应添加重复主题的特殊处理
      val topics = createPartitionsRequest.data.topics.asScala.toSeq
      val dupes = topics.groupBy(_.name)
        .filter {
          _._2.size > 1
        }
        .keySet
      val notDuped = topics.filterNot(topic => dupes.contains(topic.name))
      val (authorized, unauthorized) = authHelper.partitionSeqByAuthorized(request.context, ALTER, TOPIC,
        notDuped)(_.name)

      val (queuedForDeletion, valid) = authorized.partition { topic =>
        zkSupport.controller.topicDeletionManager.isTopicQueuedUpForDeletion(topic.name)
      }

      val errors = dupes.map(_ -> new ApiError(Errors.INVALID_REQUEST, "Duplicate topic in request.")) ++
        unauthorized.map(_.name -> new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED, "The topic authorization is failed.")) ++
        queuedForDeletion.map(_.name -> new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is queued for deletion."))

      zkSupport.adminManager.createPartitions(
        createPartitionsRequest.data.timeoutMs,
        valid,
        createPartitionsRequest.data.validateOnly,
        controllerMutationQuota,
        result => sendResponseCallback(result ++ errors))
    }
  }

  def handleDeleteTopicsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val controllerMutationQuota = quotas.controllerMutation.newQuotaFor(request, strictSinceVersion = 5)

    def sendResponseCallback(results: DeletableTopicResultCollection): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseData = new DeleteTopicsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setResponses(results)
        val responseBody = new DeleteTopicsResponse(responseData)
        trace(s"Sending delete topics response $responseBody for correlation id ${request.header.correlationId} to client ${request.header.clientId}.")
        responseBody
      }

      requestHelper.sendResponseMaybeThrottleWithControllerQuota(controllerMutationQuota, request, createResponse)
    }

    val deleteTopicRequest = request.body[DeleteTopicsRequest]
    val results = new DeletableTopicResultCollection(deleteTopicRequest.numberOfTopics())
    val toDelete = mutable.Set[String]()
    if (!zkSupport.controller.isActive) {
      deleteTopicRequest.topics().forEach { topic =>
        results.add(new DeletableTopicResult()
          .setName(topic.name())
          .setTopicId(topic.topicId())
          .setErrorCode(Errors.NOT_CONTROLLER.code))
      }
      sendResponseCallback(results)
    } else if (!config.deleteTopicEnable) {
      val error = if (request.context.apiVersion < 3) Errors.INVALID_REQUEST else Errors.TOPIC_DELETION_DISABLED
      deleteTopicRequest.topics().forEach { topic =>
        results.add(new DeletableTopicResult()
          .setName(topic.name())
          .setTopicId(topic.topicId())
          .setErrorCode(error.code))
      }
      sendResponseCallback(results)
    } else {
      val topicIdsFromRequest = deleteTopicRequest.topicIds().asScala.filter(topicId => topicId != Uuid.ZERO_UUID).toSet
      deleteTopicRequest.topics().forEach { topic =>
        if (topic.name() != null && topic.topicId() != Uuid.ZERO_UUID)
          throw new InvalidRequestException("Topic name and topic ID can not both be specified.")
        val name = if (topic.topicId() == Uuid.ZERO_UUID) topic.name()
        else zkSupport.controller.controllerContext.topicName(topic.topicId).orNull
        results.add(new DeletableTopicResult()
          .setName(name)
          .setTopicId(topic.topicId()))
      }
      val authorizedDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
        results.asScala.filter(result => result.name() != null))(_.name)
      val authorizedDeleteTopics = authHelper.filterByAuthorized(request.context, DELETE, TOPIC,
        results.asScala.filter(result => result.name() != null))(_.name)
      results.forEach { topic =>
        val unresolvedTopicId = topic.topicId() != Uuid.ZERO_UUID && topic.name() == null
        if (unresolvedTopicId) {
          topic.setErrorCode(Errors.UNKNOWN_TOPIC_ID.code)
        } else if (topicIdsFromRequest.contains(topic.topicId) && !authorizedDescribeTopics.contains(topic.name)) {

          // 因为客户端没有描述权限，所以不应该在响应中返回名称。但是请注意，我们并不认为topicId本身是敏感的，
          // 因此没有理由用' UNKNOWN_TOPIC_ID '来掩盖这种情况。
          topic.setName(null)
          topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        } else if (!authorizedDeleteTopics.contains(topic.name)) {
          topic.setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
        } else if (!metadataCache.contains(topic.name)) {
          topic.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        } else {
          toDelete += topic.name
        }
      }
      // 如果没有授权的主题立即返回
      if (toDelete.isEmpty)
        sendResponseCallback(results)
      else {
        def handleDeleteTopicsResults(errors: Map[String, Errors]): Unit = {
          errors.foreach {
            case (topicName, error) =>
              results.find(topicName)
                .setErrorCode(error.code)
          }
          sendResponseCallback(results)
        }

        zkSupport.adminManager.deleteTopics(
          deleteTopicRequest.data.timeoutMs,
          toDelete,
          controllerMutationQuota,
          handleDeleteTopicsResults
        )
      }
    }
  }

  def handleDeleteRecordsRequest(request: RequestChannel.Request): Unit = {
    val deleteRecordsRequest = request.body[DeleteRecordsRequest]

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val authorizedForDeleteTopicOffsets = mutable.Map[TopicPartition, Long]()

    val topics = deleteRecordsRequest.data.topics.asScala
    val authorizedTopics = authHelper.filterByAuthorized(request.context, DELETE, TOPIC, topics)(_.name)
    val deleteTopicPartitions = topics.flatMap { deleteTopic =>
      deleteTopic.partitions.asScala.map { deletePartition =>
        new TopicPartition(deleteTopic.name, deletePartition.partitionIndex) -> deletePartition.offset
      }
    }
    for ((topicPartition, offset) <- deleteTopicPartitions) {
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
      else
        authorizedForDeleteTopicOffsets += (topicPartition -> offset)
    }

    // 发送DeleteRecordsResponse的回调
    def sendResponseCallback(authorizedTopicResponses: Map[TopicPartition, DeleteRecordsPartitionResult]): Unit = {
      val mergedResponseStatus = authorizedTopicResponses ++ unauthorizedTopicResponses ++ nonExistingTopicResponses
      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.errorCode != Errors.NONE.code) {
          debug("DeleteRecordsRequest with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            Errors.forCode(status.errorCode).exceptionName))
        }
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DeleteRecordsResponse(new DeleteRecordsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(mergedResponseStatus.groupBy(_._1.topic).map { case (topic, partitionMap) => {
            new DeleteRecordsTopicResult()
              .setName(topic)
              .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(partitionMap.map { case (topicPartition, partitionResult) => {
                new DeleteRecordsPartitionResult().setPartitionIndex(topicPartition.partition)
                  .setLowWatermark(partitionResult.lowWatermark)
                  .setErrorCode(partitionResult.errorCode)
              }
              }.toList.asJava.iterator()))
          }
          }.toList.asJava.iterator()))))
    }

    if (authorizedForDeleteTopicOffsets.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // 调用副本管理器将消息附加到副本
      replicaManager.deleteRecords(
        deleteRecordsRequest.data.timeoutMs.toLong,
        authorizedForDeleteTopicOffsets,
        sendResponseCallback)
    }
  }

  def handleInitProducerIdRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val transactionalId = initProducerIdRequest.data.transactionalId

    if (transactionalId != null) {
      if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    } else if (!authHelper.authorize(request.context, IDEMPOTENT_WRITE, CLUSTER, CLUSTER_NAME, true, false)
      && !authHelper.authorizeByResourceType(request.context, AclOperation.WRITE, ResourceType.TOPIC)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    def sendResponseCallback(result: InitProducerIdResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val finalError =
          if (initProducerIdRequest.version < 4 && result.error == Errors.PRODUCER_FENCED) {
            // 对于较老的客户机，它们无法理解新的PRODUCER_FENCED错误代码，
            // 因此我们需要返回INVALID_PRODUCER_EPOCH以具有相同的客户机处理逻辑。
            Errors.INVALID_PRODUCER_EPOCH
          } else {
            result.error
          }
        val responseData = new InitProducerIdResponseData()
          .setProducerId(result.producerId)
          .setProducerEpoch(result.producerEpoch)
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(finalError.code)
        val responseBody = new InitProducerIdResponse(responseData)
        trace(s"Completed $transactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
        responseBody
      }

      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    val producerIdAndEpoch = (initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch) match {
      case (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH) => Right(None)
      case (RecordBatch.NO_PRODUCER_ID, _) | (_, RecordBatch.NO_PRODUCER_EPOCH) => Left(Errors.INVALID_REQUEST)
      case (_, _) => Right(Some(new ProducerIdAndEpoch(initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch)))
    }

    producerIdAndEpoch match {
      case Right(producerIdAndEpoch) => txnCoordinator.handleInitProducerId(transactionalId, initProducerIdRequest.data.transactionTimeoutMs,
        producerIdAndEpoch, sendResponseCallback, requestLocal)
      case Left(error) => requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  def handleEndTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val endTxnRequest = request.body[EndTxnRequest]
    val transactionalId = endTxnRequest.data.transactionalId

    if (authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
      def sendResponseCallback(error: Errors): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (endTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // 对于较老的客户机，它们无法理解新的PRODUCER_FENCED错误代码，因此我们需要返回INVALID_PRODUCER_EPOCH以
              // 具有相同的客户机处理逻辑。
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }
          val responseBody = new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(finalError.code)
            .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed ${endTxnRequest.data.transactionalId}'s EndTxnRequest " +
            s"with committed: ${endTxnRequest.data.committed}, " +
            s"errors: $error from client ${request.header.clientId}.")
          responseBody
        }

        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleEndTransaction(endTxnRequest.data.transactionalId,
        endTxnRequest.data.producerId,
        endTxnRequest.data.producerEpoch,
        endTxnRequest.result(),
        sendResponseCallback,
        requestLocal)
    } else
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(new EndTxnResponseData()
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs))
      )
  }

  def handleWriteTxnMarkersRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val writeTxnMarkersRequest = request.body[WriteTxnMarkersRequest]
    val errors = new ConcurrentHashMap[java.lang.Long, util.Map[TopicPartition, Errors]]()
    val markers = writeTxnMarkersRequest.markers
    val numAppends = new AtomicInteger(markers.size)

    if (numAppends.get == 0) {
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
      return
    }

    def updateErrors(producerId: Long, currentErrors: ConcurrentHashMap[TopicPartition, Errors]): Unit = {
      val previousErrors = errors.putIfAbsent(producerId, currentErrors)
      if (previousErrors != null)
        previousErrors.putAll(currentErrors)
    }

    /**
     * 这是在事务标记的日志追加成功时调用的回调。在处理单个WriteTxnMarkersRequest时可以多次调用该方法，
     * 因为请求中的每个TransactionMarker都有一个追加，因此可能会有多个追加到日志中的标记。只有在所有附件返回后才会发送最终响应。
     */
    def maybeSendResponseCallback(producerId: Long, result: TransactionResult)(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      trace(s"End transaction marker append for producer id $producerId completed with status: $responseStatus")
      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors](responseStatus.map { case (k, v) => k -> v.error }.asJava)
      updateErrors(producerId, currentErrors)
      val successfulOffsetsPartitions = responseStatus.filter { case (topicPartition, partitionResponse) =>
        topicPartition.topic == GROUP_METADATA_TOPIC_NAME && partitionResponse.error == Errors.NONE
      }.keys

      if (successfulOffsetsPartitions.nonEmpty) {
        // 一旦为事务性偏移量提交写入结束事务标记，就调用组协调器将偏移量具体化到缓存中
        try {
          groupCoordinator.scheduleHandleTxnCompletion(producerId, successfulOffsetsPartitions, result)
        } catch {
          case e: Exception =>
            error(s"Received an exception while trying to update the offsets cache on transaction marker append", e)
            val updatedErrors = new ConcurrentHashMap[TopicPartition, Errors]()
            successfulOffsetsPartitions.foreach(updatedErrors.put(_, Errors.UNKNOWN_SERVER_ERROR))
            updateErrors(producerId, updatedErrors)
        }
      }

      if (numAppends.decrementAndGet() == 0)
        requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
    }

    // TODO: 当前的追加API使得对每个producerId进行单独的写操作变得简单一些，但是最好只对日志进行一次追加。
    //  这需要将控件记录的构建推入Log，以便我们只附加那些具有有效生产者纪元的记录，
    //  并在ReplicaManager中公开一个新的appendControlRecord API。现在，我们采用了更简单的方法
    var skippedMarkers = 0
    for (marker <- markers.asScala) {
      val producerId = marker.producerId
      val partitionsWithCompatibleMessageFormat = new mutable.ArrayBuffer[TopicPartition]

      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors]()
      marker.partitions.forEach { partition =>
        replicaManager.getMagic(partition) match {
          case Some(magic) =>
            if (magic < RecordBatch.MAGIC_VALUE_V2)
              currentErrors.put(partition, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT)
            else
              partitionsWithCompatibleMessageFormat += partition
          case None =>
            currentErrors.put(partition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
      }

      if (!currentErrors.isEmpty)
        updateErrors(producerId, currentErrors)

      if (partitionsWithCompatibleMessageFormat.isEmpty) {
        numAppends.decrementAndGet()
        skippedMarkers += 1
      } else {
        val controlRecords = partitionsWithCompatibleMessageFormat.map { partition =>
          val controlRecordType = marker.transactionResult match {
            case TransactionResult.COMMIT => ControlRecordType.COMMIT
            case TransactionResult.ABORT => ControlRecordType.ABORT
          }
          val endTxnMarker = new EndTransactionMarker(controlRecordType, marker.coordinatorEpoch)
          partition -> MemoryRecords.withEndTransactionMarker(producerId, marker.producerEpoch, endTxnMarker)
        }.toMap

        replicaManager.appendRecords(
          timeout = config.requestTimeoutMs.toLong,
          requiredAcks = -1,
          internalTopicsAllowed = true,
          origin = AppendOrigin.Coordinator,
          entriesPerPartition = controlRecords,
          requestLocal = requestLocal,
          responseCallback = maybeSendResponseCallback(producerId, marker.transactionResult))
      }
    }

    // 由于所有分区都有不正确的日志格式，因此没有写入日志追加部分，因此我们需要发送错误响应
    if (skippedMarkers == markers.size)
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
  }

  def ensureInterBrokerVersion(version: ApiVersion): Unit = {
    if (config.interBrokerProtocolVersion < version)
      throw new UnsupportedVersionException(s"inter.broker.protocol.version: ${config.interBrokerProtocolVersion.version} is less than the required version: ${version.version}")
  }

  def handleAddPartitionToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val addPartitionsToTxnRequest = request.body[AddPartitionsToTxnRequest]
    val transactionalId = addPartitionsToTxnRequest.data.transactionalId
    val partitionsToAdd = addPartitionsToTxnRequest.partitions.asScala
    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        addPartitionsToTxnRequest.getErrorResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception))
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedPartitions = mutable.Set[TopicPartition]()

      val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC,
        partitionsToAdd.filterNot(tp => Topic.isInternal(tp.topic)))(_.topic)
      for (topicPartition <- partitionsToAdd) {
        if (!authorizedTopics.contains(topicPartition.topic))
          unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
        else if (!metadataCache.contains(topicPartition))
          nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
        else
          authorizedPartitions.add(topicPartition)
      }

      if (unauthorizedTopicErrors.nonEmpty || nonExistingTopicErrors.nonEmpty) {
        // 任何分区检查失败都会导致整个请求失败。对于失败的分区，我们发送适当的错误代码，对于授权检查成功的分区，
        // 我们发送一个“operation_not_attempts”错误代码，以表明它们没有被添加到事务中。
        val partitionErrors = unauthorizedTopicErrors ++ nonExistingTopicErrors ++
          authorizedPartitions.map(_ -> Errors.OPERATION_NOT_ATTEMPTED)
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new AddPartitionsToTxnResponse(requestThrottleMs, partitionErrors.asJava))
      } else {
        def sendResponseCallback(error: Errors): Unit = {
          def createResponse(requestThrottleMs: Int): AbstractResponse = {
            val finalError =
              if (addPartitionsToTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
                // 对于较旧的客户机，它们无法理解新的PRODUCER_FENCED错误代码，
                // 因此我们需要返回旧的INVALID_PRODUCER_EPOCH以具有相同的客户机处理逻辑。
                Errors.INVALID_PRODUCER_EPOCH
              } else {
                error
              }

            val responseBody: AddPartitionsToTxnResponse = new AddPartitionsToTxnResponse(requestThrottleMs,
              partitionsToAdd.map { tp => (tp, finalError) }.toMap.asJava)
            trace(s"Completed $transactionalId's AddPartitionsToTxnRequest with partitions $partitionsToAdd: errors: $error from client ${request.header.clientId}")
            responseBody
          }

          requestHelper.sendResponseMaybeThrottle(request, createResponse)
        }

        txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
          addPartitionsToTxnRequest.data.producerId,
          addPartitionsToTxnRequest.data.producerEpoch,
          authorizedPartitions,
          sendResponseCallback,
          requestLocal)
      }
    }
  }

  def handleAddOffsetsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val addOffsetsToTxnRequest = request.body[AddOffsetsToTxnRequest]
    val transactionalId = addOffsetsToTxnRequest.data.transactionalId
    val groupId = addOffsetsToTxnRequest.data.groupId
    val offsetTopicPartition = new TopicPartition(GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId))

    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs)))
    else if (!authHelper.authorize(request.context, READ, GROUP, groupId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs))
      )
    else {
      def sendResponseCallback(error: Errors): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (addOffsetsToTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // For older clients, they could not understand the new PRODUCER_FENCED error code,
              // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }

          val responseBody: AddOffsetsToTxnResponse = new AddOffsetsToTxnResponse(
            new AddOffsetsToTxnResponseData()
              .setErrorCode(finalError.code)
              .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed $transactionalId's AddOffsetsToTxnRequest for group $groupId on partition " +
            s"$offsetTopicPartition: errors: $error from client ${request.header.clientId}")
          responseBody
        }

        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
        addOffsetsToTxnRequest.data.producerId,
        addOffsetsToTxnRequest.data.producerEpoch,
        Set(offsetTopicPartition),
        sendResponseCallback,
        requestLocal)
    }
  }

  def handleTxnOffsetCommitRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val header = request.header
    val txnOffsetCommitRequest = request.body[TxnOffsetCommitRequest]

    // authorize for the transactionalId and the consumer group. Note that we skip producerId authorization
    // since it is implied by transactionalId authorization
    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, txnOffsetCommitRequest.data.transactionalId))
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
    else if (!authHelper.authorize(request.context, READ, GROUP, txnOffsetCommitRequest.data.groupId))
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.GROUP_AUTHORIZATION_FAILED.exception)
    else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedTopicCommittedOffsets = mutable.Map[TopicPartition, TxnOffsetCommitRequest.CommittedOffset]()
      val committedOffsets = txnOffsetCommitRequest.offsets.asScala
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, committedOffsets)(_._1.topic)

      for ((topicPartition, commitedOffset) <- committedOffsets) {
        if (!authorizedTopics.contains(topicPartition.topic))
          unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
        else if (!metadataCache.contains(topicPartition))
          nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
        else
          authorizedTopicCommittedOffsets += (topicPartition -> commitedOffset)
      }

      // the callback for sending an offset commit response
      def sendResponseCallback(authorizedTopicErrors: Map[TopicPartition, Errors]): Unit = {
        val combinedCommitStatus = mutable.Map() ++= authorizedTopicErrors ++= unauthorizedTopicErrors ++= nonExistingTopicErrors
        if (isDebugEnabled)
          combinedCommitStatus.forKeyValue { (topicPartition, error) =>
            if (error != Errors.NONE) {
              debug(s"TxnOffsetCommit with correlation id ${header.correlationId} from client ${header.clientId} " +
                s"on partition $topicPartition failed due to ${error.exceptionName}")
            }
          }

        // We need to replace COORDINATOR_LOAD_IN_PROGRESS with COORDINATOR_NOT_AVAILABLE
        // for older producer client from 0.11 to prior 2.0, which could potentially crash due
        // to unexpected loading error. This bug is fixed later by KAFKA-7296. Clients using
        // txn commit protocol >= 2 (version 2.3 and onwards) are guaranteed to have
        // the fix to check for the loading error.
        if (txnOffsetCommitRequest.version < 2) {
          combinedCommitStatus ++= combinedCommitStatus.collect {
            case (tp, error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS => tp -> Errors.COORDINATOR_NOT_AVAILABLE
          }
        }

        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          new TxnOffsetCommitResponse(requestThrottleMs, combinedCommitStatus.asJava))
      }

      if (authorizedTopicCommittedOffsets.isEmpty)
        sendResponseCallback(Map.empty)
      else {
        val offsetMetadata = convertTxnOffsets(authorizedTopicCommittedOffsets.toMap)
        groupCoordinator.handleTxnCommitOffsets(
          txnOffsetCommitRequest.data.groupId,
          txnOffsetCommitRequest.data.producerId,
          txnOffsetCommitRequest.data.producerEpoch,
          txnOffsetCommitRequest.data.memberId,
          Option(txnOffsetCommitRequest.data.groupInstanceId),
          txnOffsetCommitRequest.data.generationId,
          offsetMetadata,
          sendResponseCallback,
          requestLocal)
      }
    }
  }

  private def convertTxnOffsets(offsetsMap: immutable.Map[TopicPartition, TxnOffsetCommitRequest.CommittedOffset]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    val currentTimestamp = time.milliseconds
    offsetsMap.map { case (topicPartition, partitionData) =>
      val metadata = if (partitionData.metadata == null) OffsetAndMetadata.NoMetadata else partitionData.metadata
      topicPartition -> new OffsetAndMetadata(
        offset = partitionData.offset,
        leaderEpoch = partitionData.leaderEpoch,
        metadata = metadata,
        commitTimestamp = currentTimestamp,
        expireTimestamp = None)
    }
  }

  def handleDescribeAcls(request: RequestChannel.Request): Unit = {
    aclApis.handleDescribeAcls(request)
  }

  def handleCreateAcls(request: RequestChannel.Request): Unit = {
    metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    aclApis.handleCreateAcls(request)
  }

  def handleDeleteAcls(request: RequestChannel.Request): Unit = {
    metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    aclApis.handleDeleteAcls(request)
  }

  def handleOffsetForLeaderEpochRequest(request: RequestChannel.Request): Unit = {
    val offsetForLeaderEpoch = request.body[OffsetsForLeaderEpochRequest]
    val topics = offsetForLeaderEpoch.data.topics.asScala.toSeq

    // OffsetsForLeaderEpoch API最初仅用于代理间通信和需要集群权限。对于KIP-320，
    // 消费者现在也使用该API来检查在leader更改后的日志截断，因此我们还允许主题描述权限。
    val (authorizedTopics, unauthorizedTopics) =
    if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME, logIfDenied = false))
      (topics, Seq.empty[OffsetForLeaderTopic])
    else authHelper.partitionSeqByAuthorized(request.context, DESCRIBE, TOPIC, topics)(_.topic)

    val endOffsetsForAuthorizedPartitions = replicaManager.lastOffsetForLeaderEpoch(authorizedTopics)
    val endOffsetsForUnauthorizedPartitions = unauthorizedTopics.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        new EpochEndOffset()
          .setPartition(offsetForLeaderPartition.partition)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      }

      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }

    val endOffsetsForAllTopics = new OffsetForLeaderTopicResultCollection(
      (endOffsetsForAuthorizedPartitions ++ endOffsetsForUnauthorizedPartitions).asJava.iterator
    )

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new OffsetsForLeaderEpochResponse(new OffsetForLeaderEpochResponseData()
        .setThrottleTimeMs(requestThrottleMs)
        .setTopics(endOffsetsForAllTopics)))
  }

  def handleAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val alterConfigsRequest = request.body[AlterConfigsRequest]
    val (authorizedResources, unauthorizedResources) = alterConfigsRequest.configs.asScala.toMap.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER_LOGGER =>
          throw new InvalidRequestException(s"AlterConfigs is deprecated and does not support the resource type ${ConfigResource.Type.BROKER_LOGGER}")
        case ConfigResource.Type.BROKER =>
          authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(request.context, ALTER_CONFIGS, TOPIC, resource.name)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }
    val authorizedResult = zkSupport.adminManager.alterConfigs(authorizedResources, alterConfigsRequest.validateOnly)
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(resource)
    }

    def responseCallback(requestThrottleMs: Int): AlterConfigsResponse = {
      val data = new AlterConfigsResponseData()
        .setThrottleTimeMs(requestThrottleMs)
      (authorizedResult ++ unauthorizedResult).foreach { case (resource, error) =>
        data.responses().add(new AlterConfigsResourceResponse()
          .setErrorCode(error.error.code)
          .setErrorMessage(error.message)
          .setResourceName(resource.name)
          .setResourceType(resource.`type`.id))
      }
      new AlterConfigsResponse(data)
    }

    requestHelper.sendResponseMaybeThrottle(request, responseCallback)
  }

  def handleAlterPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    authHelper.authorizeClusterOperation(request, ALTER)
    val alterPartitionReassignmentsRequest = request.body[AlterPartitionReassignmentsRequest]

    def sendResponseCallback(result: Either[Map[TopicPartition, ApiError], ApiError]): Unit = {
      val responseData = result match {
        case Right(topLevelError) =>
          new AlterPartitionReassignmentsResponseData().setErrorMessage(topLevelError.message).setErrorCode(topLevelError.error.code)

        case Left(assignments) =>
          val topicResponses = assignments.groupBy(_._1.topic).map {
            case (topic, reassignmentsByTp) =>
              val partitionResponses = reassignmentsByTp.map {
                case (topicPartition, error) =>
                  new ReassignablePartitionResponse().setPartitionIndex(topicPartition.partition)
                    .setErrorCode(error.error.code).setErrorMessage(error.message)
              }
              new ReassignableTopicResponse().setName(topic).setPartitions(partitionResponses.toList.asJava)
          }
          new AlterPartitionReassignmentsResponseData().setResponses(topicResponses.toList.asJava)
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterPartitionReassignmentsResponse(responseData.setThrottleTimeMs(requestThrottleMs))
      )
    }

    val reassignments = alterPartitionReassignmentsRequest.data.topics.asScala.flatMap {
      reassignableTopic =>
        reassignableTopic.partitions.asScala.map {
          reassignablePartition =>
            val tp = new TopicPartition(reassignableTopic.name, reassignablePartition.partitionIndex)
            if (reassignablePartition.replicas == null)
              tp -> None // revert call
            else
              tp -> Some(reassignablePartition.replicas.asScala.map(_.toInt))
        }
    }.toMap

    zkSupport.controller.alterPartitionReassignments(reassignments, sendResponseCallback)
  }

  def handleListPartitionReassignmentsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.notYetSupported(request))
    authHelper.authorizeClusterOperation(request, DESCRIBE)
    val listPartitionReassignmentsRequest = request.body[ListPartitionReassignmentsRequest]

    def sendResponseCallback(result: Either[Map[TopicPartition, ReplicaAssignment], ApiError]): Unit = {
      val responseData = result match {
        case Right(error) => new ListPartitionReassignmentsResponseData().setErrorMessage(error.message).setErrorCode(error.error.code)

        case Left(assignments) =>
          val topicReassignments = assignments.groupBy(_._1.topic).map {
            case (topic, reassignmentsByTp) =>
              val partitionReassignments = reassignmentsByTp.map {
                case (topicPartition, assignment) =>
                  new ListPartitionReassignmentsResponseData.OngoingPartitionReassignment()
                    .setPartitionIndex(topicPartition.partition)
                    .setAddingReplicas(assignment.addingReplicas.toList.asJava.asInstanceOf[java.util.List[java.lang.Integer]])
                    .setRemovingReplicas(assignment.removingReplicas.toList.asJava.asInstanceOf[java.util.List[java.lang.Integer]])
                    .setReplicas(assignment.replicas.toList.asJava.asInstanceOf[java.util.List[java.lang.Integer]])
              }.toList

              new ListPartitionReassignmentsResponseData.OngoingTopicReassignment().setName(topic)
                .setPartitions(partitionReassignments.asJava)
          }.toList

          new ListPartitionReassignmentsResponseData().setTopics(topicReassignments.asJava)
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListPartitionReassignmentsResponse(responseData.setThrottleTimeMs(requestThrottleMs))
      )
    }

    val partitionsOpt = listPartitionReassignmentsRequest.data.topics match {
      case topics: Any =>
        Some(topics.iterator().asScala.flatMap { topic =>
          topic.partitionIndexes.iterator().asScala
            .map { tp => new TopicPartition(topic.name(), tp) }
        }.toSet)
      case _ => None
    }

    zkSupport.controller.listPartitionReassignments(partitionsOpt, sendResponseCallback)
  }

  private def configsAuthorizationApiError(resource: ConfigResource): ApiError = {
    val error = resource.`type` match {
      case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER => Errors.CLUSTER_AUTHORIZATION_FAILED
      case ConfigResource.Type.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
      case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.name}")
    }
    new ApiError(error, null)
  }

  def handleIncrementalAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val alterConfigsRequest = request.body[IncrementalAlterConfigsRequest]

    val configs = alterConfigsRequest.data.resources.iterator.asScala.map { alterConfigResource =>
      val configResource = new ConfigResource(ConfigResource.Type.forId(alterConfigResource.resourceType),
        alterConfigResource.resourceName)
      configResource -> alterConfigResource.configs.iterator.asScala.map {
        alterConfig =>
          new AlterConfigOp(new ConfigEntry(alterConfig.name, alterConfig.value),
            OpType.forId(alterConfig.configOperation))
      }.toBuffer
    }.toMap

    val (authorizedResources, unauthorizedResources) = configs.partition { case (resource, _) =>
      resource.`type` match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER =>
          authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(request.context, ALTER_CONFIGS, TOPIC, resource.name)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt")
      }
    }

    val authorizedResult = zkSupport.adminManager.incrementalAlterConfigs(authorizedResources, alterConfigsRequest.data.validateOnly)
    val unauthorizedResult = unauthorizedResources.keys.map { resource =>
      resource -> configsAuthorizationApiError(resource)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => new IncrementalAlterConfigsResponse(
      requestThrottleMs, (authorizedResult ++ unauthorizedResult).asJava))
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val describeConfigsRequest = request.body[DescribeConfigsRequest]
    val (authorizedResources, unauthorizedResources) = describeConfigsRequest.data.resources.asScala.partition { resource =>
      ConfigResource.Type.forId(resource.resourceType) match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER =>
          authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)
        case ConfigResource.Type.TOPIC =>
          authHelper.authorize(request.context, DESCRIBE_CONFIGS, TOPIC, resource.resourceName)
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.resourceName}")
      }
    }
    val authorizedConfigs = configHelper.describeConfigs(authorizedResources.toList, describeConfigsRequest.data.includeSynonyms, describeConfigsRequest.data.includeDocumentation)
    val unauthorizedConfigs = unauthorizedResources.map { resource =>
      val error = ConfigResource.Type.forId(resource.resourceType) match {
        case ConfigResource.Type.BROKER | ConfigResource.Type.BROKER_LOGGER => Errors.CLUSTER_AUTHORIZATION_FAILED
        case ConfigResource.Type.TOPIC => Errors.TOPIC_AUTHORIZATION_FAILED
        case rt => throw new InvalidRequestException(s"Unexpected resource type $rt for resource ${resource.resourceName}")
      }
      new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(error.code)
        .setErrorMessage(error.message)
        .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
        .setResourceName(resource.resourceName)
        .setResourceType(resource.resourceType)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(new DescribeConfigsResponseData().setThrottleTimeMs(requestThrottleMs)
        .setResults((authorizedConfigs ++ unauthorizedConfigs).asJava)))
  }

  def handleAlterReplicaLogDirsRequest(request: RequestChannel.Request): Unit = {
    val alterReplicaDirsRequest = request.body[AlterReplicaLogDirsRequest]
    if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {

      // todo handleAlterReplicaLogDirsRequest
      val result = replicaManager.alterReplicaLogDirs(alterReplicaDirsRequest.partitionDirs.asScala)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterReplicaLogDirsResponse(new AlterReplicaLogDirsResponseData()
          .setResults(result.groupBy(_._1.topic).map {
            case (topic, errors) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
              .setTopicName(topic)
              .setPartitions(errors.map {
                case (tp, error) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(error.code)
              }.toList.asJava)
          }.toList.asJava)
          .setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterReplicaDirsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleDescribeLogDirsRequest(request: RequestChannel.Request): Unit = {
    val describeLogDirsDirRequest = request.body[DescribeLogDirsRequest]
    val logDirInfos = {
      if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
        val partitions =
          if (describeLogDirsDirRequest.isAllTopicPartitions)
            replicaManager.logManager.allLogs.map(_.topicPartition).toSet
          else
            describeLogDirsDirRequest.data.topics.asScala.flatMap(
              logDirTopic => logDirTopic.partitions.asScala.map(partitionIndex =>
                new TopicPartition(logDirTopic.topic, partitionIndex))).toSet

        replicaManager.describeLogDirs(partitions)
      } else {
        List.empty[DescribeLogDirsResponseData.DescribeLogDirsResult]
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
      .setThrottleTimeMs(throttleTimeMs)
      .setResults(logDirInfos.asJava)))
  }

  def handleCreateTokenRequest(request: RequestChannel.Request): Unit = {
    metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    // the callback for sending a create token response
    def sendResponseCallback(createResult: CreateTokenResult): Unit = {
      trace(s"Sending create token response for correlation id ${request.header.correlationId} " +
        s"to client ${request.header.clientId}.")
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(requestThrottleMs, createResult.error, request.context.principal, createResult.issueTimestamp,
          createResult.expiryTimestamp, createResult.maxTimestamp, createResult.tokenId, ByteBuffer.wrap(createResult.hmac)))
    }

    if (!allowTokenRequests(request))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(requestThrottleMs, Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, request.context.principal))
    else {
      val renewerList = createTokenRequest.data.renewers.asScala.toList.map(entry =>
        new KafkaPrincipal(entry.principalType, entry.principalName))

      if (renewerList.exists(principal => principal.getPrincipalType != KafkaPrincipal.USER_TYPE)) {
        requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
          CreateDelegationTokenResponse.prepareResponse(requestThrottleMs, Errors.INVALID_PRINCIPAL_TYPE, request.context.principal))
      }
      else {
        tokenManager.createToken(
          request.context.principal,
          renewerList,
          createTokenRequest.data.maxLifetimeMs,
          sendResponseCallback
        )
      }
    }
  }

  def handleRenewTokenRequest(request: RequestChannel.Request): Unit = {
    metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val renewTokenRequest = request.body[RenewDelegationTokenRequest]

    // 发送更新令牌响应的回调
    def sendResponseCallback(error: Errors, expiryTimestamp: Long): Unit = {
      trace("Sending renew token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
          new RenewDelegationTokenResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(error.code)
            .setExpiryTimestampMs(expiryTimestamp)))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, DelegationTokenManager.ErrorTimestamp)
    else {
      tokenManager.renewToken(
        request.context.principal,
        ByteBuffer.wrap(renewTokenRequest.data.hmac),
        renewTokenRequest.data.renewPeriodMs,
        sendResponseCallback
      )
    }
  }

  def handleExpireTokenRequest(request: RequestChannel.Request): Unit = {
    metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val expireTokenRequest = request.body[ExpireDelegationTokenRequest]

    // 发送过期令牌响应的回调函数
    def sendResponseCallback(error: Errors, expiryTimestamp: Long): Unit = {
      trace("Sending expire token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
          new ExpireDelegationTokenResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(error.code)
            .setExpiryTimestampMs(expiryTimestamp)))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, DelegationTokenManager.ErrorTimestamp)
    else {
      tokenManager.expireToken(
        request.context.principal,
        expireTokenRequest.hmac(),
        expireTokenRequest.expiryTimePeriod(),
        sendResponseCallback
      )
    }
  }

  def handleDescribeTokensRequest(request: RequestChannel.Request): Unit = {
    val describeTokenRequest = request.body[DescribeDelegationTokenRequest]

    // 用于发送描述令牌响应的回调
    def sendResponseCallback(error: Errors, tokenDetails: List[DelegationToken]): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeDelegationTokenResponse(requestThrottleMs, error, tokenDetails.asJava))
      trace("Sending describe token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, List.empty)
    else if (!config.tokenAuthEnabled)
      sendResponseCallback(Errors.DELEGATION_TOKEN_AUTH_DISABLED, List.empty)
    else {
      val requestPrincipal = request.context.principal

      if (describeTokenRequest.ownersListEmpty()) {
        sendResponseCallback(Errors.NONE, List())
      }
      else {
        val owners = if (describeTokenRequest.data.owners == null)
          None
        else
          Some(describeTokenRequest.data.owners.asScala.map(p => new KafkaPrincipal(p.principalType(), p.principalName)).toList)

        def authorizeToken(tokenId: String) = authHelper.authorize(request.context, DESCRIBE, DELEGATION_TOKEN, tokenId)

        def eligible(token: TokenInformation) = DelegationTokenManager.filterToken(requestPrincipal, owners, token, authorizeToken)

        val tokens = tokenManager.getTokens(eligible)
        sendResponseCallback(Errors.NONE, tokens)
      }
    }
  }

  def allowTokenRequests(request: RequestChannel.Request): Boolean = {
    val protocol = request.context.securityProtocol
    if (request.context.principal.tokenAuthenticated ||
      protocol == SecurityProtocol.PLAINTEXT ||
      // 禁止来自单向SSL的请求
      (protocol == SecurityProtocol.SSL && request.context.principal == KafkaPrincipal.ANONYMOUS))
      false
    else
      true
  }

  def handleElectReplicaLeader(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.notYetSupported(request))

    val electionRequest = request.body[ElectLeadersRequest]

    def sendResponseCallback(
                              error: ApiError
                            )(
                              results: Map[TopicPartition, ApiError]
                            ): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        val adjustedResults = if (electionRequest.data.topicPartitions == null) {
          /* 在跨所有分区执行选举时，我们应该只返回进行了选举或导致错误的分区。
          *  换句话说，不需要选举的分区(因为它们已经有了正确的leader)不会返回给客户机。
           */
          results.filter { case (_, error) =>
            error.error != Errors.ELECTION_NOT_NEEDED
          }
        } else results

        val electionResults = new util.ArrayList[ReplicaElectionResult]()
        adjustedResults
          .groupBy { case (tp, _) => tp.topic }
          .forKeyValue { (topic, ps) =>
            val electionResult = new ReplicaElectionResult()

            electionResult.setTopic(topic)
            ps.forKeyValue { (topicPartition, error) =>
              val partitionResult = new PartitionResult()
              partitionResult.setPartitionId(topicPartition.partition)
              partitionResult.setErrorCode(error.error.code)
              partitionResult.setErrorMessage(error.message)
              electionResult.partitionResult.add(partitionResult)
            }

            electionResults.add(electionResult)
          }

        new ElectLeadersResponse(
          requestThrottleMs,
          error.error.code,
          electionResults,
          electionRequest.version
        )
      })
    }

    if (!authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val error = new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null)
      val partitionErrors: Map[TopicPartition, ApiError] =
        electionRequest.topicPartitions.iterator.map(partition => partition -> error).toMap

      sendResponseCallback(error)(partitionErrors)
    } else {
      val partitions = if (electionRequest.data.topicPartitions == null) {
        metadataCache.getAllTopics().flatMap(metadataCache.getTopicPartitions)
      } else {
        electionRequest.topicPartitions
      }

      replicaManager.electLeaders(
        zkSupport.controller,
        partitions,
        electionRequest.electionType,
        sendResponseCallback(ApiError.NONE),
        electionRequest.data.timeoutMs
      )
    }
  }

  def handleOffsetDeleteRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val offsetDeleteRequest = request.body[OffsetDeleteRequest]
    val groupId = offsetDeleteRequest.data.groupId

    if (authHelper.authorize(request.context, DELETE, GROUP, groupId)) {
      val topics = offsetDeleteRequest.data.topics.asScala
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, topics)(_.name)

      val topicPartitionErrors = mutable.Map[TopicPartition, Errors]()
      val topicPartitions = mutable.ArrayBuffer[TopicPartition]()

      for (topic <- topics) {
        for (partition <- topic.partitions.asScala) {
          val tp = new TopicPartition(topic.name, partition.partitionIndex)
          if (!authorizedTopics.contains(topic.name))
            topicPartitionErrors(tp) = Errors.TOPIC_AUTHORIZATION_FAILED
          else if (!metadataCache.contains(tp))
            topicPartitionErrors(tp) = Errors.UNKNOWN_TOPIC_OR_PARTITION
          else
            topicPartitions += tp
        }
      }

      val (groupError, authorizedTopicPartitionsErrors) = groupCoordinator.handleDeleteOffsets(
        groupId, topicPartitions, requestLocal)

      topicPartitionErrors ++= authorizedTopicPartitionsErrors

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        if (groupError != Errors.NONE)
          offsetDeleteRequest.getErrorResponse(requestThrottleMs, groupError)
        else {
          val topics = new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection
          topicPartitionErrors.groupBy(_._1.topic).forKeyValue { (topic, topicPartitions) =>
            val partitions = new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection
            topicPartitions.forKeyValue { (topicPartition, error) =>
              partitions.add(
                new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                  .setPartitionIndex(topicPartition.partition)
                  .setErrorCode(error.code)
              )
            }
            topics.add(new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
              .setName(topic)
              .setPartitions(partitions))
          }

          new OffsetDeleteResponse(new OffsetDeleteResponseData()
            .setTopics(topics)
            .setThrottleTimeMs(requestThrottleMs))
        }
      })
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        offsetDeleteRequest.getErrorResponse(requestThrottleMs, Errors.GROUP_AUTHORIZATION_FAILED))
    }
  }

  def handleDescribeClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val describeClientQuotasRequest = request.body[DescribeClientQuotasRequest]

    if (!authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeClientQuotasRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      metadataSupport match {
        case ZkSupport(adminManager, controller, zkClient, forwardingManager, metadataCache) =>
          val result = adminManager.describeClientQuotas(describeClientQuotasRequest.filter)

          val entriesData = result.iterator.map { case (quotaEntity, quotaValues) =>
            val entityData = quotaEntity.entries.asScala.iterator.map { case (entityType, entityName) =>
              new DescribeClientQuotasResponseData.EntityData()
                .setEntityType(entityType)
                .setEntityName(entityName)
            }.toBuffer

            val valueData = quotaValues.iterator.map { case (key, value) =>
              new DescribeClientQuotasResponseData.ValueData()
                .setKey(key)
                .setValue(value)
            }.toBuffer

            new DescribeClientQuotasResponseData.EntryData()
              .setEntity(entityData.asJava)
              .setValues(valueData.asJava)
          }.toBuffer

          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new DescribeClientQuotasResponse(new DescribeClientQuotasResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setEntries(entriesData.asJava)))
        case RaftSupport(_, metadataCache) =>
          val result = metadataCache.describeClientQuotas(describeClientQuotasRequest.data())
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            result.setThrottleTimeMs(requestThrottleMs)
            new DescribeClientQuotasResponse(result)
          })
      }
    }
  }

  def handleAlterClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val alterClientQuotasRequest = request.body[AlterClientQuotasRequest]

    if (authHelper.authorize(request.context, ALTER_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      val result = zkSupport.adminManager.alterClientQuotas(alterClientQuotasRequest.entries.asScala,
        alterClientQuotasRequest.validateOnly)

      val entriesData = result.iterator.map { case (quotaEntity, apiError) =>
        val entityData = quotaEntity.entries.asScala.iterator.map { case (key, value) =>
          new AlterClientQuotasResponseData.EntityData()
            .setEntityType(key)
            .setEntityName(value)
        }.toBuffer

        new AlterClientQuotasResponseData.EntryData()
          .setErrorCode(apiError.error.code)
          .setErrorMessage(apiError.message)
          .setEntity(entityData.asJava)
      }.toBuffer

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterClientQuotasResponse(new AlterClientQuotasResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setEntries(entriesData.asJava)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterClientQuotasRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleDescribeUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.notYetSupported(request))
    val describeUserScramCredentialsRequest = request.body[DescribeUserScramCredentialsRequest]

    if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
      val result = zkSupport.adminManager.describeUserScramCredentials(
        Option(describeUserScramCredentialsRequest.data.users).map(_.asScala.map(_.name).toList))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleAlterUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val alterUserScramCredentialsRequest = request.body[AlterUserScramCredentialsRequest]

    if (!zkSupport.controller.isActive) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.NOT_CONTROLLER.exception))
    } else if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val result = zkSupport.adminManager.alterUserScramCredentials(
        alterUserScramCredentialsRequest.data.upsertions().asScala, alterUserScramCredentialsRequest.data.deletions().asScala)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleAlterIsrRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    val alterIsrRequest = request.body[AlterIsrRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    if (!zkSupport.controller.isActive)
      requestHelper.sendResponseExemptThrottle(request, alterIsrRequest.getErrorResponse(
        AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.NOT_CONTROLLER.exception))
    else
      zkSupport.controller.alterIsrs(alterIsrRequest.data, alterIsrResp =>
        requestHelper.sendResponseExemptThrottle(request, new AlterIsrResponse(alterIsrResp))
      )
  }

  def handleUpdateFeatures(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldAlwaysForward(request))
    val updateFeaturesRequest = request.body[UpdateFeaturesRequest]

    def sendResponseCallback(errors: Either[ApiError, Map[String, ApiError]]): Unit = {
      def createResponse(throttleTimeMs: Int): UpdateFeaturesResponse = {
        errors match {
          case Left(topLevelError) =>
            UpdateFeaturesResponse.createWithErrors(
              topLevelError,
              Collections.emptyMap(),
              throttleTimeMs)
          case Right(featureUpdateErrors) =>
            UpdateFeaturesResponse.createWithErrors(
              ApiError.NONE,
              featureUpdateErrors.asJava,
              throttleTimeMs)
        }
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => createResponse(requestThrottleMs))
    }

    if (!authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      sendResponseCallback(Left(new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED)))
    } else if (!zkSupport.controller.isActive) {
      sendResponseCallback(Left(new ApiError(Errors.NOT_CONTROLLER)))
    } else if (!config.isFeatureVersioningSupported) {
      sendResponseCallback(Left(new ApiError(Errors.INVALID_REQUEST, "Feature versioning system is disabled.")))
    } else {
      zkSupport.controller.updateFeatures(updateFeaturesRequest, sendResponseCallback)
    }
  }

  def handleDescribeCluster(request: RequestChannel.Request): Unit = {
    val describeClusterRequest = request.body[DescribeClusterRequest]

    var clusterAuthorizedOperations = Int.MinValue // Default value in the schema
    // get cluster authorized operations
    if (describeClusterRequest.data.includeClusterAuthorizedOperations) {
      if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
        clusterAuthorizedOperations = authHelper.authorizedOperations(request, Resource.CLUSTER)
      else
        clusterAuthorizedOperations = 0
    }

    val brokers = metadataCache.getAliveBrokerNodes(request.context.listenerName)
    val controllerId = metadataSupport.controllerId.getOrElse(MetadataResponse.NO_CONTROLLER_ID)

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
      val data = new DescribeClusterResponseData()
        .setThrottleTimeMs(requestThrottleMs)
        .setClusterId(clusterId)
        .setControllerId(controllerId)
        .setClusterAuthorizedOperations(clusterAuthorizedOperations);


      brokers.foreach { broker =>
        data.brokers.add(new DescribeClusterResponseData.DescribeClusterBroker()
          .setBrokerId(broker.id)
          .setHost(broker.host)
          .setPort(broker.port)
          .setRack(broker.rack))
      }

      new DescribeClusterResponse(data)
    })
  }

  def handleEnvelope(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))

    // 如果转发尚未启用或该请求已在无效端点上收到，则我们将该请求视为不可解析并关闭连接。
    if (!isForwardingEnabled(request)) {
      info(s"Closing connection ${request.context.connectionId} because it sent an `Envelope` " +
        "request even though forwarding has not been enabled")
      requestChannel.closeConnection(request, Collections.emptyMap())
      return
    } else if (!request.context.fromPrivilegedListener) {
      info(s"Closing connection ${request.context.connectionId} from listener ${request.context.listenerName} " +
        s"because it sent an `Envelope` request, which is only accepted on the inter-broker listener " +
        s"${config.interBrokerListenerName}.")
      requestChannel.closeConnection(request, Collections.emptyMap())
      return
    } else if (!authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new ClusterAuthorizationException(
        s"Principal ${request.context.principal} does not have required CLUSTER_ACTION for envelope"))
      return
    } else if (!zkSupport.controller.isActive) {
      requestHelper.sendErrorResponseMaybeThrottle(request, new NotControllerException(
        s"Broker $brokerId is not the active controller"))
      return
    }

    EnvelopeUtils.handleEnvelopeRequest(request, requestChannel.metrics, handle(_, requestLocal))
  }

  def handleDescribeProducersRequest(request: RequestChannel.Request): Unit = {
    val describeProducersRequest = request.body[DescribeProducersRequest]

    def partitionError(
                        topicPartition: TopicPartition,
                        apiError: ApiError
                      ): DescribeProducersResponseData.PartitionResponse = {
      new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(topicPartition.partition)
        .setErrorCode(apiError.error.code)
        .setErrorMessage(apiError.message)
    }

    val response = new DescribeProducersResponseData()
    describeProducersRequest.data.topics.forEach { topicRequest =>
      val topicResponse = new DescribeProducersResponseData.TopicResponse()
        .setName(topicRequest.name)

      val invalidTopicError = checkValidTopic(topicRequest.name)

      val topicError = invalidTopicError.orElse {
        if (!authHelper.authorize(request.context, READ, TOPIC, topicRequest.name)) {
          Some(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED))
        } else if (!metadataCache.contains(topicRequest.name))
          Some(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION))
        else {
          None
        }
      }

      topicRequest.partitionIndexes.forEach { partitionId =>
        val topicPartition = new TopicPartition(topicRequest.name, partitionId)
        val partitionResponse = topicError match {
          case Some(error) => partitionError(topicPartition, error)
          case None => replicaManager.activeProducerState(topicPartition)
        }
        topicResponse.partitions.add(partitionResponse)
      }

      response.topics.add(topicResponse)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeProducersResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  private def checkValidTopic(topic: String): Option[ApiError] = {
    try {
      Topic.validate(topic)
      None
    } catch {
      case e: Throwable => Some(ApiError.fromThrowable(e))
    }
  }

  def handleDescribeTransactionsRequest(request: RequestChannel.Request): Unit = {
    val describeTransactionsRequest = request.body[DescribeTransactionsRequest]
    val response = new DescribeTransactionsResponseData()

    describeTransactionsRequest.data.transactionalIds.forEach { transactionalId =>
      val transactionState = if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionalId)) {
        new DescribeTransactionsResponseData.TransactionState()
          .setTransactionalId(transactionalId)
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
      } else {
        txnCoordinator.handleDescribeTransactions(transactionalId)
      }

      // Include only partitions which the principal is authorized to describe
      val topicIter = transactionState.topics.iterator()
      while (topicIter.hasNext) {
        val topic = topicIter.next().topic
        if (!authHelper.authorize(request.context, DESCRIBE, TOPIC, topic)) {
          topicIter.remove()
        }
      }
      response.transactionStates.add(transactionState)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleListTransactionsRequest(request: RequestChannel.Request): Unit = {
    val listTransactionsRequest = request.body[ListTransactionsRequest]
    val filteredProducerIds = listTransactionsRequest.data.producerIdFilters.asScala.map(Long.unbox).toSet
    val filteredStates = listTransactionsRequest.data.stateFilters.asScala.toSet
    val response = txnCoordinator.handleListTransactions(filteredProducerIds, filteredStates)

    //响应应该只包含主体具有“描述”访问权限的transactionalIds。
    val transactionStateIter = response.transactionStates.iterator()
    while (transactionStateIter.hasNext) {
      val transactionState = transactionStateIter.next()
      if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionState.transactionalId)) {
        transactionStateIter.remove()
      }
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new ListTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleAllocateProducerIdsRequest(request: RequestChannel.Request): Unit = {
    val zkSupport = metadataSupport.requireZkOrThrow(KafkaApis.shouldNeverReceive(request))
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    val allocateProducerIdsRequest = request.body[AllocateProducerIdsRequest]

    if (!zkSupport.controller.isActive)
      requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs =>
        allocateProducerIdsRequest.getErrorResponse(throttleTimeMs, Errors.NOT_CONTROLLER.exception))
    else
      zkSupport.controller.allocateProducerIds(allocateProducerIdsRequest.data, producerIdsResponse =>
        requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs =>
          new AllocateProducerIdsResponse(producerIdsResponse.setThrottleTimeMs(throttleTimeMs)))
      )
  }

  private def updateRecordConversionStats(request: RequestChannel.Request,
                                          tp: TopicPartition,
                                          conversionStats: RecordConversionStats): Unit = {
    val conversionCount = conversionStats.numRecordsConverted
    if (conversionCount > 0) {
      request.header.apiKey match {
        case ApiKeys.PRODUCE =>
          brokerTopicStats.topicStats(tp.topic).produceMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.produceMessageConversionsRate.mark(conversionCount)
        case ApiKeys.FETCH =>
          brokerTopicStats.topicStats(tp.topic).fetchMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.fetchMessageConversionsRate.mark(conversionCount)
        case _ =>
          throw new IllegalStateException("Message conversion info is recorded only for Produce/Fetch requests")
      }
      request.messageConversionsTimeNanos = conversionStats.conversionTimeNanos
    }
    request.temporaryMemoryBytes = conversionStats.temporaryMemoryBytes
  }

  private def isBrokerEpochStale(zkSupport: ZkSupport, brokerEpochInRequest: Long): Boolean = {
    // 如果控制器没有升级到使用KIP-380，则LeaderAndIsr/UpdateMetadata/StopReplica请求中的代理纪元是未知的
    if (brokerEpochInRequest == AbstractControlRequest.UNKNOWN_BROKER_EPOCH) false
    else {
      // brokerEpochInRequest > controller.brokerEpoch is possible in rare scenarios where the controller gets notified
      // about the new broker epoch and sends a control request with this epoch before the broker learns about it
      brokerEpochInRequest < zkSupport.controller.brokerEpoch
    }
  }
}

object KafkaApis {
  // 同步副本和非同步副本的流量都计入复制配额，以确保总复制流量不超过配额。
  private[server] def sizeOfThrottledPartitions(versionId: Short,
                                                unconvertedResponse: FetchResponse,
                                                quota: ReplicationQuotaManager): Int = {
    FetchResponse.sizeOf(versionId, unconvertedResponse.responseData.entrySet
      .iterator.asScala.filter(element => quota.isThrottled(element.getKey)).asJava)
  }

  private[server] def shouldNeverReceive(request: RequestChannel.Request): Exception = {
    new UnsupportedVersionException(s"Should never receive when using a Raft-based metadata quorum: ${request.header.apiKey()}")
  }

  private[server] def shouldAlwaysForward(request: RequestChannel.Request): Exception = {
    new UnsupportedVersionException(s"Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: ${request.header.apiKey}")
  }

  private def unsupported(text: String): Exception = {
    new UnsupportedVersionException(s"Unsupported when using a Raft-based metadata quorum: $text")
  }

  private def notYetSupported(request: RequestChannel.Request): Exception = {
    notYetSupported(request.header.apiKey().toString)
  }

  private def notYetSupported(text: String): Exception = {
    new UnsupportedVersionException(s"Not yet supported when using a Raft-based metadata quorum: $text")
  }
}
