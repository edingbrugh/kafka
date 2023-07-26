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

import kafka.server.{BrokerToControllerChannelManager, ControllerRequestCompletionHandler}
import kafka.utils.Logging
import kafka.zk.{KafkaZkClient, ProducerIdBlockZNode}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AllocateProducerIdsRequest, AllocateProducerIdsResponse}
import org.apache.kafka.server.common.ProducerIdsBlock

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success, Try}

/**
 * ProducerIdManager是事务协调器的一部分，它以一种独特的方式提供producerId，使相同的producerId不会跨多个事务协调器分配两次。
 *
 * ProducerId由控制器管理。当请求一个新的id范围时，我们保证会收到一个唯一的块。
 */

object ProducerIdManager {
  // 一旦我们从当前块中消耗的pid达到这个百分比，就触发下一个块的获取
  val PidPrefetchThreshold = 0.90

  // 创建一个ProducerIdGenerate，直接与ZooKeeper接口，IBP < 3.0-IV0
  def zk(brokerId: Int, zkClient: KafkaZkClient): ZkProducerIdManager = {
    new ZkProducerIdManager(brokerId, zkClient)
  }

  // 创建一个使用AllocateProducerIds RPC的ProducerIdGenerate, IBP >= 3.0-IV0
  def rpc(brokerId: Int,
            brokerEpochSupplier: () => Long,
            controllerChannel: BrokerToControllerChannelManager,
            maxWaitMs: Int): RPCProducerIdManager = {
    new RPCProducerIdManager(brokerId, brokerEpochSupplier, controllerChannel, maxWaitMs)
  }
}

trait ProducerIdManager {
  def generateProducerId(): Long
  def shutdown() : Unit = {}
}

object ZkProducerIdManager {
  def getNewProducerIdBlock(brokerId: Int, zkClient: KafkaZkClient, logger: Logging): ProducerIdsBlock = {
    // 从ZK获取或创建现有的PID块，并尝试更新它。我们在循环中重试，因为其他代理可能在滚动升级期间生成PID块
    var zkWriteComplete = false
    while (!zkWriteComplete) {
      // 再次从zookeeper中刷新当前的producerId块
      val (dataOpt, zkVersion) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)

      // 生成新的producerId块
      val newProducerIdBlock = dataOpt match {
        case Some(data) =>
          val currProducerIdBlock = ProducerIdBlockZNode.parseProducerIdBlockData(data)
          logger.debug(s"Read current producerId block $currProducerIdBlock, Zk path version $zkVersion")

          if (currProducerIdBlock.producerIdEnd > Long.MaxValue - ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE) {
            // 我们已经耗尽了所有的producerid(哇!)，将其视为致命错误
            logger.fatal(s"Exhausted all producerIds as the next block's end producerId is will has exceeded long type limit (current block end producerId is ${currProducerIdBlock.producerIdEnd})")
            throw new KafkaException("Have exhausted all producerIds.")
          }

          new ProducerIdsBlock(brokerId, currProducerIdBlock.producerIdEnd + 1L, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
        case None =>
          logger.debug(s"There is no producerId block yet (Zk path version $zkVersion), creating the first block")
          new ProducerIdsBlock(brokerId, 0L, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE)
      }

      val newProducerIdBlockData = ProducerIdBlockZNode.generateProducerIdBlockJson(newProducerIdBlock)

      // 尝试将新的producerId块写入zookeeper
      val (succeeded, version) = zkClient.conditionalUpdatePath(ProducerIdBlockZNode.path, newProducerIdBlockData, zkVersion, None)
      zkWriteComplete = succeeded

      if (zkWriteComplete) {
        logger.info(s"Acquired new producerId block $newProducerIdBlock by writing to Zk with path version $version")
        return newProducerIdBlock
      }
    }
    throw new IllegalStateException()
  }
}

class ZkProducerIdManager(brokerId: Int,
                          zkClient: KafkaZkClient) extends ProducerIdManager with Logging {

  this.logIdent = "[ZK ProducerId Manager " + brokerId + "]: "

  private var currentProducerIdBlock: ProducerIdsBlock = ProducerIdsBlock.EMPTY
  private var nextProducerId: Long = _

  // 获取第一个producerid块
  this synchronized {
    allocateNewProducerIdBlock()
    nextProducerId = currentProducerIdBlock.producerIdStart
  }

  private def allocateNewProducerIdBlock(): Unit = {
    this synchronized {
      currentProducerIdBlock = ZkProducerIdManager.getNewProducerIdBlock(brokerId, zkClient, this)
    }
  }

  def generateProducerId(): Long = {
    this synchronized {
      // 如果该块已耗尽，则获取一个新的producerid块
      if (nextProducerId > currentProducerIdBlock.producerIdEnd) {
        allocateNewProducerIdBlock()
        nextProducerId = currentProducerIdBlock.producerIdStart
      }
      nextProducerId += 1
      nextProducerId - 1
    }
  }
}

class RPCProducerIdManager(brokerId: Int,
                           brokerEpochSupplier: () => Long,
                           controllerChannel: BrokerToControllerChannelManager,
                           maxWaitMs: Int) extends ProducerIdManager with Logging {

  this.logIdent = "[RPC ProducerId Manager " + brokerId + "]: "

  private val nextProducerIdBlock = new ArrayBlockingQueue[Try[ProducerIdsBlock]](1)
  private val requestInFlight = new AtomicBoolean(false)
  private var currentProducerIdBlock: ProducerIdsBlock = ProducerIdsBlock.EMPTY
  private var nextProducerId: Long = -1L

  override def generateProducerId(): Long = {
    this synchronized {
      if (nextProducerId == -1L) {
        // 发送初始请求以获取第一个块
        maybeRequestNextBlock()
        nextProducerId = 0L
      } else {
        nextProducerId += 1

        // 检查是否需要获取下一个block
        if (nextProducerId >= (currentProducerIdBlock.producerIdStart + currentProducerIdBlock.producerIdLen * ProducerIdManager.PidPrefetchThreshold)) {
          maybeRequestNextBlock()
        }
      }

      // 如果我们已经耗尽了当前的块，抓取下一个块(必要时等待)
      if (nextProducerId > currentProducerIdBlock.producerIdEnd) {
        val block = nextProducerIdBlock.poll(maxWaitMs, TimeUnit.MILLISECONDS)
        if (block == null) {
          throw Errors.REQUEST_TIMED_OUT.exception("Timed out waiting for next producer ID block")
        } else {
          block match {
            case Success(nextBlock) =>
              currentProducerIdBlock = nextBlock
              nextProducerId = currentProducerIdBlock.producerIdStart
            case Failure(t) => throw t
          }
        }
      }
      nextProducerId
    }
  }


  private def maybeRequestNextBlock(): Unit = {
    if (nextProducerIdBlock.isEmpty && requestInFlight.compareAndSet(false, true)) {
      sendRequest()
    }
  }

  private[transaction] def sendRequest(): Unit = {
    val message = new AllocateProducerIdsRequestData()
      .setBrokerEpoch(brokerEpochSupplier.apply())
      .setBrokerId(brokerId)

    val request = new AllocateProducerIdsRequest.Builder(message)
    debug("Requesting next Producer ID block")
    controllerChannel.sendRequest(request, new ControllerRequestCompletionHandler() {
      override def onComplete(response: ClientResponse): Unit = {
        val message = response.responseBody().asInstanceOf[AllocateProducerIdsResponse]
        handleAllocateProducerIdsResponse(message)
      }

      override def onTimeout(): Unit = handleTimeout()
    })
  }

  private[transaction] def handleAllocateProducerIdsResponse(response: AllocateProducerIdsResponse): Unit = {
    requestInFlight.set(false)
    val data = response.data
    Errors.forCode(data.errorCode()) match {
      case Errors.NONE =>
        debug(s"Got next producer ID block from controller $data")
        // 响应进行一些完整性检查
        if (data.producerIdStart() < currentProducerIdBlock.producerIdEnd) {
          nextProducerIdBlock.put(Failure(new KafkaException(
            s"Producer ID block is not monotonic with current block: current=$currentProducerIdBlock response=$data")))
        } else if (data.producerIdStart() < 0 || data.producerIdLen() < 0 || data.producerIdStart() > Long.MaxValue - data.producerIdLen()) {
          nextProducerIdBlock.put(Failure(new KafkaException(s"Producer ID block includes invalid ID range: $data")))
        } else {
          nextProducerIdBlock.put(
            Success(new ProducerIdsBlock(brokerId, data.producerIdStart(), data.producerIdLen())))
        }
      case Errors.STALE_BROKER_EPOCH =>
        warn("Our broker epoch was stale, trying again.")
        maybeRequestNextBlock()
      case Errors.BROKER_ID_NOT_REGISTERED =>
        warn("Our broker ID is not yet known by the controller, trying again.")
        maybeRequestNextBlock()
      case e: Errors =>
        warn("Had an unknown error from the controller, giving up.")
        nextProducerIdBlock.put(Failure(e.exception()))
    }
  }

  private[transaction] def handleTimeout(): Unit = {
    warn("Timed out when requesting AllocateProducerIds from the controller.")
    requestInFlight.set(false)
    nextProducerIdBlock.put(Failure(Errors.REQUEST_TIMED_OUT.exception))
    maybeRequestNextBlock()
  }
}
