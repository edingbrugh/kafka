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

package kafka.controller

import com.typesafe.scalalogging.Logger
import kafka.utils.Logging

object StateChangeLogger {
  private val logger = Logger("state.change.logger")
}

/**
 * 一个简单的类，根据状态更改记录器是否在kafkaController的上下文中被使用来适当地设置' logIdent '
 * (例如，ReplicaManager和MetadataCache记录到状态更改记录器，而不管代理是否是控制器)。
 */
class StateChangeLogger(brokerId: Int, inControllerContext: Boolean, controllerEpoch: Option[Int]) extends Logging {

  if (controllerEpoch.isDefined && !inControllerContext)
    throw new IllegalArgumentException("Controller epoch should only be defined if inControllerContext is true")

  override lazy val logger = StateChangeLogger.logger

  locally {
    val prefix = if (inControllerContext) "Controller" else "Broker"
    val epochEntry = controllerEpoch.fold("")(epoch => s" epoch=$epoch")
    logIdent = s"[$prefix id=$brokerId$epochEntry] "
  }

  def withControllerEpoch(controllerEpoch: Int): StateChangeLogger =
    new StateChangeLogger(brokerId, inControllerContext, Some(controllerEpoch))

  def messageWithPrefix(message: String): String = msgWithLogIdent(message)

}
