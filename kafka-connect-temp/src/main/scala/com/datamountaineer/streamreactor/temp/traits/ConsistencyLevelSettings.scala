/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.temp.traits

import org.apache.kafka.common.config.ConfigException

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait ConsistencyLevelSettings[T <: Enum[T]] extends BaseSettings {
  val consistencyLevelConstant: String

  def getConsistencyLevel(implicit ct: ClassTag[T]): Option[T] = {

    val enum: Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]

    val consistencyLevel = getString(consistencyLevelConstant) match {
      case "" => None
      case other =>
        Try(Enum.valueOf[T](enum, other)) match {
          case Failure(e) => throw new ConfigException(s"'$other' is not a valid $consistencyLevelConstant. " +
            s"Available values are:${enum.getEnumConstants.map(_.toString).mkString(",")}")
          case Success(cl) => Some(cl)
        }
    }

    consistencyLevel
  }
}
