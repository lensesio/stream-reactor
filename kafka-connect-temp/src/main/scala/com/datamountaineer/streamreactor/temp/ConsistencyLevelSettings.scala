package com.datamountaineer.streamreactor.temp

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
