/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.sink.conversion

import java.time.temporal.ChronoUnit
import java.time.LocalDate
import java.time.ZoneId
import java.util.Calendar
import java.util.Date

object TimeUtils {

  def dateWithTimeFieldsOnly(hour: Int, minute: Int, second: Int, millis: Int): Date = {
    val calendar = Calendar.getInstance()

    // Set date fields to epoch start (January 1, 1970)
    calendar.set(Calendar.YEAR, 1970)
    calendar.set(Calendar.MONTH, Calendar.JANUARY)
    calendar.set(Calendar.DAY_OF_MONTH, 1)

    // Set time fields
    calendar.set(Calendar.HOUR_OF_DAY, hour)
    calendar.set(Calendar.MINUTE, minute)
    calendar.set(Calendar.SECOND, second)
    calendar.set(Calendar.MILLISECOND, millis)

    calendar.getTime
  }

  def toLocalDate(date: Date): LocalDate =
    date.toInstant.atZone(ZoneId.systemDefault).toLocalDate

  def daysSinceEpoch(date: Date): Long = {
    val localDate = toLocalDate(date)
    val epoch     = LocalDate.ofEpochDay(0)
    ChronoUnit.DAYS.between(epoch, localDate)
  }
}
