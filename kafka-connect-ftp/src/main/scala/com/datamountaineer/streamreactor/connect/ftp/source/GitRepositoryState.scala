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

package com.datamountaineer.streamreactor.connect.ftp.source

import java.util.Properties

object GitRepositoryState {
  val props = {
    val p = new Properties()
    p.load(getClass.getClassLoader.getResourceAsStream("git.properties"))
    p
  }

  def describe: String = props.getProperty("git.commit.id.describe")
  def build: String = props.getProperty("git.build.time")
  def commitDate: String = props.getProperty("git.commit.time")
  def commitId: String = props.getProperty("git.commit.id")

  def summary:String = s"$describe ($commitId), committed at $commitDate, built at $build"
}