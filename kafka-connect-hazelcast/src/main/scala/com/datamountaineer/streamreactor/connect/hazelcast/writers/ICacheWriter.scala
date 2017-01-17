/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.hazelcast.writers

import javax.cache.{Cache, Caching}

import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkSettings
import com.hazelcast.cache.HazelcastCachingProvider
import com.hazelcast.config.CacheConfig
import com.hazelcast.core.HazelcastInstance
import org.apache.kafka.connect.sink.SinkRecord

/**
  * Created by andrew@datamountaineer.com on 02/12/2016. 
  * stream-reactor
  */
case class JCacheWriter(client: HazelcastInstance, topic: String, settings: HazelCastSinkSettings) extends Writer(settings) {

  val cachingProvider = new HazelcastCachingProvider

  

  val cacheManager = cachingProvider.getCacheManager
  val cache = cacheManager.getCache(settings.topicObject(topic).name).asInstanceOf[Cache[String, Object]]

  override def write(record: SinkRecord): Unit = cache.put(buildPKs(record), convert(record))
}
