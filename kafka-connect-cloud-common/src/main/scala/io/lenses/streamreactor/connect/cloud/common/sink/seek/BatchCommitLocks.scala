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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

/**
 * Per-topic-partition mutual exclusion between a batch-mode `commitBatch` and the deletes of the
 * `.temp-upload` orphan sweep (LC-349).
 *
 * The commit holds the partition's lock across stage -> CAS -> copy chain -> finalise. The sweep
 * takes it once per delete chunk, so a commit never waits for more than one master GET plus one
 * `deleteFiles` chunk. While the sweep holds it, no commit is in flight on that partition, so the
 * sweep's fresh master read is authoritative for every CAS that landed after its LIST.
 *
 * The lock is only ever taken and released inside one synchronous block, never across an `IO`
 * boundary.
 */
trait BatchCommitLocks {

  /** Runs `f` holding `tp`'s lock, waiting uninterruptibly. Used by the commit side. */
  def withLock[A](tp: TopicPartition)(f: => A): A

  /**
   * Runs `f` holding `tp`'s lock. Throws `InterruptedException` (without running `f`) if the
   * thread is interrupted while waiting, so `shutdownNow` can stop a sweep blocked behind a commit.
   */
  def withLockInterruptibly[A](tp: TopicPartition)(f: => A): A
}

object BatchCommitLocks {

  def apply(): PerPartitionBatchCommitLocks = new PerPartitionBatchCommitLocks

  /** Runs every body directly. The default for granular mode, `NoIndexManager` and tests. */
  object NoOp extends BatchCommitLocks {
    override def withLock[A](tp: TopicPartition)(f: => A): A = f

    override def withLockInterruptibly[A](tp: TopicPartition)(f: => A): A = f
  }
}

final class PerPartitionBatchCommitLocks extends BatchCommitLocks {

  // Entries are created on first use and NEVER removed. Removing one on revoke could hand a later
  // commit a fresh lock while an old sweep still holds (or waits on) the previous one, and the
  // two would no longer exclude each other. Memory is bounded by the partitions ever assigned.
  private val locks = new ConcurrentHashMap[TopicPartition, ReentrantLock]()

  // Fair, so a commit that queues while a sweep chunk runs gets the lock before that sweep's next
  // chunk: this is what bounds the commit's wait to one chunk.
  private[sink] def lockFor(tp: TopicPartition): ReentrantLock =
    locks.computeIfAbsent(tp, _ => new ReentrantLock(true))

  private[sink] def size: Int = locks.size()

  override def withLock[A](tp: TopicPartition)(f: => A): A = {
    val lock = lockFor(tp)
    lock.lock()
    try f
    finally lock.unlock()
  }

  override def withLockInterruptibly[A](tp: TopicPartition)(f: => A): A = {
    val lock = lockFor(tp)
    lock.lockInterruptibly()
    try f
    finally lock.unlock()
  }
}
