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
package io.lenses.streamreactor.connect.cloud.common.testing

import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder
import io.circe.syntax.EncoderOps
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage._
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface._

import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/**
 * Thread-safe in-memory `StorageInterface` for exactly-once scenario tests.
 *
 * Design points:
 *
 *  - Blobs are keyed by `(bucket, path)` and tracked with a fresh UUID `eTag` per write, or, with
 *    `contentETags`, an eTag derived from the bytes (as S3 reports for a single-part PUT), so
 *    rewriting identical bytes yields the same eTag.
 *  - Conditional writes (`NoOverwriteExistingObject`, `ObjectWithETag`) implement the same
 *    semantics the production S3/GCS/ADL backends use:
 *      - `NoOverwriteExistingObject` -> `If-None-Match: *`. Succeeds only when the key is
 *        absent; otherwise returns `FileCreateError` (mirroring how `AwsS3StorageInterface`
 *        translates a 412 PreconditionFailed).
 *      - `ObjectWithETag(eTag)` -> `If-Match: <eTag>`. Compare-and-swap on the stored eTag;
 *        a mismatch returns `FileCreateError` so callers' fencing logic still applies.
 *  - `mvFile` mirrors the post-fix S3 semantics: missing source with present destination
 *    returns `Right(())` (idempotent replay); missing source with missing destination
 *    returns `FileMoveError` (loud failure, no silent data loss).
 *  - `deleteFile` is unconditional (matching the production trait's signature; the eTag
 *    parameter is recorded but not enforced because S3's `DeleteObject` doesn't support
 *    `If-Match`). Missing keys are no-ops so replay is idempotent.
 *
 * Crash injection is via [[arm]]: tests register one-shot `Hook` instances that fire on the
 * next matching write/upload/move/delete and are then consumed. Hooks default to none, so
 * happy-path scenarios need no setup.
 */
class InMemoryStorageInterface(systemName: String = "in-memory", contentETags: Boolean = false)
    extends StorageInterface[FakeFileMetadata] {

  private val store: ConcurrentHashMap[(String, String), StoredBlob] =
    new ConcurrentHashMap[(String, String), StoredBlob]()

  // Hooks are kept in insertion order so tests can layer multiple matches deterministically.
  private val hooks: AtomicReference[Vector[Hook]] = new AtomicReference(Vector.empty)

  // Counts uploadFile calls for DropUploadAfterCount.
  private val uploadCount = new AtomicInteger(0)

  // Counts content reads per key (see `readCount`), for tests asserting a path was never read.
  private val reads = new ConcurrentHashMap[(String, String), AtomicInteger]()

  // Counts `writeBlobToFile` calls per key (see `writeCount`).
  private val writes = new ConcurrentHashMap[(String, String), AtomicInteger]()

  // ---------- public test helpers ----------

  /** Register a one-shot crash-injection hook. Returns the same instance for chaining. */
  def arm(hook: Hook): InMemoryStorageInterface = {
    val _ = hooks.updateAndGet(_ :+ hook)
    this
  }

  /** Drop all armed hooks. */
  def disarmAll(): Unit = hooks.set(Vector.empty)

  /** Direct read of the underlying store -- diagnostic only. */
  def snapshot(bucket: String): Map[String, StoredBlob] =
    store.entrySet().iterator().asScala.collect {
      case e if e.getKey._1 == bucket => e.getKey._2 -> e.getValue
    }.toMap

  /** Number of objects under a (bucket, prefix). */
  def countUnder(bucket: String, prefix: String): Int =
    store.keys().asScala.count { case (b, p) => b == bucket && p.startsWith(prefix) }

  /** All keys under a (bucket, prefix), sorted. */
  def keysUnder(bucket: String, prefix: String): Seq[String] =
    store.keys().asScala.collect {
      case (b, p) if b == bucket && p.startsWith(prefix) => p
    }.toSeq.sorted

  /**
   * Force an entry's lastModified to a specific instant, simulating an aged lock for the
   * sweep path. Returns false if the key does not exist.
   */
  def setLastModified(bucket: String, path: String, instant: Instant): Boolean =
    Option(store.get((bucket, path))) match {
      case Some(blob) =>
        store.put((bucket, path), blob.copy(lastModified = instant))
        true
      case None => false
    }

  /**
   * Store raw bytes at a path with a fresh eTag, bypassing every write path's validation. For
   * seeding shapes the production writers can't produce, e.g. a 0-byte "poison" lock blob.
   */
  def putRaw(bucket: String, path: String, bytes: Array[Byte]): Unit = {
    val _ = store.put((bucket, path), StoredBlob(bytes, freshETag(), Instant.now()))
  }

  /**
   * How many times a key's content has been read: `getBlob`, `getBlobAsString` and
   * `getBlobAsStringAndEtag` (and so `getBlobAsObject`), whether or not the key existed. Metadata,
   * existence and listing calls (`getMetadata`, `pathExists`, `list*`) are not counted.
   */
  def readCount(bucket: String, path: String): Int =
    Option(reads.get((bucket, path))).map(_.get()).getOrElse(0)

  /**
   * How many `writeBlobToFile` calls a key has received (lock writes), whether they succeeded,
   * failed their precondition or were failed by a hook.
   */
  def writeCount(bucket: String, path: String): Int =
    Option(writes.get((bucket, path))).map(_.get()).getOrElse(0)

  // ---------- StorageInterface methods used by the sink write-path ----------

  override def system(): String = systemName

  override def close(): Unit = ()

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
    val n = uploadCount.incrementAndGet()
    takeFirstHook { case h: FailWriteAt if h.matches(bucket, path) => h } match {
      case Some(h) => h.asUploadError(bucket, path).asLeft
      case None =>
        takeFirstHook { case h: DropUploadAfterCount if h.matches(bucket, path) && n > h.threshold => h } match {
          case Some(_) => freshETag().asRight
          case None =>
            source.validate.toEither match {
              case Left(err) => err.asLeft
              case Right(file) =>
                try {
                  val bytes = Files.readAllBytes(file.toPath)
                  val eTag  = freshETag()
                  store.put((bucket, path), StoredBlob(bytes, eTag, Instant.now()))
                  eTag.asRight
                } catch {
                  case ex: IOException => UploadFailedError(ex, file).asLeft
                }
            }
        }
    }
  }

  override def pathExists(bucket: String, path: String): Either[PathError, Boolean] =
    store.containsKey((bucket, path)).asRight

  override def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[FakeFileMetadata],
    numResults: Int,
  ): Either[FileListError, Option[ListOfKeysResponse[FakeFileMetadata]]] = {
    val keys = collectKeys(bucket, prefix)
      .filter(k => lastFile.forall(lf => k > lf.file))
      .take(numResults)
      .map(k => fakeMeta(bucket, k))
    Right(processAsKey(bucket, prefix, keys))
  }

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[FakeFileMetadata]]] = {
    val metas = collectKeys(bucket, prefix).map(k => fakeMeta(bucket, k))
    Right(processObjectsAsFileMeta(bucket, prefix, metas))
  }

  override def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[FakeFileMetadata]]] = {
    val metas = collectKeys(bucket, prefix).map(k => fakeMeta(bucket, k))
    Right(processAsKey(bucket, prefix, metas))
  }

  override def seekToFile(
    bucket:       String,
    fileName:     String,
    lastModified: Option[Instant],
  ): Option[FakeFileMetadata] =
    lastModified.map(FakeFileMetadata(fileName, _))
      .orElse(Option(store.get((bucket, fileName))).map(b => FakeFileMetadata(fileName, b.lastModified)))

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] = {
    recordRead(bucket, path)
    val read = Option(store.get((bucket, path)))
    afterRead(bucket, path)
    read match {
      case Some(blob) => Right(new ByteArrayInputStream(blob.bytes))
      case None       => Left(FileNotFoundError(new IOException(s"No such key: $bucket/$path"), path))
    }
  }

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] = {
    recordRead(bucket, path)
    val read = Option(store.get((bucket, path)))
    afterRead(bucket, path)
    read match {
      case Some(blob) => Right(new String(blob.bytes, StandardCharsets.UTF_8))
      case None       => Left(FileNotFoundError(new IOException(s"No such key: $bucket/$path"), path))
    }
  }

  override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] = {
    recordRead(bucket, path)
    val read = Option(store.get((bucket, path)))
    afterRead(bucket, path)
    read match {
      case Some(blob) => Right((new String(blob.bytes, StandardCharsets.UTF_8), blob.eTag))
      case None       => Left(FileNotFoundError(new IOException(s"No such key: $bucket/$path"), path))
    }
  }

  override def writeBlobToFile[O](
    bucket:           String,
    path:             String,
    objectProtection: ObjectProtection[O],
  )(
    implicit
    encoder: Encoder[O],
  ): Either[UploadError, ObjectWithETag[O]] = {
    val _ = writes.computeIfAbsent((bucket, path), _ => new AtomicInteger(0)).incrementAndGet()
    takeFirstHook {
      case h: OnWriteAt if h.matches(bucket, path) && h.onlyIf(objectProtection.wrappedObject) => h
    }.foreach(_.action())
    takeFirstHook { case h: FailWriteAt if h.matches(bucket, path) => h } match {
      case Some(h) => h.asUploadError(bucket, path).asLeft
      case None =>
        takeFirstHook { case h: LoseWriteResponseAt if h.matches(bucket, path) => h } match {
          case Some(h) =>
            // The write is applied, but the caller only sees an error (lost response).
            persist(bucket, path, objectProtection, freshETag(), None)
              .flatMap(_ => h.asUploadError(bucket, path).asLeft)
          case None =>
            // CorruptETag persists normally but reports a wrong eTag back to the caller.
            val cachedOverride = takeFirstHook { case h: CorruptETag if h.matches(bucket, path) => h }
              .map(_ => freshETag())
            persist(bucket, path, objectProtection, freshETag(), cachedOverride)
        }
    }
  }

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    Option(store.get((bucket, path))) match {
      case Some(blob) => Right(ObjectMetadata(blob.bytes.length.toLong, blob.lastModified))
      case None       => Left(FileNotFoundError(new IOException(s"No such key: $bucket/$path"), path))
    }

  override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] =
    takeFirstHook { case h: FailWriteAt if h.matches(bucket, path) => h } match {
      case Some(h) => h.asUploadError(bucket, path).asLeft
      case None =>
        data.validate.toEither match {
          case Left(err) => err.asLeft
          case Right(s) =>
            store.put((bucket, path), StoredBlob(s.getBytes(StandardCharsets.UTF_8), freshETag(), Instant.now()))
            ().asRight
        }
    }

  override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] = {
    val _ = eTag
    takeFirstHook { case h: OnDeleteAt if h.matches(bucket, file) => h }.foreach(_.action())
    takeFirstHook { case h: FailDeleteAt if h.matches(bucket, file) => h } match {
      case Some(h) => h.asDeleteError(bucket, file).asLeft
      case None =>
        val _ = store.remove((bucket, file))
        ().asRight
    }
  }

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] =
    if (files.isEmpty) ().asRight
    else
      files.foldLeft[Either[FileDeleteError, Unit]](().asRight) { (acc, f) =>
        acc.flatMap(_ => deleteFile(bucket, f, ""))
      }

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit] =
    takeFirstHook { case h: FailMoveAt if h.matches(oldBucket, oldPath) => h } match {
      case Some(h) => h.asMoveError(oldPath, newPath).asLeft
      case None =>
        Option(store.get((oldBucket, oldPath))) match {
          case None =>
            // Match the post-fix S3 semantics: missing source + present destination is
            // idempotent success; missing source + missing destination is a loud failure.
            if (store.containsKey((newBucket, newPath))) ().asRight
            else FileMoveError(
              new IllegalStateException(
                s"Source $oldBucket/$oldPath and destination $newBucket/$newPath both missing",
              ),
              oldPath,
              newPath,
            ).asLeft
          case Some(blob) =>
            maybeEtag match {
              case Some(expected) if expected != blob.eTag =>
                FileMoveError(
                  new IllegalStateException(
                    s"eTag mismatch on $oldBucket/$oldPath: expected $expected, found ${blob.eTag}",
                  ),
                  oldPath,
                  newPath,
                ).asLeft
              case _ =>
                store.put((newBucket, newPath), blob.copy(eTag = freshETag(), lastModified = Instant.now()))
                val _ = store.remove((oldBucket, oldPath))
                ().asRight
            }
        }
    }

  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] = {
    val _ = (bucket, path)
    ().asRight
  }

  override def touchFile(bucket: String, path: String): Either[FileTouchError, Unit] =
    Option(store.get((bucket, path))) match {
      case Some(blob) =>
        store.put((bucket, path), blob.copy(lastModified = Instant.now()))
        ().asRight
      case None => Left(FileTouchError(new IOException(s"No such key: $bucket/$path"), path))
    }

  // ---------- helpers ----------

  private def fakeMeta(bucket: String, key: String): FakeFileMetadata =
    FakeFileMetadata(key, Option(store.get((bucket, key))).map(_.lastModified).getOrElse(Instant.EPOCH))

  private def collectKeys(bucket: String, prefix: Option[String]): Seq[String] = {
    val pfx = prefix.getOrElse("")
    store.keys().asScala.collect {
      case (b, p) if b == bucket && p.startsWith(pfx) => p
    }.toSeq.sorted
  }

  private def freshETag(): String = UUID.randomUUID().toString

  /** Counts the read, then runs an armed `OnReadAt` action before the caller sees the content. */
  private def recordRead(bucket: String, path: String): Unit = {
    val _ = reads.computeIfAbsent((bucket, path), _ => new AtomicInteger(0)).incrementAndGet()
    takeFirstHook { case h: OnReadAt if h.matches(bucket, path) => h }.foreach(_.action())
  }

  /** Runs an armed `AfterReadAt` action once the read has taken the content, before the caller sees it. */
  private def afterRead(bucket: String, path: String): Unit =
    takeFirstHook { case h: AfterReadAt if h.matches(bucket, path) => h }.foreach(_.action())

  /** The eTag a write of `bytes` gets: fresh per write, or the bytes' digest with `contentETags`. */
  private def eTagFor(bytes: Array[Byte], fresh: String): String =
    if (contentETags)
      java.security.MessageDigest.getInstance("MD5").digest(bytes).map(b => f"${b & 0xff}%02x").mkString
    else fresh

  private def persist[O](
    bucket:             String,
    path:               String,
    objectProtection:   ObjectProtection[O],
    realETag:           String,
    cachedETagOverride: Option[String],
  )(
    implicit
    encoder: Encoder[O],
  ): Either[UploadError, ObjectWithETag[O]] = {
    val existing = Option(store.get((bucket, path)))
    objectProtection match {
      case NoOverwriteExistingObject(_) if existing.isDefined =>
        // As every backend reports a lost create race: the recoverable "already exists" error.
        NonOverwriteFileExistsError(
          new IllegalStateException(s"PreconditionFailed: $bucket/$path already exists"),
          path,
        ).asLeft
      case ObjectWithETag(_, expected) =>
        existing match {
          case None =>
            FileCreateError(
              new IllegalStateException(s"If-Match on missing key $bucket/$path"),
              s"If-Match on missing $path",
            ).asLeft
          case Some(blob) if blob.eTag != expected =>
            FileCreateError(
              new IllegalStateException(
                s"eTag mismatch on $bucket/$path: expected $expected, found ${blob.eTag}",
              ),
              s"If-Match failed for $path",
            ).asLeft
          case _ =>
            val bytes = encodeBytes(objectProtection.wrappedObject)
            val eTag  = eTagFor(bytes, realETag)
            store.put((bucket, path), StoredBlob(bytes, eTag, Instant.now()))
            Right(ObjectWithETag(objectProtection.wrappedObject, cachedETagOverride.getOrElse(eTag)))
        }
      case _ =>
        val bytes = encodeBytes(objectProtection.wrappedObject)
        val eTag  = eTagFor(bytes, realETag)
        store.put((bucket, path), StoredBlob(bytes, eTag, Instant.now()))
        Right(ObjectWithETag(objectProtection.wrappedObject, cachedETagOverride.getOrElse(eTag)))
    }
  }

  private def encodeBytes[O](o: O)(implicit encoder: Encoder[O]): Array[Byte] =
    o.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)

  private def takeFirstHook[A <: Hook](pf: PartialFunction[Hook, A]): Option[A] = {
    val takenRef = new AtomicReference[Option[A]](None)
    val _ = hooks.updateAndGet { current =>
      val idx = current.indexWhere(pf.isDefinedAt)
      if (idx < 0) current
      else {
        takenRef.set(Some(pf(current(idx))))
        current.patch(idx, Nil, 1)
      }
    }
    takenRef.get()
  }
}

object InMemoryStorageInterface {

  /** A persisted blob: opaque bytes + an opaque eTag + a last-modified timestamp. */
  final case class StoredBlob(bytes: Array[Byte], eTag: String, lastModified: Instant)

  /**
   * Crash-injection hook. Each hook is one-shot: registered with `arm`, consumed on the
   * first matching operation, after which it is removed from the queue.
   */
  sealed trait Hook {
    def asUploadError(bucket: String, path: String): UploadError =
      FileCreateError(new IllegalStateException(s"injected upload failure: $bucket/$path"), path)

    def asMoveError(oldPath: String, newPath: String): FileMoveError =
      FileMoveError(new IllegalStateException(s"injected move failure: $oldPath -> $newPath"), oldPath, newPath)

    def asDeleteError(bucket: String, path: String): FileDeleteError =
      FileDeleteError(new IllegalStateException(s"injected delete failure: $bucket/$path"), path)
  }

  /** Fail the next write to `(bucket, path)` with `err` (or a default if `None`). */
  final case class FailWriteAt(bucket: String, path: String, err: Option[UploadError] = None) extends Hook {
    def matches(b:                String, p: String): Boolean     = b == bucket && p == path
    override def asUploadError(b: String, p: String): UploadError = err.getOrElse(super.asUploadError(b, p))
  }

  /** Fail the next mvFile sourced from `(bucket, path)` with `err`. */
  final case class FailMoveAt(bucket: String, path: String, err: Option[FileMoveError] = None) extends Hook {
    def matches(b:                    String, p:       String): Boolean = b == bucket && p == path
    override def asMoveError(oldPath: String, newPath: String): FileMoveError =
      err.getOrElse(super.asMoveError(oldPath, newPath))
  }

  /** Fail the next deleteFile/deleteFiles entry at `(bucket, path)` with `err`. */
  /**
   * Run `action` on the next delete of `(bucket, path)`, before the delete (or an armed
   * `FailDeleteAt`) is applied: a deterministic point to land another step while a delete is in
   * flight.
   */
  final case class OnDeleteAt(bucket: String, path: String, action: () => Unit) extends Hook {
    def matches(b: String, p: String): Boolean = b == bucket && p == path
  }

  final case class FailDeleteAt(bucket: String, path: String, err: Option[FileDeleteError] = None) extends Hook {
    def matches(b:                String, p: String): Boolean         = b == bucket && p == path
    override def asDeleteError(b: String, p: String): FileDeleteError = err.getOrElse(super.asDeleteError(b, p))
  }

  /**
   * Persist the next conditional write to `(bucket, path)` with a bogus eTag so the next
   * conditional write/move issued by the caller fails CAS. The persisted bytes are correct.
   */
  final case class CorruptETag(bucket: String, path: String) extends Hook {
    def matches(b: String, p: String): Boolean = b == bucket && p == path
  }

  /**
   * Run `action` on the next content read of `(bucket, path)`, before the read sees the store: a
   * deterministic point to interleave another thread's step (e.g. a concurrent fence or close)
   * between a caller's check and its use of what it read.
   */
  final case class OnReadAt(bucket: String, path: String, action: () => Unit) extends Hook {
    def matches(b: String, p: String): Boolean = b == bucket && p == path
  }

  /**
   * Run `action` on the next content read of `(bucket, path)`, after the read has taken the content
   * and before the caller sees it: the caller then acts on what the store held before `action`, as
   * when another thread's step lands between a read and the decision taken on it.
   */
  final case class AfterReadAt(bucket: String, path: String, action: () => Unit) extends Hook {
    def matches(b: String, p: String): Boolean = b == bucket && p == path
  }

  /**
   * Run `action` on the next `writeBlobToFile` to `(bucket, path)` whose object satisfies `onlyIf`,
   * before the write is applied: a deterministic point to land another instance's write between a
   * caller's read and its conditional write. `onlyIf` keeps the action's own writes to the same
   * path from consuming the hook.
   */
  final case class OnWriteAt(
    bucket: String,
    path:   String,
    action: () => Unit,
    onlyIf: Any => Boolean = _ => true,
  ) extends Hook {
    def matches(b: String, p: String): Boolean = b == bucket && p == path
  }

  /**
   * Apply the next `writeBlobToFile` to `(bucket, path)` (with its normal conditional-write
   * semantics), then report a failure to the caller anyway: the write landed but its response
   * was lost, e.g. a timeout whose SDK retry then fails its precondition against the object the
   * first attempt already changed.
   */
  final case class LoseWriteResponseAt(bucket: String, path: String) extends Hook {
    def matches(b:                String, p: String): Boolean = b == bucket && p == path
    override def asUploadError(b: String, p: String): UploadError =
      FileCreateError(new IllegalStateException(s"injected lost write response: $b/$p"), p)
  }

  /**
   * On every uploadFile to a path matching `bucket`, increment a global counter. Once the
   * counter exceeds `threshold`, the next matching upload "succeeds" at the API level but
   * the bytes are not persisted. The hook is consumed when it fires.
   */
  final case class DropUploadAfterCount(bucket: String, threshold: Int) extends Hook {
    def matches(b: String, _p: String): Boolean = b == bucket
  }
}

/** File metadata for the in-memory fake. */
final case class FakeFileMetadata(file: String, lastModified: Instant) extends FileMetadata
