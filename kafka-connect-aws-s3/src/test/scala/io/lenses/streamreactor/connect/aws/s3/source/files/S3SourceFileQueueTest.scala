package io.lenses.streamreactor.connect.aws.s3.source.files

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import org.mockito.InOrder
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3SourceFileQueueTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private val rootLocation = RemoteS3RootLocation("bucket:path")
  private val files        = (0 to 3).map(file => rootLocation.withPath(file.toString + ".json")).toList

  "list" should "cache a batch of results from the beginning" in {

    val (sourceLister: S3SourceLister, order: InOrder, sourceFileQueue: S3SourceFileQueue) = setUp

    // file 0 = 0.json
    sourceFileQueue.next() should be(Right(Some(files(0).atLine(-1))))
    order.verify(sourceLister).listBatch(rootLocation, None, 2)
    order.verifyNoMoreInteractions()
    sourceFileQueue.markFileComplete(files(0))

    // file 1 = 1.json
    sourceFileQueue.next() should be(Right(Some(files(1).atLine(-1))))
    order.verifyNoMoreInteractions()
    sourceFileQueue.markFileComplete(files(1))

    // file 2 = 2.json
    sourceFileQueue.next() should be(Right(Some(files(2).atLine(-1))))
    order.verify(sourceLister).listBatch(rootLocation, Some(files(1)), 2)
    order.verifyNoMoreInteractions()
    sourceFileQueue.markFileComplete(files(2))

    // file 3 = 3.json
    sourceFileQueue.next() should be(Right(Some(files(3).atLine(-1))))
    order.verifyNoMoreInteractions()
    sourceFileQueue.markFileComplete(files(3))

    // No more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(sourceLister).listBatch(rootLocation, Some(files(3)), 2)
    order.verifyNoMoreInteractions()

    // Try again, but still no more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(sourceLister).listBatch(rootLocation, Some(files(3)), 2)
    order.verifyNoMoreInteractions()

  }

  private def setUp = {
    val sourceLister = mock[S3SourceLister]
    when(sourceLister.listBatch(rootLocation, None, 2)).thenReturn(files.slice(0, 2).asRight)
    when(sourceLister.listBatch(rootLocation, Some(files(1)), 2)).thenReturn(files.slice(2, 4).asRight)
    when(sourceLister.listBatch(rootLocation, Some(files(2)), 2)).thenReturn(files.slice(3, 4).asRight)
    when(sourceLister.listBatch(rootLocation, Some(files(3)), 2)).thenReturn(List.empty[RemoteS3PathLocation].asRight)

    val order = inOrder(sourceLister)

    val sourceFileQueue = new S3SourceFileQueue(rootLocation, 2, sourceLister)
    (sourceLister, order, sourceFileQueue)
  }

  "list" should "process the init file before reading additional files" in {

    val (sourceLister: S3SourceLister, order: InOrder, sourceFileQueue: S3SourceFileQueue) = setUp

    sourceFileQueue.init(files(2).atLine(1000))

    // file 2 = 2.json
    sourceFileQueue.next() should be(Right(Some(files(2).atLine(1000))))
    order.verifyNoMoreInteractions()
    sourceFileQueue.markFileComplete(files(2))

    // file 3 = 3.json
    sourceFileQueue.next() should be(Right(Some(files(3).atLine(-1))))
    order.verify(sourceLister).listBatch(rootLocation, Some(files(2)), 2)
    order.verifyNoMoreInteractions()
    sourceFileQueue.markFileComplete(files(3))

    // No more files
    sourceFileQueue.next() should be(Right(None))
    order.verify(sourceLister).listBatch(rootLocation, Some(files(3)), 2)
    order.verifyNoMoreInteractions()

  }

  "markFileComplete" should "return error when no files in list" in {

    val (_: S3SourceLister, _: InOrder, sourceFileQueue: S3SourceFileQueue) = setUp

    sourceFileQueue.markFileComplete(files(2)) should be(Left("No files in queue to mark as complete"))
  }

  "markFileComplete" should "return error when file is not next file" in {

    val (_: S3SourceLister, _: InOrder, sourceFileQueue: S3SourceFileQueue) = setUp

    sourceFileQueue.init(files(1).atLine(100))

    sourceFileQueue.markFileComplete(files(2)) should be(
      Left("File (bucket:2.json) does not match that at head of the queue, which is (bucket:1.json)"),
    )

  }
}
