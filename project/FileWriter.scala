import sbt.File
import sbt.Project
import sbt.io.IO

class FileWriter(projects: Seq[Project]) {
  def generate(file: File): Seq[File] = {
    val contents = projects
      .map(s => s""""${s.id}"""")
      .mkString(",")

    IO.write(file, s"[$contents]")
    Seq(file)
  }
}
