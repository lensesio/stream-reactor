import sbt.File
import sbt.internal.ProjectMatrix
import sbt.io.IO

class FileWriter(projects: Seq[ProjectMatrix]) {
  def generate(file : File): Seq[File] = {
    val contents = projects
      .flatMap(_.allProjects())
      .map(_._1.id)
      .zipWithIndex
      .map{case (s, i) => s""""$i": "$s""""}
      .mkString(",")

    IO.write(file, "{" + contents + "}")
    Seq(file)
  }
}
