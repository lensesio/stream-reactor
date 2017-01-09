package com.datamountaineer.streamreactor.connect.ftp

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