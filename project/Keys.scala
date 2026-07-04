import sbt.*

// Task keys placed in the meta-build (project/) so `@transient` is honored by the sbt macro;
// sbt 2's cached-task detector otherwise rejects Seq[File] outputs on keys declared inside
// build.sbt itself.
object WvletBuildKeys:

  @transient
  lazy val stdlibGen = taskKey[Seq[File]]("Generate stdlib.scala from wvlet-stdlib .wv files")

  @transient
  lazy val sampleQueryGen = taskKey[Seq[File]]("Generate SampleQuery.scala for the playground")
