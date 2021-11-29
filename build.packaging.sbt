import sbtrelease.ReleasePlugin.autoImport.ReleaseKeys.useDefaults
import sbtrelease.ReleaseStateTransformations.{checkSnapshotDependencies, inquireVersions, setReleaseVersion}
import sbtrelease.Version

// Enable sbt-release default behavior. With default enabled, sbt-release will
// set sensible default when getting release version. For example, 0.1-SNAPSHOT
// will be inferred as 0.1.
lazy val useDefaultReleaseOption = { st: State =>
  st.put(useDefaults, true)
}

val publishEnvironment: String = sys.env.getOrElse("ENVIRONMENT", "DEVELOPMENT")

releaseVersion := {
  if (publishEnvironment.equals("STAGING")) {
    { ver => Version(ver).map(_.withoutQualifier.string + "-RC-" + git.gitHeadCommit.value.get).get }
  } else {
    { ver => Version(ver).get.string }
  }
}

releaseProcess := Seq[ReleaseStep](
  useDefaultReleaseOption,
  checkSnapshotDependencies,              // : Ensure no snapshot dependencies
  inquireVersions,                        // : Get the release version
  setReleaseVersion,                      // : Set the release version obtained from inquireVersions step
  releaseStepCommand("assembly")
)
