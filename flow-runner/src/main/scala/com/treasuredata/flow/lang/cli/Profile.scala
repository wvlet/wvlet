package com.treasuredata.flow.lang.cli

import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.codec.PrimitiveCodec.AnyCodec
import wvlet.airframe.config.YamlReader
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

import java.io.File
import scala.jdk.CollectionConverters.*

case class Profile(connector: List[Connector])

case class Connector(
    name: String,
    `type`: String,
    user: Option[String] = Some("user"),
    password: Option[String] = None,
    database: String = "default",
    host: Option[String] = None,
    port: Option[Int] = None,
    schema: Option[String] = None
)

object Profile extends LogSupport:

  def getProfile(profile: String): Option[Profile] =
    val configPath = sys.props("user.home") + "/.flow/profiles.yml"
    val configFile = new File(configPath)
    if !configFile.exists() then
      None
    else
      val yamlString = IOUtil.readAsString(configPath)
      // replace environment variables ($xxxx) in the yaml to real env values
      val yamlStringEvaluated = yamlString
        .split("\n")
        .map { line =>
          val envPattern = """\$([A-Za-z0-9_]+)""".r
          envPattern.replaceAllIn(line, m => sys.env.getOrElse(m.group(1), m.matched))
        }
        .mkString("\n")

      val yamlMap =
        new org.yaml.snakeyaml.Yaml()
          .load(yamlStringEvaluated)
          .asInstanceOf[java.util.Map[AnyRef, AnyRef]]
          .asScala
          .toMap
      val msgpack  = YamlReader(yamlMap).toMsgpack
      val codec    = MessageCodec.of[Map[String, Profile]]
      val profiles = codec.fromMsgPack(msgpack)
      profiles.get(profile)

end Profile
