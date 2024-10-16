package wvlet.lang.ui.editor

import scala.scalajs.js
import typings.monacoEditor.mod.languages.IMonarchLanguage

val WvletMonarchLanguage: IMonarchLanguage =
  new IMonarchLanguage:
    defaultToken = "invalid"

    // TODO Get these keywords from WvletToken class once WvletToken supports Scala.js
    val keywords = Keywords

    val typeKeywords = TypeKeywords

    val operators = js.Array(
      ":",
      ";",
      ",",
      ".",
      "_",
      "@",
      "$",
      "*",
      "?",
      "<-",
      "->",
      "=>",
      "=",
      "!=",
      "<",
      ">",
      "<=",
      ">=",
      "+",
      "-",
      "/",
      "//",
      "%",
      "!",
      "&",
      "|",
      "#"
    )

    val symbols = "[=><!~?:;,\\._@$&|+\\-*/^%]+"

    val numberTokenRules: Rules = js.Array(
      js.Array(
        js.RegExp("[1-9][0-9_]*.[0-9]+"),
        new:
          val token = "number.float"
      ),
      js.Array(
        js.RegExp("0[xX][0-9a-fA-F_]+"),
        new:
          val token = "number"
      ),
      js.Array(
        js.RegExp("[1-9][0-9_]*"),
        new:
          val token = "number"
      )
    )

    val keywordTokenRules: Rules = js.Array(
      js.Array(
        js.RegExp("[a-z_][a-z_0-9\\.]*"),
        new:
          val cases = js.Dictionary(
            "@keywords"     -> "keyword",
            "@typeKeywords" -> "type.keyword",
            "@default"      -> "identifier"
          )
      )
    )

    type Rules = js.Array[js.Array[js.Object]]
    val tokenizer =
      new:
        val root: Rules =
          keywordTokenRules ++
            js.Array[js.Array[js.Object]](
              js.Array(
                js.RegExp("[A-Z][a-zA-Z_0-9][a-zA-Z_\\.0-9]*"),
                new:
                  val token = "type.identifier"
              ),
              js.Array(
                js.RegExp("--.*"),
                new:
                  val token = "comment"
              ),
              js.Array(
                js.RegExp("\"\"\""),
                new:
                  val token = "comment"
                  val next  = "@multilinecomment"
              ),
              js.Array(
                js.RegExp("@symbols"),
                new:
                  val cases = js.Dictionary("@operators" -> "operator", "@default" -> "")
              ),
              js.Array(
                js.RegExp("\""),
                new:
                  val token = "string.quote"
                  val next  = "@string"
              ),
              js.Array(
                js.RegExp("'.*?'"),
                new:
                  val token = "string"
              ),
              js.Array(
                js.RegExp("`.*?`"),
                new:
                  val token = "string.backquoted"
              )
            ) ++ numberTokenRules
        val multilinecomment: Rules = js.Array(
          js.Array(
            js.RegExp("\"\"\""),
            new:
              val token = "comment"
              val next  = "@pop"
          ),
          js.Array(
            js.RegExp("."),
            new:
              val token = "comment"
          )
        )
        val string: Rules = js.Array(
          js.Array(
            js.RegExp("\""),
            new:
              val token = "string.quote"
              val next  = "@pop"
          ),
          js.Array(
            js.RegExp("\\$\\{"),
            new:
              val token = "string.interpolation"
              val next  = "@interpolation"
          ),
          js.Array(
            js.RegExp("."),
            new:
              val token = "string"
          )
        )
        val interpolation: Rules =
          keywordTokenRules ++
            js.Array[js.Array[js.Object]](
              js.Array(
                js.RegExp("}"),
                new:
                  val token = "string.interpolation"
                  val next  = "@pop"
              ),
              js.Array(
                js.RegExp("@symbols"),
                new:
                  val cases = js.Dictionary("@operators" -> "operator", "@default" -> "")
              )
            ) ++ numberTokenRules
