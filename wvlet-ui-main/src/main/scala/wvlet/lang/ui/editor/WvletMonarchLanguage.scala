package wvlet.lang.ui.editor

import scala.scalajs.js
import typings.monacoEditor.mod.languages.IMonarchLanguage

val WvletMonarchLanguage: IMonarchLanguage =
  new IMonarchLanguage:
    defaultToken = "invalid"

    // TODO Get these keywords from WvletToken class once WvletToken supports Scala.js
    val keywords = js.Array(
      "test",
      "should",
      "be",
      "contain",
      "debug",
      "def",
      "inline",
      "type",
      "extends",
      "native",
      "show",
      "sample",
      "this",
      "of",
      "in",
      "by",
      "as",
      "to",
      "with",
      "from",
      "agg",
      "select",
      "for",
      "let",
      "where",
      "group",
      "having",
      "order",
      "limit",
      "transform",
      "pivot",
      "distinct",
      "asc",
      "desc",
      "join",
      "on",
      "left",
      "right",
      "full",
      "inner",
      "cross",
      "add",
      "exclude",
      "rename",
      "shift",
      "drop",
      "describe",
      "concat",
      "dedup",
      "intersect",
      "except",
      "all",
      "over",
      "partition",
      "unbounded",
      "preceding",
      "following",
      "current",
      "range",
      "row",
      "run",
      "import",
      "export",
      "package",
      "model",
      "execute",
      "val",
      "if",
      "then",
      "else",
      "end",
      "and",
      "or",
      "not",
      "is",
      "like",
      "save",
      "append",
      "delete",
      "truncate"
    )

    val typeKeywords = js.Array(
      "boolean",
      "double",
      "byte",
      "int",
      "short",
      "char",
      "void",
      "long",
      "float",
      "string",
      "array",
      "map"
    )

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

    val tokenizer =
      new:
        val root: js.Array[js.Array[js.Object]] = js.Array(
          js.Array(
            js.RegExp("[a-z_][a-z_\\.]*"),
            new:
              val cases = js.Dictionary(
                "@keywords"     -> "keyword",
                "@typeKeywords" -> "type.keyword",
                "@default"      -> "identifier"
              )
          ),
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
            js.RegExp("@symbols"),
            new:
              val cases = js.Dictionary("@operators" -> "operator", "@default" -> "")
          ),
          js.Array(
            js.RegExp("\".*?\""),
            new:
              val token = "string"
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
          ),
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
