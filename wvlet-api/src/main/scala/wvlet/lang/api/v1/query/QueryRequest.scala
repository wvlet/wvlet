package wvlet.lang.api.v1.query

import wvlet.airframe.ulid.ULID
import wvlet.lang.api.LinePosition

case class QueryRequest(
    // wvlet query text
    query: String,
    querySelection: QuerySelection = QuerySelection.Single,
    linePosition: LinePosition = LinePosition.NoPosition,
    profile: Option[String] = None,
    schema: Option[String] = None,
    // If true, evaluate test expressions
    isDebugRun: Boolean = true,
    // Limit the max output rows for debug run
    maxRows: Option[Int] = None,
    requestId: ULID = ULID.newULID
):
  def queryLine: String =
    val lines = query.split("\n")
    val line =
      if linePosition.isEmpty then
        0
      else
        linePosition.line - 1
    if line < lines.length then
      lines(line)
    else
      ""
