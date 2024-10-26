package wvlet.lang.api.v1.query

import wvlet.airframe.ulid.ULID
import wvlet.lang.api.NodeLocation

case class QueryRequest(
    // wvlet query text
    query: String,
    querySelection: QuerySelection = QuerySelection.Single,
    nodeLocation: NodeLocation = NodeLocation.NoLocation,
    profile: Option[String] = None,
    schema: Option[String] = None,
    // If true, evaluate test expressions
    isDebugRun: Boolean = true,
    requestId: ULID = ULID.newULID
)
