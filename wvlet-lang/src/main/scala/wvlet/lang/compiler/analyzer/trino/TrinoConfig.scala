package wvlet.lang.compiler.analyzer.trino

/**
  * Connection parameters for the Trino REST client.
  *
  * Mirrors the JDBC `Properties` shape but stays in plain Scala so it can be used from JVM,
  * Node.js, and Native equally.
  */
case class TrinoConfig(
    host: String,
    port: Int = 8080,
    user: String = "wvlet",
    catalog: Option[String] = None,
    schema: Option[String] = None,
    useHttps: Boolean = false,
    source: String = "wvlet",
    // Basic authentication (user + password). Requires useHttps — Trino coordinators reject
    // password credentials over insecure connections
    password: Option[String] = None,
    // Bearer-token authentication (JWT / OAuth2 access token). Mutually exclusive with password
    token: Option[String] = None
):
  def withHost(host: String): TrinoConfig            = copy(host = host)
  def withPort(port: Int): TrinoConfig               = copy(port = port)
  def withUser(user: String): TrinoConfig            = copy(user = user)
  def withCatalog(catalog: String): TrinoConfig      = copy(catalog = Some(catalog))
  def noCatalog(): TrinoConfig                       = copy(catalog = None)
  def withSchema(schema: String): TrinoConfig        = copy(schema = Some(schema))
  def noSchema(): TrinoConfig                        = copy(schema = None)
  def withHttps(enable: Boolean = true): TrinoConfig = copy(useHttps = enable)
  def withSource(source: String): TrinoConfig        = copy(source = source)
  def withPassword(password: String): TrinoConfig    = copy(password = Some(password))
  def noPassword(): TrinoConfig                      = copy(password = None)
  def withToken(token: String): TrinoConfig          = copy(token = Some(token))
  def noToken(): TrinoConfig                         = copy(token = None)

  def baseUri: String =
    val scheme =
      if useHttps then
        "https"
      else
        "http"
    s"${scheme}://${host}:${port}"

end TrinoConfig
