package wvlet.lang.compiler.parser

import TokenType.*

enum SqlToken(val tokenType: TokenType, val str: String):
  import SqlToken.*
  def isIdentifier: Boolean      = tokenType == Identifier || isNonReservedKeyword
  def isLiteral: Boolean         = tokenType == Literal
  def isKeyword: Boolean         = tokenType == Keyword
  def isReservedKeyword: Boolean = isKeyword && !SqlToken.nonReservedKeywords.contains(this)

  def isNonReservedKeyword: Boolean = isKeyword && SqlToken.nonReservedKeywords.contains(this)

  def isOperator: Boolean = tokenType == Op

  def isUpdateStart: Boolean    = updateStartTokens.contains(this)
  def isQueryStart: Boolean     = queryStartTokens.contains(this)
  def isQueryDelimiter: Boolean = queryDelimiters.contains(this)

  def isStringLiteral: Boolean = stringLiterals.contains(this)

  // special tokens
  case EMPTY      extends SqlToken(Control, "<empty>")
  case ERROR      extends SqlToken(Control, "<erroneous token>")
  case EOF        extends SqlToken(Control, "<eof>")
  case NEWLINE    extends SqlToken(Control, "<newline>")
  case WHITESPACE extends SqlToken(Control, "<whitespace>")

  // doc or comments
  case COMMENT     extends SqlToken(Doc, "<comment>")
  case DOC_COMMENT extends SqlToken(Doc, "<doc comment>")

  // Literals
  case INTEGER_LITERAL     extends SqlToken(Literal, "<integer literal>")
  case DECIMAL_LITERAL     extends SqlToken(Literal, "<decimal literal>")
  case EXP_LITERAL         extends SqlToken(Literal, "<exp literal>")
  case LONG_LITERAL        extends SqlToken(Literal, "<long literal>")
  case FLOAT_LITERAL       extends SqlToken(Literal, "<float literal>")
  case DOUBLE_LITERAL      extends SqlToken(Literal, "<double literal>")
  case SINGLE_QUOTE_STRING extends SqlToken(Literal, "<'string'>")
  case DOUBLE_QUOTE_STRING extends SqlToken(Literal, "<\"string\">")
  case TRIPLE_QUOTE_STRING extends SqlToken(Literal, "<\"\"\"string\"\"\">")

  // literal keywords
  case NULL  extends SqlToken(Keyword, "null")
  case TRUE  extends SqlToken(Keyword, "true")
  case FALSE extends SqlToken(Keyword, "false")

  // Identifiers
  case IDENTIFIER extends SqlToken(Identifier, "<identifier>")
  // Identifier wrapped in double quotes "...."
  case DOUBLE_QUOTED_IDENTIFIER extends SqlToken(Identifier, "<doublequoted identifier>")
  case BACK_QUOTED_IDENTIFIER   extends SqlToken(Identifier, "<backquoted identifier>")

  case SINGLE_QUOTE extends SqlToken(Quote, "'")
  case DOUBLE_QUOTE extends SqlToken(Quote, "\"")
  case BACK_QUOTE   extends SqlToken(Quote, "`")

  // Parentheses
  case L_PAREN   extends SqlToken(Op, "(")
  case R_PAREN   extends SqlToken(Op, ")")
  case L_BRACE   extends SqlToken(Op, "{")
  case R_BRACE   extends SqlToken(Op, "}")
  case L_BRACKET extends SqlToken(Op, "[")
  case R_BRACKET extends SqlToken(Op, "]")

  // Special symbols
  case COLON        extends SqlToken(Op, ":")
  case DOUBLE_COLON extends SqlToken(Op, "::")
  case SEMICOLON    extends SqlToken(Op, ";")
  case COMMA        extends SqlToken(Op, ",")
  case DOT          extends SqlToken(Op, ".")
  case UNDERSCORE   extends SqlToken(Op, "_")
  case AT_SYMBOL    extends SqlToken(Op, "@")
  case AT           extends SqlToken(Keyword, "at")
  case DOLLAR       extends SqlToken(Op, "$")
  case STAR         extends SqlToken(Op, "*")
  case QUESTION     extends SqlToken(Op, "?")

  case R_ARROW extends SqlToken(Op, "->")

  // Special keywords
  case EQ   extends SqlToken(Op, "=")
  case NEQ  extends SqlToken(Op, "!=")
  case NEQ2 extends SqlToken(Op, "<>")
  case LT   extends SqlToken(Op, "<")
  case GT   extends SqlToken(Op, ">")
  case LTEQ extends SqlToken(Op, "<=")
  case GTEQ extends SqlToken(Op, ">=")

  case PLUS    extends SqlToken(Op, "+")
  case MINUS   extends SqlToken(Op, "-")
  case DIV     extends SqlToken(Op, "/")
  case DIV_INT extends SqlToken(Op, "//")
  case MOD     extends SqlToken(Op, "%")

  case EXCLAMATION extends SqlToken(Op, "!")

  case AMP  extends SqlToken(Op, "&")
  case PIPE extends SqlToken(Op, "|")

  // query keywords
  case SELECT      extends SqlToken(Keyword, "select")
  case FROM        extends SqlToken(Keyword, "from")
  case WHERE       extends SqlToken(Keyword, "where")
  case GROUP       extends SqlToken(Keyword, "group")
  case BY          extends SqlToken(Keyword, "by")
  case GROUPING    extends SqlToken(Keyword, "grouping")
  case SETS        extends SqlToken(Keyword, "sets")
  case CUBE        extends SqlToken(Keyword, "cube")
  case ROLLUP      extends SqlToken(Keyword, "rollup")
  case ORDER       extends SqlToken(Keyword, "order")
  case ASC         extends SqlToken(Keyword, "asc")
  case DESC        extends SqlToken(Keyword, "desc")
  case LIMIT       extends SqlToken(Keyword, "limit")
  case OFFSET      extends SqlToken(Keyword, "offset")
  case FETCH       extends SqlToken(Keyword, "fetch")
  case FIRST       extends SqlToken(Keyword, "first")
  case NEXT        extends SqlToken(Keyword, "next")
  case ROW         extends SqlToken(Keyword, "row")
  case ROWS        extends SqlToken(Keyword, "rows")
  case ONLY        extends SqlToken(Keyword, "only")
  case AS          extends SqlToken(Keyword, "as")
  case ASOF        extends SqlToken(Keyword, "asof")
  case JOIN        extends SqlToken(Keyword, "join")
  case ON          extends SqlToken(Keyword, "on")
  case LEFT        extends SqlToken(Keyword, "left")
  case RIGHT       extends SqlToken(Keyword, "right")
  case OUTER       extends SqlToken(Keyword, "outer")
  case INNER       extends SqlToken(Keyword, "inner")
  case FULL        extends SqlToken(Keyword, "full")
  case CROSS       extends SqlToken(Keyword, "cross")
  case NATURAL     extends SqlToken(Keyword, "natural")
  case USING       extends SqlToken(Keyword, "using")
  case WINDOW      extends SqlToken(Keyword, "window")
  case OVER        extends SqlToken(Keyword, "over")
  case QUALIFY     extends SqlToken(Keyword, "qualify")
  case LATERAL     extends SqlToken(Keyword, "lateral")
  case SPECIFIC    extends SqlToken(Keyword, "specific")
  case UNNEST      extends SqlToken(Keyword, "unnest")
  case ORDINALITY  extends SqlToken(Keyword, "ordinality")
  case TABLESAMPLE extends SqlToken(Keyword, "tablesample")
  case BERNOULLI   extends SqlToken(Keyword, "bernoulli")
  case SAMPLE      extends SqlToken(Keyword, "sample")
  case EXPLODE     extends SqlToken(Keyword, "explode")
  case PERCENT     extends SqlToken(Keyword, "percent")
  case RESERVOIR   extends SqlToken(Keyword, "reservoir")

  // Hive-specific keywords
  case CLUSTER    extends SqlToken(Keyword, "cluster")
  case DISTRIBUTE extends SqlToken(Keyword, "distribute")
  case SORT       extends SqlToken(Keyword, "sort")

  case ALL      extends SqlToken(Keyword, "all")
  case DISTINCT extends SqlToken(Keyword, "distinct")
  case VALUE    extends SqlToken(Keyword, "value")
  case VALUES   extends SqlToken(Keyword, "values")

  case CAST     extends SqlToken(Keyword, "cast")
  case TRY_CAST extends SqlToken(Keyword, "try_cast")
  case TRIM     extends SqlToken(Keyword, "trim")
  case EXTRACT  extends SqlToken(Keyword, "extract")
  case LEADING  extends SqlToken(Keyword, "leading")
  case TRAILING extends SqlToken(Keyword, "trailing")
  case BOTH     extends SqlToken(Keyword, "both")

  // window spec
  case PARTITION extends SqlToken(Keyword, "partition")
  case RANGE     extends SqlToken(Keyword, "range")
  case PRECEDING extends SqlToken(Keyword, "preceding")
  case FOLLOWING extends SqlToken(Keyword, "following")
  case CURRENT   extends SqlToken(Keyword, "current")
  case UNBOUNDED extends SqlToken(Keyword, "unbounded")
  case EXCLUDE   extends SqlToken(Keyword, "exclude")
  case NO        extends SqlToken(Keyword, "no")
  case OTHERS    extends SqlToken(Keyword, "others")
  case TIES      extends SqlToken(Keyword, "ties")

  // pivot
  case PIVOT     extends SqlToken(Keyword, "pivot")
  case UNPIVOT   extends SqlToken(Keyword, "unpivot")
  case INCLUDING extends SqlToken(Keyword, "including")
  case EXCLUDING extends SqlToken(Keyword, "excluding")

  // set ops
  case UNION     extends SqlToken(Keyword, "union")
  case EXCEPT    extends SqlToken(Keyword, "except")
  case INTERSECT extends SqlToken(Keyword, "intersect")

  // condition
  case CASE      extends SqlToken(Keyword, "case")
  case WHEN      extends SqlToken(Keyword, "when")
  case THEN      extends SqlToken(Keyword, "then")
  case ELSE      extends SqlToken(Keyword, "else")
  case END       extends SqlToken(Keyword, "end")
  case WITH      extends SqlToken(Keyword, "with")
  case WITHOUT   extends SqlToken(Keyword, "without")
  case RECURSIVE extends SqlToken(Keyword, "recursive")
  case HAVING    extends SqlToken(Keyword, "having")
  case FILTER    extends SqlToken(Keyword, "filter")

  // DDL keywords
  case ALTER          extends SqlToken(Keyword, "alter")
  case SYSTEM         extends SqlToken(Keyword, "system")
  case SESSION        extends SqlToken(Keyword, "session")
  case RESET          extends SqlToken(Keyword, "reset")
  case EXPLAIN        extends SqlToken(Keyword, "explain")
  case PLAN           extends SqlToken(Keyword, "plan")
  case IMPLEMENTATION extends SqlToken(Keyword, "implementation")
  case FOR            extends SqlToken(Keyword, "for")
  case DESCRIBE       extends SqlToken(Keyword, "describe")
  case INPUT          extends SqlToken(Keyword, "input")
  case OUTPUT         extends SqlToken(Keyword, "output")

  // DDL entity types (non-reserved so they can be used as table names)
  case CATALOG   extends SqlToken(Keyword, "catalog")
  case DATABASE  extends SqlToken(Keyword, "database")
  case SCHEMA    extends SqlToken(Keyword, "schema")
  case TABLE     extends SqlToken(Keyword, "table")
  case VIEW      extends SqlToken(Keyword, "view")
  case STATEMENT extends SqlToken(Keyword, "statement")

  case INSERT     extends SqlToken(Keyword, "insert")
  case UPSERT     extends SqlToken(Keyword, "upsert")
  case INTO       extends SqlToken(Keyword, "into")
  case OVERWRITE  extends SqlToken(Keyword, "overwrite")
  case MERGE      extends SqlToken(Keyword, "merge")
  case MATCHED    extends SqlToken(Keyword, "matched")
  case UPDATE     extends SqlToken(Keyword, "update")
  case SET        extends SqlToken(Keyword, "set")
  case DELETE     extends SqlToken(Keyword, "delete")
  case CREATE     extends SqlToken(Keyword, "create")
  case DROP       extends SqlToken(Keyword, "drop")
  case PREPARE    extends SqlToken(Keyword, "prepare")
  case EXECUTE    extends SqlToken(Keyword, "execute")
  case DEALLOCATE extends SqlToken(Keyword, "deallocate")
  // If needs to be a token for 'create table if not exists ...' syntax
  case IF      extends SqlToken(Keyword, "if")
  case REPLACE extends SqlToken(Keyword, "replace")

  case ADD        extends SqlToken(Keyword, "add")
  case COLUMN     extends SqlToken(Keyword, "column")
  case PRIMARY    extends SqlToken(Keyword, "primary")
  case KEY        extends SqlToken(Keyword, "key")
  case UNIQUE     extends SqlToken(Keyword, "unique")
  case INDEX      extends SqlToken(Keyword, "index")
  case CONSTRAINT extends SqlToken(Keyword, "constraint")
  case FOREIGN    extends SqlToken(Keyword, "foreign")
  case REFERENCES extends SqlToken(Keyword, "references")
  case DEFAULT    extends SqlToken(Keyword, "default")
  case NULLABLE   extends SqlToken(Keyword, "nullable")

  // commands
  case SHOW      extends SqlToken(Keyword, "show")
  case USE       extends SqlToken(Keyword, "use")
  case FUNCTIONS extends SqlToken(Keyword, "functions")

  // logical expressions
  case NOT     extends SqlToken(Keyword, "not")
  case EXISTS  extends SqlToken(Keyword, "exists")
  case LIKE    extends SqlToken(Keyword, "like")
  case RLIKE   extends SqlToken(Keyword, "rlike")
  case ESCAPE  extends SqlToken(Keyword, "escape")
  case IN      extends SqlToken(Keyword, "in")
  case BETWEEN extends SqlToken(Keyword, "between")
  case AND     extends SqlToken(Keyword, "and")
  case OR      extends SqlToken(Keyword, "or")
  case IS      extends SqlToken(Keyword, "is")
  case NULLS   extends SqlToken(Keyword, "nulls")
  case IGNORE  extends SqlToken(Keyword, "ignore")
  case RESPECT extends SqlToken(Keyword, "respect")
  case LAST    extends SqlToken(Keyword, "last")
  case UNKNOWN extends SqlToken(Keyword, "unknown")
  case CHECK   extends SqlToken(Keyword, "check")

  // literal start keywords
  case MAP       extends SqlToken(Keyword, "map")
  case ARRAY     extends SqlToken(Keyword, "array")
  case DATE      extends SqlToken(Keyword, "date")
  case TIME      extends SqlToken(Keyword, "time")
  case TIMESTAMP extends SqlToken(Keyword, "timestamp")
  case DECIMAL   extends SqlToken(Keyword, "decimal")
  case JSON      extends SqlToken(Keyword, "json")
  case INTERVAL  extends SqlToken(Keyword, "interval")

  // JSON OBJECT
  case ABSENT extends SqlToken(Keyword, "absent")
  case KEYS   extends SqlToken(Keyword, "keys")

  // For internal
  case TO extends SqlToken(Keyword, "to")

  // For AT TIME ZONE syntax
  case ZONE extends SqlToken(Keyword, "zone")

  // ALTER TABLE specific tokens
  case RENAME        extends SqlToken(Keyword, "rename")
  case TYPE          extends SqlToken(Keyword, "type")
  case AUTHORIZATION extends SqlToken(Keyword, "authorization")
  case PROPERTIES    extends SqlToken(Keyword, "properties")
  case USER          extends SqlToken(Keyword, "user")
  case ROLE          extends SqlToken(Keyword, "role")
  case DATA          extends SqlToken(Keyword, "data")
  case AFTER         extends SqlToken(Keyword, "after")

  // These should not be keyword tokens as it conflicts with function names
  //  case YEAR   extends SqlToken(Keyword, "year")
  //  case MONTH  extends SqlToken(Keyword, "month")
  //  case DAY    extends SqlToken(Keyword, "day")
  //  case HOUR   extends SqlToken(Keyword, "hour")
  //  case MINUTE extends SqlToken(Keyword, "minute")
  //  case SECOND extends SqlToken(Keyword, "second")

end SqlToken

object SqlToken:
  val keywords       = SqlToken.values.filter(_.tokenType == Keyword).toSeq
  val specialSymbols = SqlToken.values.filter(_.tokenType == Op).toSeq

  val literalStartKeywords = Seq(
    SqlToken.NULL,
    SqlToken.TRUE,
    SqlToken.FALSE,
    SqlToken.CASE,
    SqlToken.MAP,
    SqlToken.ARRAY,
    SqlToken.DATE,
    SqlToken.TIME,
    SqlToken.TIMESTAMP,
    SqlToken.DECIMAL,
    SqlToken.JSON,
    SqlToken.INTERVAL,
    SqlToken.CAST,
    SqlToken.TRY_CAST,
    SqlToken.EXTRACT
  )

  // Keywords that can be used as unquoted identifiers
  val nonReservedKeywords = Set(
    SqlToken.IF,      // IF can be used as a function name
    SqlToken.REPLACE, // REPLACE can be used as a function name
    SqlToken.TRIM,    // TRIM can be used as a function name
    SqlToken.FILTER,  // FILTER can be used as a function name (e.g., filter(array, lambda))
    SqlToken.EXPLODE, // EXPLODE can be used as a function name (e.g., Hive's explode)
    SqlToken.LEADING,
    SqlToken.TRAILING,
    SqlToken.BOTH,
    SqlToken.KEY,
    SqlToken.VALUE, // VALUE can be used as a column name
    SqlToken.SYSTEM,
    SqlToken.PRIMARY,
    SqlToken.UNIQUE,
    SqlToken.INDEX,
    SqlToken.COLUMN,
    SqlToken.DEFAULT,
    SqlToken.CONSTRAINT,
    SqlToken.FOREIGN,
    SqlToken.REFERENCES,
    SqlToken.CHECK,
    SqlToken.FIRST,
    SqlToken.LAST,
    SqlToken.ASC,
    SqlToken.DESC,
    SqlToken.NULLS,
    SqlToken.ROW,
    SqlToken.ROWS,
    SqlToken.RANGE,
    SqlToken.PRECEDING,
    SqlToken.FOLLOWING,
    SqlToken.CURRENT,
    SqlToken.EXCLUDE,
    SqlToken.OTHERS,
    SqlToken.TIES,
    SqlToken.NO,
    SqlToken.WITHOUT,
    SqlToken.ORDINALITY,
    SqlToken.SAMPLE,
    SqlToken.PERCENT,
    SqlToken.RESERVOIR,
    SqlToken.NEXT, // NEXT can be used as a column name in Hive
    // DDL entity types - non-reserved so they can be used as table/column names
    SqlToken.CATALOG,
    SqlToken.DATABASE,
    SqlToken.SCHEMA,
    SqlToken.TABLE,
    SqlToken.VIEW,
    SqlToken.STATEMENT,
    SqlToken.FUNCTIONS,
    SqlToken.INPUT,
    SqlToken.OUTPUT,
    // Grouping keywords - can be used as function names
    SqlToken.GROUPING,
    SqlToken.SETS,
    SqlToken.CUBE,
    SqlToken.ROLLUP,
    // Data types - non-reserved so they can be used as column names
    SqlToken.DATE,
    SqlToken.TIME,
    SqlToken.TIMESTAMP,
    SqlToken.DECIMAL,
    SqlToken.MAP, // MAP can be used as a table/column alias
    SqlToken.JSON,
    // JSON object modifiers
    SqlToken.ABSENT,
    SqlToken.KEYS,
    // ALTER TABLE specific tokens - non-reserved
    SqlToken.RENAME,
    SqlToken.TYPE,
    SqlToken.AUTHORIZATION,
    SqlToken.PROPERTIES,
    SqlToken.USER,
    SqlToken.ROLE,
    SqlToken.DATA,
    SqlToken.AFTER,
    // ZONE can be used as a column name in bracket expressions (e.g., map[zone])
    SqlToken.ZONE
  )

  val allKeywordsAndSymbols = keywords ++ literalStartKeywords ++ specialSymbols
  val keywordAndSymbolTable =
    val m = Map.newBuilder[String, SqlToken]
    allKeywordsAndSymbols.foreach {
      case t: SqlToken if t.isKeyword =>
        m += t.str -> t
        // Support upper-case keywords in SQL
        m += t.str.toUpperCase -> t
      case t: SqlToken =>
        m += t.str -> t
    }
    m.result()

  val joinKeywords = Set(
    SqlToken.JOIN,
    SqlToken.LEFT,
    SqlToken.RIGHT,
    SqlToken.OUTER,
    SqlToken.INNER,
    SqlToken.CROSS,
    SqlToken.NATURAL,
    SqlToken.ON
  )

  val queryStartTokens = List(SqlToken.SELECT, SqlToken.VALUES, SqlToken.WITH)
  val updateStartTokens = List(
    SqlToken.INSERT,
    SqlToken.UPSERT,
    SqlToken.UPDATE,
    SqlToken.MERGE,
    SqlToken.DELETE,
    SqlToken.CREATE,
    SqlToken.DROP,
    SqlToken.PREPARE,
    SqlToken.EXECUTE,
    SqlToken.DEALLOCATE
  )

  val queryDelimiters = Set(SqlToken.EOF, SqlToken.R_PAREN, SqlToken.SEMICOLON)
  val stringLiterals = Set(
    SqlToken.SINGLE_QUOTE_STRING,
    SqlToken.DOUBLE_QUOTE_STRING,
    SqlToken.TRIPLE_QUOTE_STRING
  )

  given tokenTypeInfo: TokenTypeInfo[SqlToken] with
    override def empty: SqlToken      = SqlToken.EMPTY
    override def errorToken: SqlToken = SqlToken.ERROR
    override def eofToken: SqlToken   = SqlToken.EOF

    override def identifier: SqlToken                   = SqlToken.IDENTIFIER
    override def findToken(s: String): Option[SqlToken] = SqlToken.keywordAndSymbolTable.get(s)
    override def integerLiteral: SqlToken               = SqlToken.INTEGER_LITERAL
    override def longLiteral: SqlToken                  = SqlToken.LONG_LITERAL
    override def decimalLiteral: SqlToken               = SqlToken.DECIMAL_LITERAL
    override def expLiteral: SqlToken                   = SqlToken.EXP_LITERAL
    override def doubleLiteral: SqlToken                = SqlToken.DOUBLE_LITERAL
    override def floatLiteral: SqlToken                 = SqlToken.FLOAT_LITERAL

    override def commentToken: SqlToken         = SqlToken.COMMENT
    override def docCommentToken: SqlToken      = SqlToken.DOC_COMMENT
    override def singleQuoteString: SqlToken    = SqlToken.SINGLE_QUOTE_STRING
    override def doubleQuoteString: SqlToken    = SqlToken.DOUBLE_QUOTE_STRING
    override def tripleQuoteString: SqlToken    = SqlToken.TRIPLE_QUOTE_STRING
    override def whiteSpace: SqlToken           = SqlToken.WHITESPACE
    override def backQuotedIdentifier: SqlToken = SqlToken.BACK_QUOTED_IDENTIFIER

end SqlToken
