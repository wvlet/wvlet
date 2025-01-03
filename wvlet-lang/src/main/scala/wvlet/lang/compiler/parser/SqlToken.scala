package wvlet.lang.compiler.parser

import TokenType.*

enum SqlToken(val tokenType: TokenType, val str: String):
  def isIdentifier: Boolean      = tokenType == Identifier
  def isLiteral: Boolean         = tokenType == Literal
  def isReservedKeyword: Boolean = tokenType == Keyword
  def isOperator: Boolean        = tokenType == Op

  // special tokens
  case EMPTY      extends SqlToken(Control, "<empty>")
  case ERROR      extends SqlToken(Control, "<erroneous token>")
  case EOF        extends SqlToken(Control, "<eof>")
  case NEWLINE    extends SqlToken(Control, "<newline>")
  case WHITESPACE extends SqlToken(Control, "<whitespace>")

  // doc or comments
  case COMMENT extends SqlToken(Doc, "<comment>")
  // Literals
  case INTEGER_LITERAL extends SqlToken(Literal, "<integer literal>")
  case DECIMAL_LITERAL extends SqlToken(Literal, "<decimal literal>")
  case EXP_LITERAL     extends SqlToken(Literal, "<exp literal>")
  case LONG_LITERAL    extends SqlToken(Literal, "<long literal>")
  case FLOAT_LITERAL   extends SqlToken(Literal, "<float literal>")
  case DOUBLE_LITERAL  extends SqlToken(Literal, "<double literal>")
  case STRING_LITERAL  extends SqlToken(Literal, "<string literal>")

  // literal keywords
  case NULL  extends SqlToken(Keyword, "null")
  case TRUE  extends SqlToken(Keyword, "true")
  case FALSE extends SqlToken(Keyword, "false")

  // Identifiers
  case IDENTIFIER extends SqlToken(Identifier, "<identifier>")
  // Identifier wrapped in backquotes `....`
  case BACKQUOTED_IDENTIFIER extends SqlToken(Identifier, "<quoted identifier>")

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
  case COLON      extends SqlToken(Op, ":")
  case SEMICOLON  extends SqlToken(Op, ";")
  case COMMA      extends SqlToken(Op, ",")
  case DOT        extends SqlToken(Op, ".")
  case UNDERSCORE extends SqlToken(Op, "_")
  case AT         extends SqlToken(Op, "@")
  case DOLLAR     extends SqlToken(Op, "$")
  case STAR       extends SqlToken(Op, "*")
  case QUESTION   extends SqlToken(Op, "?")

  case R_ARROW extends SqlToken(Op, "->")

  // Special keywords
  case EQ   extends SqlToken(Op, "=")
  case NEQ  extends SqlToken(Op, "!=")
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
  case SELECT   extends SqlToken(Keyword, "select")
  case FROM     extends SqlToken(Keyword, "from")
  case WHERE    extends SqlToken(Keyword, "where")
  case GROUP    extends SqlToken(Keyword, "group")
  case BY       extends SqlToken(Keyword, "by")
  case ORDER    extends SqlToken(Keyword, "order")
  case ASC      extends SqlToken(Keyword, "asc")
  case DESC     extends SqlToken(Keyword, "desc")
  case LIMIT    extends SqlToken(Keyword, "limit")
  case OFFSET   extends SqlToken(Keyword, "offset")
  case FETCH    extends SqlToken(Keyword, "fetch")
  case FIRST    extends SqlToken(Keyword, "first")
  case NEXT     extends SqlToken(Keyword, "next")
  case ROW      extends SqlToken(Keyword, "row")
  case ROWS     extends SqlToken(Keyword, "rows")
  case ONLY     extends SqlToken(Keyword, "only")
  case AS       extends SqlToken(Keyword, "as")
  case JOIN     extends SqlToken(Keyword, "join")
  case ON       extends SqlToken(Keyword, "on")
  case LEFT     extends SqlToken(Keyword, "left")
  case RIGHT    extends SqlToken(Keyword, "right")
  case OUTER    extends SqlToken(Keyword, "outer")
  case INNER    extends SqlToken(Keyword, "inner")
  case CROSS    extends SqlToken(Keyword, "cross")
  case NATURAL  extends SqlToken(Keyword, "natural")
  case USING    extends SqlToken(Keyword, "using")
  case WINDOW   extends SqlToken(Keyword, "window")
  case QUALIFY  extends SqlToken(Keyword, "qualify")
  case LATERAL  extends SqlToken(Keyword, "lateral")
  case SPECIFIC extends SqlToken(Keyword, "specific")

  case ALL      extends SqlToken(Keyword, "all")
  case DISTINCT extends SqlToken(Keyword, "distinct")
  case VALUE    extends SqlToken(Keyword, "value")
  case VALUES   extends SqlToken(Keyword, "values")

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

  // DDL keywords
  case ALTER          extends SqlToken(Keyword, "alter")
  case SYSTEM         extends SqlToken(Keyword, "system")
  case SESSION        extends SqlToken(Keyword, "session")
  case RESET          extends SqlToken(Keyword, "reset")
  case EXPLAIN        extends SqlToken(Keyword, "explain")
  case IMPLEMENTATION extends SqlToken(Keyword, "implementation")
  case FOR            extends SqlToken(Keyword, "for")
  case DESCRIBE       extends SqlToken(Keyword, "describe")
  case CATALOG        extends SqlToken(Keyword, "catalog")
  case SCHEMA         extends SqlToken(Keyword, "schema")
  case TABLE          extends SqlToken(Keyword, "table")
  case STATEMENT      extends SqlToken(Keyword, "statement")

  case INSERT extends SqlToken(Keyword, "insert")
  case UPSERT extends SqlToken(Keyword, "upsert")
  case INTO   extends SqlToken(Keyword, "into")
  case MERGE  extends SqlToken(Keyword, "merge")
  case UPDATE extends SqlToken(Keyword, "update")
  case SET    extends SqlToken(Keyword, "set")
  case DELETE extends SqlToken(Keyword, "delete")
  case CREATE extends SqlToken(Keyword, "create")

  case DROP extends SqlToken(Keyword, "drop")

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

  // logical expressions
  case NOT     extends SqlToken(Keyword, "not")
  case EXISTS  extends SqlToken(Keyword, "exists")
  case LIKE    extends SqlToken(Keyword, "like")
  case IN      extends SqlToken(Keyword, "in")
  case BETWEEN extends SqlToken(Keyword, "between")
  case AND     extends SqlToken(Keyword, "and")
  case OR      extends SqlToken(Keyword, "or")
  case IS      extends SqlToken(Keyword, "is")
  case NULLS   extends SqlToken(Keyword, "nulls")
  case LAST    extends SqlToken(Keyword, "last")
  case UNKNOWN extends SqlToken(Keyword, "unknown")
  case CHECK   extends SqlToken(Keyword, "check")

  // literal start keywords
  case MAP   extends SqlToken(Keyword, "map")
  case ARRAY extends SqlToken(Keyword, "array")
end SqlToken

object SqlToken:
  val keywords       = SqlToken.values.filter(_.tokenType == Keyword).toSeq
  val specialSymbols = SqlToken.values.filter(_.tokenType == Op).toSeq

  val literalStartKeyword = Seq(
    SqlToken.NULL,
    SqlToken.TRUE,
    SqlToken.FALSE,
    SqlToken.CASE,
    SqlToken.MAP,
    SqlToken.ARRAY
  )

  val allKeywordsAndSymbols = keywords ++ literalStartKeyword ++ specialSymbols
  val keywordAndSymbolTable = allKeywordsAndSymbols.map(x => x.str -> x).toMap

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

  val queryDelimiters = Set(SqlToken.EOF, SqlToken.R_PAREN, SqlToken.SEMICOLON)

  given tokenTypeInfo: TokenTypeInfo[SqlToken] with
    override def empty: SqlToken = SqlToken.EMPTY

end SqlToken
