grammar FlowLang;

tokens {
    DELIMITER
}


statements:
    statement (statement)*
    ;

statement:
    schemaDef
    | typeDef
    | query
    ;

schemaDef:
    SCHEMA identifier COLON
      (schemaElement (COMMA schemaElement)* COMMA?)?
    END?
    ;

schemaElement:
    identifier COLON identifier
    ;

typeDef:
    TYPE identifier '(' paramList ')' COLON
      typeDefElem*
    END?
    ;

paramList:
    param (COMMA param)* COMMA?
    ;

param:
    identifier COLON identifier
    ;

typeDefElem:
    DEF identifier (COLON identifier)? EQ expr
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier:
    IDENTIFIER
    | BACKQUOTED_IDENTIFIER
    ;

expr:
    NULL             # nullLiteral
    | number         # numberLiteral
    | str            # stringLiteral
    | function       # functionCall
    | '[' exprList ']' # arrayConstructor
    | expr '[' expr ']' # arrayAccess
    | identifier # columnReference
    ;

booleanExpression
    : '!' booleanExpression                                        #logicalNot
    | valueExpression                                                   #booleanDeafault
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

valueExpression
    : expr
    ;




function:
    identifier '(' exprList ')'
    ;

exprList:
    expr (COMMA expr)* COMMA?
    ;


arrayExpr:
    '[' exprList ']'
    ;


// Can't use string as a rule name because it's Java keyword
str
    : STRING                                #basicStringLiteral
    | UNICODE_STRING       #unicodeStringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;


number
    : DECIMAL_VALUE  #decimalLiteral
    | DOUBLE_VALUE   #doubleLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

query:
   flowerExpr
   ;

flowerExpr:
    forExpr
    (WHERE booleanExpression)?
    selectExpr?
    ;

forExpr:
    FOR identifier IN expr
    | FROM expr
    ;

selectExpr:
    SELECT (AS identifier)? selectItemList
    ;

selectItemList:
    selectItem (COMMA selectItem)* COMMA?
    ;

selectItem:
    (identifier COLON)? expr
    ;


COLON: ':';
COMMA: ',';

AS: 'AS';
DEF: 'DEF';
END: 'END';
FOR: 'FOR';
FROM: 'FROM';
IN: 'IN';
SCHEMA: 'SCHEMA';
SELECT: 'SELECT';
TYPE: 'TYPE';
WHERE: 'WHERE';

NULL: 'NULL';
AND: 'AND';
OR: 'OR';
TRUE: 'TRUE';
FALSE: 'FALSE';

EQ  : '=';
NEQ : '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : (DIGIT | '_') +
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;


fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
