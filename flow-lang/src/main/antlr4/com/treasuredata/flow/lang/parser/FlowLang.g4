grammar FlowLang;

tokens {
    DELIMITER
}


statements:
    singleStatement (singleStatement)*
    ;

singleStatement:
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
    TYPE identifier ('(' paramList ')')? COLON
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
    DEF identifier (COLON identifier)? EQ expression
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier:
    IDENTIFIER # unquotedIdentifier
    | BACKQUOTED_IDENTIFIER # backQuotedIdentifier
    ;

expression:
    NULL             # nullLiteral
    | number         # numberLiteral
    | str            # stringLiteral
    | function       # functionCall
    | '[' exprList ']' # arrayConstructor
    | expression '[' expression ']' # arrayAccess
    | identifier # columnReference
    ;

booleanExpression
    : '!' booleanExpression                                        #logicalNot
    | valueExpression                                                   #booleanDeafault
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

valueExpression
    : expression
    ;




function:
    identifier '(' exprList ')'
    ;

exprList:
    expression (COMMA expression)* COMMA?
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
    FOR identifier IN expression
    | FROM relation
    ;

selectExpr:
    SELECT (AS identifier)? selectItemList
    ;

selectItemList:
    selectItem (COMMA selectItem)* COMMA?
    ;

selectItem:
    (identifier COLON)? expression
    ;


relation
    : left=aliasedRelation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=aliasedRelation
      )                                           #joinRelation
//    | left=relation
//      LATERAL VIEW EXPLODE '(' expression (',' expression)* ')' tableAlias=identifier
//      AS identifier (',' identifier)*  #lateralView
    | aliasedRelation #relationDefault
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | '(' query ')'                                                   #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  #unnest
    | LATERAL '(' query ')'                                           #lateral
    | '(' relation ')'                                                #parenthesizedRelation
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | ON '(' identifier (',' identifier)* ')'
    ;


COLON: ':';
COMMA: ',';

AS: 'AS';
DEF: 'DEF';
END: 'END';
FOR: 'FOR';
FROM: 'FROM';
IN: 'IN';
ON: 'ON';
SCHEMA: 'SCHEMA';
SELECT: 'SELECT';
TYPE: 'TYPE';
WHERE: 'WHERE';

UNNEST: 'UNNEST';
LATERAL: 'LATERAL';
WITH: 'WITH';
ORDINALITY: 'ORDINALITY';

CROSS: 'CROSS';
FULL: 'FULL';
INNER: 'INNER';
JOIN: 'JOIN';
LEFT: 'LEFT';
NATURAL: 'NATURAL';
OUTER: 'OUTER';
RIGHT: 'RIGHT';


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

SELF: 'self';

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
