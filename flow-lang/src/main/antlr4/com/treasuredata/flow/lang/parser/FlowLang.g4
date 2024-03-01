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
    DEF identifier (COLON identifier)? EQ primaryExpression
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier:
    IDENTIFIER # unquotedIdentifier
    | BACKQUOTED_IDENTIFIER # backQuotedIdentifier
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : '!' booleanExpression                                        #logicalNot
    | valueExpression                                              #booleanDeafault
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

valueExpression
    : primaryExpression #valueExpressionDefault
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                     #comparison
    ;

primaryExpression
    : NULL                                                                             #nullLiteral
    | '_'                                                                              #currentReference
    | number                                                                           #numericLiteral
    | booleanValue                                                                     #booleanLiteral
    | str                                                                              #stringLiteral
    | BINARY_LITERAL                                                                   #binaryLiteral
    | '?'                                                                              #parameter
    | qualifiedName '(' ASTERISK ')'                                                   #functionCall
    | qualifiedName '(' ')'                                                            #functionCall
    | qualifiedName '(' (valueExpression (',' valueExpression)*) ')'                   #functionCall
    | '(' query ')'                                                                    #subqueryExpression
    | '[' (expression (',' expression)*)? ']'                                          #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                            #subscript
    | identifier                                                                       #columnReference
    | base=primaryExpression '.' fieldName=primaryExpression                           #dereference
    | '(' expression ')'                                                               #parenthesizedExpression
    ;


exprList:
    primaryExpression (COMMA primaryExpression)* COMMA?
    ;


arrayExpr:
    '[' exprList ']'
    ;


// Can't use string as a rule name because it's Java keyword
str
    : STRING               #basicStringLiteral
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
    FROM relation
    queryBlock*
    ;

queryBlock
    : join                        #joinRelation
    | GROUP BY groupByItemList    #aggregateRelation
    | WHERE booleanExpression     #filterRelation
    | selectExpr                  #projectRelation
    ;

selectExpr:
    SELECT OVERRIDE? (AS identifier)? selectItemList?
    ;

selectItemList:
    selectItem (COMMA selectItem)* COMMA?
    ;

selectItem
    : (identifier COLON)? expression #selectSingle
    | (qualifiedName '.')? ASTERISK  #selectAll
    ;

groupByItemList:
    groupByItem (COMMA groupByItem)* COMMA?
    ;

groupByItem
    : (identifier COLON)? expression
    ;


relation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | '(' query ')'                                                   #subqueryRelation
//    | UNNEST '(' primaryExpression (',' primaryExpression)* ')' (WITH ORDINALITY)?  #unnest
//    | LATERAL '(' query ')'                                           #lateral
    | '(' relation ')'                                                #parenthesizedRelation
    ;

join
    : joinType JOIN relation joinCriteria
    | CROSS JOIN relation
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

AS: 'as';
DEF: 'def';
END: 'end';
FOR: 'for';
FROM: 'from';
IN: 'in';
ON: 'on';
SCHEMA: 'schema';
SELECT: 'select';
GROUP: 'group';
BY: 'by';
TYPE: 'type';
WHERE: 'where';
OVERRIDE: 'override';

UNNEST: 'unnest';
LATERAL: 'lateral';
WITH: 'with';
ORDINALITY: 'ordinality';

CROSS: 'cross';
FULL: 'full';
INNER: 'inner';
JOIN: 'join';
LEFT: 'left';
NATURAL: 'natural';
OUTER: 'outer';
RIGHT: 'right';


NULL: 'null';
AND: 'and';
OR: 'or';
TRUE: 'true';
FALSE: 'false';

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
    : 'u&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'x\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT (DIGIT | '_')*
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
    : 'e' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
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
