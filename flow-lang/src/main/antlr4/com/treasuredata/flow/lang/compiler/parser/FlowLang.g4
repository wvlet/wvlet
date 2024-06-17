parser grammar FlowLang;

tokens {
    DELIMITER
}

statements:
    packageDef?
    singleStatement+
    ;

packageDef:
    PACKAGE identifier
    ;

singleStatement
    : importStatement
    | query
    | typeAlias
    | typeDef
    | functionDef
    | tableDef
    | subscribeDef
    | moduleDef
    | test
    ;

importStatement:
    IMPORT importExpr (COMMA importExpr)*
    ;

importExpr
    : importRef (FROM fromRef=str)?
    ;

importRef
    : qualifiedName (DOT ASTERISK)?
    | qualifiedName AS alias=identifier
    ;


query:
    FROM relation
    queryBlock*
    // As most of the relational database has no notion of sorted relation in the middle of query stages
    // limit the usage of ORDER BY to the end of the query
    (ORDER BY sortItem (COMMA sortItem)* COMMA?)?
    (LIMIT limit=INTEGER_VALUE)?
    ;

queryBlock
    : join                        #joinRelation
    | GROUP BY groupByItemList    #aggregateRelation
    | WHERE booleanExpression     #filterRelation
    | transformExpr               #transformRelation
    | selectExpr                  #projectRelation
    | LIMIT limit=INTEGER_VALUE   #limitRelation
    ;

selectExpr:
    SELECT (AS identifier)? selectItemList?
    ;

selectItemList:
    selectItem (COMMA selectItem)* COMMA?
    ;

selectItem
    : (identifier COLON)? expression   #selectSingle
    | (qualifiedName COLON)? ASTERISK  #selectAll
    ;

transformExpr:
    TRANSFORM selectItemList
    ;

test:
    TEST COLON testItem*
    ;

testItem:
    primaryExpression
    ;

subscribeDef:
    SUBSCRIBE src=identifier AS name=identifier COLON
    (WATERMARK_COLUMN COLON watermarkColumn=identifier)?
    (WINDOW_SIZE COLON windowSize=str)?
    subscribeParam*
    END?
    ;

subscribeParam:
    identifier COLON primaryExpression
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
    : LPAREN identifier (DOT identifier)* RPAREN
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | LBRACE query RBRACE                                             #subqueryRelation
    | LPAREN relation RPAREN                                          #parenthesizedRelation
//    | UNNEST LPAREN primaryExpression (COMMA primaryExpression)* RPAREN (WITH ORDINALITY)?  #unnest
//    | LATERAL LPAREN query RPAREN                                           #lateral
    | str                                                             #fileScan
    ;

join
    : joinType? JOIN relation joinCriteria
    | CROSS JOIN relation
    ;

joinType
    : INNER
    | LEFT
    | RIGHT
    | FULL
    ;

joinCriteria
    : ON booleanExpression
    | ON LPAREN identifier (COMMA identifier)* RPAREN
    ;


typeAlias:
    TYPE alias=identifier EQ sourceType=identifier
    ;

typeDef:
    TYPE identifier (LPAREN paramList RPAREN)?
    (COLON typeElem*)?
    END?
    ;

typeElem
    : DEF identifier (DOT identifier)? EQ primaryExpression   #typeDefDef
    | columnName=identifier COLON typeName=identifier         #typeValDef
    ;


functionDef:
    DEF name=identifier (LPAREN paramList RPAREN)? (COLON resultTypeName=identifier)? EQ
        body=expression
    END?
    ;

tableDef:
    TABLE identifier COLON
    tableParam*
    END
    ;

tableParam:
    identifier COLON primaryExpression
    ;


moduleDef:
    MODULE identifier COLON
      typeElem*
    END?
    ;

paramList:
    param (COMMA param)* COMMA?
    ;

param:
    identifier COLON identifier
    ;


qualifiedName
    : identifier (DOT identifier)*
    ;

identifier:
    IDENTIFIER                # unquotedIdentifier
    | BACKQUOTED_IDENTIFIER   # backQuotedIdentifier
    // A workaround for using reserved words (join, select, etc.) as function names
    | reserved                # reservedWordIdentifier
    ;


reserved
    : SELECT | JOIN | TRANSFORM | TYPE
    ;


sortItem
    : expression ordering=(ASC | DESC)? // (NULLS nullOrdering=(FIRST | LAST))?
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : (EXCLAMATION | NOT) booleanExpression                        #logicalNot
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
    | number                                                                           #numericLiteral
    | booleanValue                                                                     #booleanLiteral
    | str                                                                              #stringLiteral
    | BINARY_LITERAL                                                                   #binaryLiteral
    | QUESTION                                                                         #parameter
    | primaryExpression LPAREN (valueExpression (COMMA valueExpression)*)? RPAREN      #functionCall
    | primaryExpression qualifiedName valueExpression                                  #functionCallApply
    | LPAREN query RPAREN                                                              #subqueryExpression
    | LBRACKET (expression (COMMA expression)*)? RBRACKET                              #arrayConstructor
    // | value=primaryExpression LBRACKET index=valueExpression RBRACKET               #subscript
    | qualifiedName                                                                    #columnReference
    | base=primaryExpression DOT next=primaryExpression                                #dereference
    | LPAREN expression RPAREN                                                         #parenthesizedExpression
    | UNDERSCORE                                                                       #contextRef
    ;


exprList:
    primaryExpression (COMMA primaryExpression)* COMMA?
    ;


arrayExpr:
    LBRACKET exprList RBRACKET
    ;


// Can't use string as a rule name because it's Java keyword
str
    : TRIPLE_QUOTED_STRING                              #tripleQuoteStringLiteral
    | (SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING)     #basicStringLiteral
    //  | UNICODE_STRING                                    #unicodeStringLiteral
    ;

comparisonOperator
    : EQ
    | IS NOT?
    | NEQ
    | LT
    | LTE
    | GT
    | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;


number
    : DECIMAL_VALUE  #decimalLiteral
    | DOUBLE_VALUE   #doubleLiteral
    | INTEGER_VALUE  #integerLiteral
    ;



COLON: ':';
COMMA: ',';
DOT: '.';

AS: 'as';
DEF: 'def';
END: 'end';
FOR: 'for';
FROM: 'from';
IN: 'in';
IS: 'is';
ON: 'on';
MODULE: 'module';
SELECT: 'select';
TRANSFORM: 'transform';
GROUP: 'group';
BY: 'by';
ORDER: 'order';
LIMIT: 'limit';
TYPE: 'type';
WHERE: 'where';
TABLE: 'table';
SUBSCRIBE: 'subscribe';
WATERMARK_COLUMN: 'watermark_column';
WINDOW_SIZE: 'window_size';

PACKAGE: 'package';
IMPORT: 'import';
TEST: 'test';

ASC: 'asc';
DESC: 'desc';

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
RIGHT: 'right';


NULL: 'null';
NOT: 'not';
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

EXCLAMATION: '!';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';

QUESTION: '?';
UNDERSCORE: '_';

LPAREN: '(';
RPAREN: ')';
LBRACKET: '[';
RBRACKET: ']';
LBRACE: '{';
RBRACE: '}';

SINGLE_QUOTED_STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;


DOUBLE_QUOTED_STRING
    : '"' ( ~'"' | '""' )* '"'
    ;


TRIPLE_QUOTED_STRING
    : '"""' .*? '"""'
    ;

//UNICODE_STRING
//    : 'u&\'' ( ~'\'' | '\'\'' )* '\''
//    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'x\'' (~'\'')* '\''
    ;

// Allow underscroe like 100_000_000 for readability
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
