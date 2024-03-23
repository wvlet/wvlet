grammar FlowLang;

tokens {
    DELIMITER
}


statements:
    singleStatement (singleStatement)*
    ;

singleStatement
    : query
    | typeAlias
    | typeDef
    | functionDef
    | tableDef
    | subscribeDef
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

transformExpr:
    TRANSFORM selectItemList
    ;


subscribeDef:
    SUBSCRIBE FROM src=identifier AS name=identifier COLON
    subscribeParam+
    END?
    ;

subscribeParam:
    identifier COLON primaryExpression
    ;


selectItemList:
    selectItem (COMMA selectItem)* COMMA?
    ;

selectItem
    : (identifier COLON)? expression #selectSingle
    | (qualifiedName COLON)? ASTERISK  #selectAll
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
    : '(' identifier ('.' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | '{' query '}'                                                   #subqueryRelation
    | '(' relation ')'                                                #parenthesizedRelation
//    | UNNEST '(' primaryExpression (',' primaryExpression)* ')' (WITH ORDINALITY)?  #unnest
//    | LATERAL '(' query ')'                                           #lateral
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
    | ON '(' identifier (COMMA identifier)* ')'
    ;


typeAlias:
    TYPE alias=identifier EQ sourceType=identifier
    ;

typeDef:
    TYPE identifier ('(' paramList ')')?
    (COLON typeElem*)?
    END?
    ;

typeElem
    : DEF identifier ('.' identifier)? EQ primaryExpression   #typeDefDef
    | columnName=identifier COLON typeName=identifier         #typeValDef
    ;


functionDef:
    DEF name=identifier ('(' paramList ')')? (COLON resultTypeName=identifier)? EQ
        body=expression
    END
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
      (moduleElement (COMMA moduleElement)* COMMA?)?
    END?
    ;

moduleElement:
    DEF identifier (COLON identifier)? EQ primaryExpression
    ;

paramList:
    param (COMMA param)* COMMA?
    ;

param:
    identifier COLON identifier
    ;


qualifiedName
    : identifier ('.' identifier)*
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
    : ('!' | NOT) booleanExpression                                #logicalNot
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
    | '?'                                                                              #parameter
    | primaryExpression '(' (valueExpression (',' valueExpression)*)? ')'              #functionCall
    | '(' query ')'                                                                    #subqueryExpression
    | '[' (expression (',' expression)*)? ']'                                          #arrayConstructor
    // | value=primaryExpression '[' index=valueExpression ']'                            #subscript
    | qualifiedName                                                                    #columnReference
    | base=primaryExpression '.' next=primaryExpression                                #dereference
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
    : (SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING)     #basicStringLiteral
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

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';

SINGLE_QUOTED_STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

DOUBLE_QUOTED_STRING
    : '"' ( ~'"' | '""' )* '"'
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
