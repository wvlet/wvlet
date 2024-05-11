lexer grammar FlowLangLexer;

tokens {
    DELIMITER
}


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
