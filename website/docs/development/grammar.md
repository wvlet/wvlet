
# Language Grammar

This document describes the grammar of Wvlet language. 

:::warning
As of September 2024, Wvlet is still under active development. The language syntax and grammar structure may change, so use this grammar just as a reference.
:::


```antlr4
packageDef: 'package' qualifiedId statement*

qualifiedId: identifier ('.' identifier)*

identifier  : IDENTIFIER
            | BACKQUOTED_IDENTIFIER
            | '*'
            | reserved  # Necessary to use reserved words as identifiers
IDENTIFIER  : (LETTER | '_') (LETTER | DIGIT | '_')*
BACKQUOTED_IDENTIFIER: '`' (~'`' | '``')+ '`'
// All reserved keywords (TokenType.Keyword)
reserved   : 'from' | 'select' | 'agg' | 'where' | 'group' | 'by' | ...


statements: statement+
statement : importStatement
          | modelDef
          | query ';'?
          | typeDef
          | funDef
          | showCommand queryOp*
          | executeCommand expression

importStatement: 'import' importRef (from str)?
importRef      : qualifiedId ('.' '*')?
               | qualifiedId 'as' identifier

modelDef   : 'model' identifier modelParams? (':' qualifiedId)? '=' modelBody
modelBody  : query 'end'
modelParams: '(' modelParam (',' modelParam)* ')'
modelParam : identifier ':' identifier ('=' expression)?

// top-level query
query      : queryBody update?
queryBody  : querySingle queryBlock*
// A rule for sub queries
querySingle: 'from' relation (',' relation)* ','? queryBlock*
           | 'select' selectItems queryBlock*
           // For parenthesized query, do not continue queryBlock for disambiguation
           | '(' queryBody ')' 

// relation that can be used after 'from', 'join', 'concat' (set operation), etc.:
relation       : relationPrimary ('as' identifier)?
relationPrimary: qualifiedId ('(' functionArg (',' functionArg)* ')')?
               | querySingle
               | str               // file scan
               | strInterpolation  // embedded raw SQL
               | arrayValue
arrayValue     : '[' arrayValue (',' arrayValue)* ','? ']'

queryBlock: joinExpr
          | 'group' 'by' groupByItemList
          | 'where' booleanExpression
          | 'transform' transformExpr
          | 'select' 'distinct'? selectItems
          | 'agg' selectItems
          | 'pivot' 'on' pivotItem (',' pivotItem)*
               ('group' 'by' groupByItemList)?
               ('agg' selectItems)?
          | 'limit' INTEGER_VALUE
          | 'order' 'by' sortItem (',' sortItem)* ','?)?
          | 'add' selectItems
          | 'exclude' identifier ((',' identifier)* ','?)?
          | 'shift' identifier (',' identifier)* ','?
          | 'test' testExpr
          | 'show' identifier
          | 'sample' sampleExpr
          | 'concat' relation
          | ('intersect' | 'except') 'all'? relation
          | 'dedup'
          | 'describe'
          | 'debug' '{' (queryBlock | update)+ '}'

update       : 'save' 'as' updateTarget saveOptions?
             | 'append' 'to' updateTarget
             | 'delete'
updateTarget : qualifiedId | stringLiteral 
saveOptions: 'with' updateOption (',' saveOption)* ','?
saveOption : identifier ':' expression

joinExpr    : 'asof'? joinType? 'join' relation joinCriteria
            | 'cross' 'join' relation
joinType    : 'inner' | 'left' | 'right' | 'full'
joinCriteria: 'on' booleanExpression
            // using equi join keys
            | 'on' identifier (',' identifier)*

groupByItemList: groupByItem (',' groupByItem)* ','?
groupByItem    : expression ('as' identifier (':' identifier)?)?

transformExpr: transformItem (',' transformItem)* ','?
transformItem: qualifiedId '=' expression

selectItems: selectItem (',' selectItem)* ','?
selectItem : (identifier '=')? expression
           | expression ('as' identifier)?

window     : 'over' '(' windowSpec ')'
windowSpec : ('partition' 'by' expression (',' expression)*)?
             ('order' 'by' sortItem (',' sortItem)*)?
             frameSpec?
frameSpec  | ('rows' | 'range') frame
frame      : '[' frameBound? ':' frameBound? ']'
frameBound : INTEGER_VALUE | INTEGER_VALUE 'days' 

testExpr: booleanExpression

showCommand: 'show' identifier

executeCommand: 'execute' expression

sampleExpr: sampleSize
          | ('reservoir' | 'system') '(' sampleSize ')'

sampleSize:  ((integerLiteral 'rows'?) | (floatLiteral '%'))

sortItem: expression ('asc' | 'desc')?

pivotKey: identifier ('in' '(' (valueExpression (',' valueExpression)*) ')')?

typeDef    : 'type' identifier typeParams? context? typeExtends? ':' typeElem* 'end'
typeParams : '[' typeParam (',' typeParam)* ']'
typeParam  : identifier ('of' identifier)?
typeExtends: 'extends' qualifiedId (',' qualifiedId)*
typeElem   : valDef | funDef

valDef     : identifier ':' identifier typeParams? ('=' expression)?
funDef:    : 'def' funName defParams? (':' identifier '*'?)? ('=' expression)?
funName    : identifier | symbol
symbol     : '+' | '-' | '*' | '/' | '%' | '&' | '|' | '=' | '==' | '!=' | '<' | '<=' | '>' | '>=' | '&&' | '||'
defParams  : '(' defParam (',' defParam)* ')'
defParam   : identifier ':' identifier ('=' expression)?

context    : '(' 'in' contextItem (',' contextItem)* ')'
contextItem: identifier (':' identifier)?

strInterpolation: identifier
                | '"' stringPart* '"'
                | '"""' stringPart* '"""'  # triple quotes string
stringPart      : stringLiteral | '${' expression '}'


expression        : booleanExpression
booleanExpression : ('!' | 'not') booleanExpression
                  | valueExpression
                  | booleanExpression ('and' | 'or') booleanExpression
valueExpression   : primaryExpression
                  | valueExpression arithmeticOperator valueExpression
                  | valueExpression comparisonOperator valueExpression

arithmeticOperator: '+' | '-' | '*' | '/' | '%'
comparisonOperator: '=' | '==' | 'is' | '!=' | 'is' 'not' | '<' | '<=' | '>' | '>=' | 'like' | 'contains' 

// Expression that can be chained with '.' operator
primaryExpression : 'this'
                  | '_'
                  | literal
                  | query
                  | 'case' expression? whenExpr+ elseExpr?                        # case-when
                  | '{' querySingle '}'                                           # subquery
                  | '(' expression ')'                                            # parenthesized expression
                  | '[' expression (',' expression)* ']'                          # array
                  | '{' rowElem (',' rowElem)* '}'                       # struct, row
                  | 'map' {' rowElem (',' rowElem)* '}'                       # map value
                  | 'if' booleanExpresssion 'then' expression 'else' expression   # if-then-else
                  | qualifiedId
                  | primaryExpression '.' primaryExpression
                  | primaryExpression '(' functionArg? (',' functionArg)* ')' window? # function call
                  | primaryExpression '[' expression ']'                              # array access
                  | primaryExpression identifier expression                           # function infix

rowElem           : stringLiteral ':' expression
functionArg       | (identifier '=')? expression

literal           : 'null' | '-'? integerLiteral | '-'? floatLiteral | booleanLiteral | stringLiteral

whenExpr          : 'when' booleanExpression 'then' expression
elseExpr          : 'else' expression
lambdaExpr        : lambdaParams '->' expression
lambdaParams      : identirifer 
                  | '(' (identifier (',' identifier)*)? ')'
```
