/*
Language: Wvlet
Description: Cross-SQL flow-style query language for functional data modeling
Website: https://wvlet.org
Category: data
*/

export default function(hljs) {
  const KEYWORDS = {
    keyword: 'model def type extends native inline val ' +
             'from select where group by having order limit agg ' +
             'join left right full inner cross asof on ' +
             'pivot unpivot partition over rows range ' +
             'add prepend exclude rename shift drop describe ' +
             'concat dedup intersect except all distinct ' +
             'save append delete truncate ' +
             'import export package execute use run ' +
             'test should be contain debug ' +
             'show explain sample count ' +
             'if then else case when end ' +
             'and or not is like between exists in as with to for let this',
    literal: 'true false null',
    built_in: 'asc desc nulls first last of map'
  };

  const WVLET_IDENT = /[a-zA-Z_][a-zA-Z0-9_]*/;
  
  // Backquoted identifiers like `column_name`
  const BACKQUOTED_IDENT = {
    className: 'symbol',
    begin: /`/,
    end: /`/,
    contains: [
      {
        begin: /\\./  // escaped characters
      }
    ]
  };

  // String interpolation support
  const STRING_INTERPOLATION = {
    className: 'subst',
    begin: /\$\{/,
    end: /\}/,
    keywords: KEYWORDS,
    contains: [] // will be defined later
  };

  // Triple-quoted strings
  const TRIPLE_QUOTE_STRING = {
    className: 'string',
    begin: /"""/,
    end: /"""/,
    contains: [
      hljs.BACKSLASH_ESCAPE,
      STRING_INTERPOLATION
    ]
  };

  // Regular strings
  const STRING = {
    className: 'string',
    variants: [
      {
        begin: /"/,
        end: /"/,
        illegal: /\n/,
        contains: [
          hljs.BACKSLASH_ESCAPE,
          STRING_INTERPOLATION
        ]
      },
      {
        begin: /'/,
        end: /'/,
        illegal: /\n/,
        contains: [hljs.BACKSLASH_ESCAPE]
      }
    ]
  };

  // Comments
  const COMMENT = hljs.COMMENT(
    /--/,
    /$/,
    {
      relevance: 0
    }
  );

  // Doc comments (triple hyphen)
  const DOC_COMMENT = hljs.COMMENT(
    /---/,
    /---/,
    {
      relevance: 10,
      contains: [
        {
          className: 'doctag',
          begin: /@\w+/
        }
      ]
    }
  );

  // Numbers
  const NUMBER = {
    className: 'number',
    variants: [
      { begin: /\b\d+L\b/ },                    // Long literal
      { begin: /\b\d+\.\d+[fF]\b/ },           // Float literal  
      { begin: /\b\d+[fF]\b/ },                 // Float literal
      { begin: /\b\d+\.\d+([eE][+-]?\d+)?\b/ }, // Decimal/Double
      { begin: /\b\d+[eE][+-]?\d+\b/ },        // Exponential
      { begin: /\b\d+\b/ }                      // Integer
    ],
    relevance: 0
  };

  // Model/function definitions
  const MODEL_DEF = {
    className: 'function',
    beginKeywords: 'model def type',
    end: /[={]/,
    excludeEnd: true,
    contains: [
      {
        className: 'title',
        begin: WVLET_IDENT,
        relevance: 0
      },
      {
        className: 'params',
        begin: /\(/,
        end: /\)/,
        excludeBegin: true,
        excludeEnd: true,
        keywords: KEYWORDS,
        contains: [
          STRING,
          NUMBER,
          hljs.C_LINE_COMMENT_MODE
        ]
      }
    ]
  };

  // Column references with dot notation
  const COLUMN_REF = {
    className: 'variable',
    begin: /\b[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*/,
    relevance: 0
  };

  // Operators
  const OPERATORS = {
    className: 'operator',
    begin: /[=!<>]=?|->|=>|<-|\+|-|\*|\/|\/\/|%|&|\||#/,
    relevance: 0
  };

  // Special underscore context
  const UNDERSCORE = {
    className: 'variable',
    begin: /\b_\b/,
    relevance: 0
  };

  // Test assertions
  const TEST_ASSERTION = {
    className: 'meta',
    begin: /^test\b/,
    end: /$/,
    keywords: 'test should be contain',
    contains: [
      STRING,
      NUMBER,
      COMMENT,
      COLUMN_REF
    ]
  };

  // From statements with file paths
  const FROM_STATEMENT = {
    begin: /\bfrom\s+'/,
    beginScope: {
      1: 'keyword'
    },
    end: /'/,
    contains: [
      {
        className: 'string',
        begin: /[^']+/
      }
    ]
  };

  // Function calls - any identifier followed by (
  const FUNCTION_CALL = {
    className: 'built_in',
    begin: /\b[a-zA-Z_][a-zA-Z0-9_]*(?=\s*\()/,
    relevance: 0
  };

  // Define STRING_INTERPOLATION contents
  STRING_INTERPOLATION.contains = [
    STRING,
    NUMBER,
    COLUMN_REF,
    OPERATORS
  ];

  return {
    name: 'Wvlet',
    aliases: ['wv'],
    case_insensitive: false,
    keywords: KEYWORDS,
    contains: [
      DOC_COMMENT,
      COMMENT,
      TRIPLE_QUOTE_STRING,
      STRING,
      NUMBER,
      MODEL_DEF,
      TEST_ASSERTION,
      FROM_STATEMENT,
      FUNCTION_CALL,  // Add function call pattern
      BACKQUOTED_IDENT,
      UNDERSCORE,
      OPERATORS,
      // COLUMN_REF is removed - keywords will be handled by highlight.js automatically
      hljs.C_BLOCK_COMMENT_MODE
    ]
  };
}