import * as monaco from 'monaco-editor';
import {languages} from 'monaco-editor';


export const conf: languages.LanguageConfiguration = {
    comments: {
        lineComment: '--'
    },
    brackets: [
        ['{', '}'],
        ['[', ']'],
        ['(', ')'],
        ['${', '}']
    ],
    autoClosingPairs: [
        { open: '{', close: '}' },
        { open: '[', close: ']' },
        { open: '(', close: ')' },
        { open: "'", close: "'", notIn: ['string', 'comment'] },
        { open: '"', close: '"', notIn: ['string'] },
        { open: '`', close: '`', notIn: ['string', 'comment'] },
    ],
    surroundingPairs: [
        { open: "'", close: "'" },
        { open: '"', close: '"' },
        { open: '`', close: '`' },
    ],
}

export const language = <languages.IMonarchLanguage> {
    // Function names highlighted after a dot; set from the standard library by setFunctionNames
    functions: [],

    defaultToken: '',
    tokenPostfix: '.wv',

    keywords: [
        "asof",
        "case",
        "when",
        "test",
        "should",
        "be",
        "contain",
        "debug",
        "def",
        "inline",
        "type",
        "extends",
        "native",
        "show",
        "sample",
        "this",
        "of",
        "in",
        "by",
        "as",
        "to",
        "with",
        "from",
        "agg",
        "select",
        "for",
        "let",
        "where",
        "group",
        "having",
        "order",
        "limit",
        "transform",
        "pivot",
        "distinct",
        "asc",
        "desc",
        "join",
        "on",
        "left",
        "right",
        "full",
        "inner",
        "cross",
        "add",
        "exclude",
        "rename",
        "shift",
        "drop",
        "describe",
        "concat",
        "dedup",
        "intersect",
        "except",
        "all",
        "over",
        "partition",
        "unbounded",
        "preceding",
        "following",
        "current",
        "range",
        "row",
        "run",
        "import",
        "export",
        "package",
        "model",
        "execute",
        "val",
        "if",
        "then",
        "else",
        "end",
        "and",
        "or",
        "not",
        "is",
        "like",
        "save",
        "append",
        "delete",
        "truncate",
        "count",
        "between",
        "exists",
        "explain",
        "unpivot",
        "update",
        "use",
        "create",
        "call",
        "escape",
        "rows",
        "nulls",
        "first",
        "last",
        "merge",
        "true",
        "false",
        "null",
        "flow",
        "stage",
        "each",
        "depends",
        "activate",
        "activity",
        "fork",
        "route",
        "wait"
    ],

    typeKeywords: [
        "boolean",
        "double",
        "byte",
        "int",
        "short",
        "char",
        "void",
        "long",
        "float",
        "string",
        "array",
        "map",
        "date",
        "decimal",
        "interval"
    ],

    operators: [
        ":",
        ";",
        ",",
        ".",
        "_",
        "@",
        "$",
        "*",
        "?",
        "<-",
        "->",
        "=>",
        "=",
        "!=",
        "<",
        ">",
        "<=",
        ">=",
        "+",
        "-",
        "/",
        "//",
        "%",
        "!",
        "&",
        "|",
        "#"
    ],

    symbols: /[=><!~?:&|+\-*\/\^%]+/,

    tokenizer: {
        root: [
            // A known function after a dot (`name.upper`) is a function call; other dotted names
            // (`t.n_name`) stay identifiers
            [/(\.)([a-z_][a-z_0-9]*)/, {
               cases : {
                   '$2@functions': ['delimiter', 'function'],
                   '@default': ['delimiter', 'identifier']
               }
            }],
            // A name before `(` is a call, unless it is a keyword (`in (...)`, `exists (...)`)
            [/[a-z_][a-z_0-9]*(?=\s*\()/, {
               cases : {
                   '@keywords': 'keyword',
                   '@typeKeywords': 'type.keyword',
                   '@default': 'function'
               }
            }],
            [/[a-z_][a-z_0-9]*/, {
               cases : {
                   '@keywords': 'keyword',
                   '@typeKeywords': 'type.keyword',
                   '@default': 'identifier'
               }
            }],
            [/[A-Z][a-zA-Z_0-9][a-zA-Z_\.0-9]*/, 'type.identifier'],
            [/---/, { token: 'comment', next:'@multilinecomment' }],
            [/--.*/, 'comment'],
            [/@symbols/, {
                cases : {
                    '@operators': 'operator',
                    '@default': ''
                }
            }],
            [/"""/, { token: 'string.quote', next:'@string' }],
            [/"/, { token:'string.quote', next:'@string'}],
            [/'.*?'/, 'string'],
            [/`.*?`/, 'string.backquoted'],
            [/[1-9][0-9_]*.[0-9]+/, 'number.float'],
            [/0[xX][0-9a-fA-F_]+/, 'number'],
            [/[1-9][0-9_]+/, 'number'],
        ],
        multilinecomment: [
          [/---/, { token: 'comment', next: '@pop' }],
          [/./, 'comment']
        ],
        string: [
          [/"""/, { token: 'string.quote', next: '@pop' }],
          [/"/, { token: 'string.quote', next: '@pop' }],
          [/\$\{/, { token: 'string.quote', next: '@interpolation' }],
          [/./, 'string']
        ],
        interpolation: [
            [/}/, { token: 'string.interpolation', next: '@pop' }],
            [/@symbols/, {
                cases: {
                    '@operators': 'operator',
                    '@default': ''
                }
            }],
            [/[1-9][0-9_]*.[0-9]+/, 'number.float'],
            [/0[xX][0-9a-fA-F_]+/, 'number'],
            [/[1-9][0-9_]+/, 'number']
        ]
    }
}

languages.register({
    id: 'wvlet',
    extensions: ['.wv'],
    aliases: ['Wvlet', 'wvlet', 'wv'],
})

languages.registerTokensProviderFactory(
    'wvlet',
    { create: async(): Promise<languages.IMonarchLanguage> => {
            return language;
        }
    }
)
languages.setLanguageConfiguration('wvlet', conf);



/**
 * Highlight these names as functions after a dot (`name.upper`), e.g. the standard library's
 * member functions. Re-registers the tokenizer, so open editors re-color.
 */
export function setFunctionNames(names: string[]): void {
    (language as any).functions = names;
    languages.setMonarchTokensProvider('wvlet', language);
}
