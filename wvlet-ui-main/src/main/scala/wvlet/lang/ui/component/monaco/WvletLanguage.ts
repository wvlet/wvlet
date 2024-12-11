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
    defaultToken: '',
    tokenPostfix: '.wv',

    keywords: [
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
        "truncate"
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
        "decimal"
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
            [/[a-z_][a-z_0-9\\.]*/, {
               cases : {
                   '@keywords': 'keyword',
                   '@typeKeywords': 'type.keyword',
                   '@default': 'identifier'
               }
            }],
            [/[A-Z][a-zA-Z_0-9][a-zA-Z_\.0-9]*/, 'type.identifier'],
            [/--.*/, 'comment'],
            [/"""/, { token: 'comment', next:'@multilinecomment' }],
            [/@symbols/, {
                cases : {
                    '@operators': 'operator',
                    '@default': ''
                }
            }],
            [/"/, { token:'string.quote', next:'@string'}],
            [/'.*?'/, 'string'],
            [/`.*?`/, 'string.backquoted'],
            [/[1-9][0-9_]*.[0-9]+/, 'number.float'],
            [/0[xX][0-9a-fA-F_]+/, 'number'],
            [/[1-9][0-9_]+/, 'number'],
        ],
        multilinecomment: [
          [/"""/, { token: 'comment', next: '@pop' }],
          [/./, 'comment']
        ],
        string: [
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


