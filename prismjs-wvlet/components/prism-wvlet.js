/*
Language: Wvlet
Description: Cross-SQL flow-style query language for functional data modeling
Website: https://wvlet.org
Category: data
*/

Prism.languages.wvlet = {
	// Doc comments (highest priority)
	'doc-comment': {
		pattern: /---[\s\S]*?---/,
		greedy: true,
		inside: {
			'doctag': /@\w+/
		}
	},

	// Line comments
	'comment': {
		pattern: /--.*$/m
	},

	// Triple-quoted strings with interpolation
	'template-string': {
		pattern: /"""[\s\S]*?"""/,
		greedy: true,
		inside: {
			'template-punctuation': {
				pattern: /^"""|"""$/,
				alias: 'punctuation'
			},
			'interpolation': {
				pattern: /\$\{[^}]*\}/,
				inside: {
					'interpolation-punctuation': {
						pattern: /^\$\{|\}$/,
						alias: 'punctuation'
					},
					// This will be populated recursively after the main grammar is defined
					'rest': {}
				}
			}
		}
	},

	// Regular strings with interpolation
	'string': [
		{
			pattern: /"(?:\\.|[^"\\]|\$\{[^}]*\})*"/,
			greedy: true,
			inside: {
				'interpolation': {
					pattern: /\$\{[^}]*\}/,
					inside: {
						'interpolation-punctuation': {
							pattern: /^\$\{|\}$/,
							alias: 'punctuation'
						}
						// Note: Avoid circular reference, use basic patterns instead
					}
				}
			}
		},
		{
			pattern: /'(?:\\.|[^'\\])*'/,
			greedy: true
		}
	],

	// Numbers (combining all variants)
	'number': /\b\d+(?:L|[fF]|\.\d+(?:[fF]|[eE][+-]?\d+)?|[eE][+-]?\d+)\b|\b\d+\b/,

	// Function definitions
	'function-definition': {
		pattern: /\b(?:model|def|type)\s+([a-zA-Z_][a-zA-Z0-9_]*)/,
		inside: {
			'keyword': /\b(?:model|def|type)\b/,
			'function': /[a-zA-Z_][a-zA-Z0-9_]*/
		}
	},

	// Keywords  
	'keyword': /\b(?:model|def|type|extends|native|inline|val|from|select|where|group|by|order|limit|agg|join|left|right|full|inner|cross|asof|on|pivot|unpivot|partition|over|rows|range|add|prepend|exclude|rename|shift|drop|describe|concat|dedup|intersect|except|all|distinct|save|append|delete|truncate|import|export|package|execute|use|run|test|should|be|contain|debug|show|explain|sample|count|if|then|else|case|when|and|or|not|is|like|between|exists|in|as|with|to|for|let|this)\b/i,

	// Literals
	'boolean': /\b(?:true|false|null)\b/i,

	// Built-in functions and keywords
	'builtin': /\b(?:asc|desc|nulls|first|last|of|map)\b/i,

	// Test assertions
	'annotation': {
		pattern: /^test\b.*$/m,
		inside: {
			'keyword': /\b(?:test|should|be|contain)\b/
		}
	},

	// From statements with file paths
	'url': {
		pattern: /\bfrom\s+(?:'[^']*'|"[^"]*")/,
		inside: {
			'keyword': /^from/,
			'string': /'[^']*'|"[^"]*"/
		}
	},

	// Function calls
	'function': {
		pattern: /\b[a-zA-Z_][a-zA-Z0-9_]*(?=\s*\()/,
		alias: 'function'
	},

	// Backquoted identifiers
	'symbol': {
		pattern: /`(?:\\.|[^`\\])*`/,
		greedy: true
	},

	// Operators
	'operator': /!=|<>|<=|>=|::|->|<-|=>|\|\||&&|==|[=<>+\-*/%!]/,

	// Punctuation
	'punctuation': /[{}[\]();,.:]/
};

// Add recursive grammar for interpolation
Prism.languages.wvlet['template-string'].inside.interpolation.inside.rest = Prism.languages.wvlet;

// Add aliases for backward compatibility
Prism.languages.wv = Prism.languages.wvlet;