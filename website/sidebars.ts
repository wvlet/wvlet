import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

/**
 * The docs sidebar follows a reader's path: get started, learn the language, pick an engine,
 * connect tools, then contribute. Doc ids are file paths under docs/ without the extension, so a
 * page can sit in any category without moving its file (and changing its URL). List every new
 * page here: docusaurus.config.ts derives the llms-full.txt order from this list. When a page
 * moves, add a redirect from its old URL in docusaurus.config.ts.
 */
const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    {
      type: 'category',
      label: 'Get Started',
      collapsed: false,
      items: [
        'index',
        'usage/install',
        'syntax/quick-start',
        'usage/playground',
        'usage/repl',
        'for-ai-agents',
      ],
    },
    {
      type: 'category',
      label: 'Language',
      link: {type: 'doc', id: 'syntax/index'},
      items: [
        'syntax/flow',
        'syntax/model-pipelines',
        'syntax/data-models',
        'syntax/table-management',
        'syntax/window',
        'syntax/asof-join',
        'syntax/unnest',
        'syntax/test-syntax',
        'syntax/stdlib',
        'syntax/stdlib-reference',
      ],
    },
    {
      type: 'category',
      label: 'Engines',
      link: {type: 'doc', id: 'engines/index'},
      items: [
        {
          type: 'category',
          label: 'Wvlet Engine',
          link: {type: 'doc', id: 'engines/wvlet-engine'},
          items: [
            'engines/wvlet-engine-tables',
            'engines/wvlet-engine-gpu',
            'engines/wvlet-engine-embedding',
          ],
        },
        'engines/duckdb',
        'engines/trino',
        'engines/snowflake',
        'usage/connectors',
        'usage/remote-server',
        'usage/catalog-import',
      ],
    },
    {
      type: 'category',
      label: 'Tools and Integrations',
      items: [
        'usage/cli',
        'usage/cli-reference',
        'usage/ui',
        'usage/vscode',
        'usage/slack',
        {
          type: 'category',
          label: 'Language Bindings',
          link: {type: 'doc', id: 'bindings/index'},
          items: ['bindings/python', 'bindings/typescript'],
        },
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      items: [
        'development/build',
        'development/design',
        'development/compiler-internals',
        'development/grammar',
        'development/sql-grammar',
        'development/syntax-highlighting',
        'development/benchmark',
        'development/release',
      ],
    },
  ],
};

export default sidebars;
