import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import vscodePackage from '../vscode-wvlet/package.json';

// The VS Code extension version is bumped in every release commit (see CLAUDE.md), so it
// doubles as the current Wvlet release version for structured data.
const wvletVersion: string = vscodePackage.version;
const siteDescription =
  'Wvlet is a flow-style query language that compiles to SQL for DuckDB, Trino, Snowflake, and Hive. ' +
  'Queries read top-to-bottom in data-processing order, so humans and AI agents can add a step without rewriting nested SQL.';

// Site-wide structured data for search engines and AI search.
const jsonLd = {
  '@context': 'https://schema.org',
  '@type': ['SoftwareApplication', 'SoftwareSourceCode'],
  name: 'Wvlet',
  alternateName: 'wvlet',
  description: siteDescription,
  url: 'https://wvlet.org/wvlet/',
  codeRepository: 'https://github.com/wvlet/wvlet',
  license: 'https://www.apache.org/licenses/LICENSE-2.0',
  softwareVersion: wvletVersion,
  version: wvletVersion,
  applicationCategory: 'DeveloperApplication',
  operatingSystem: 'macOS, Linux, Windows',
  programmingLanguage: ['Wvlet', 'SQL', 'Scala'],
  keywords: 'query language, SQL, flow-style, DuckDB, Trino, Snowflake, data modeling, AI agents',
  offers: {'@type': 'Offer', price: '0', priceCurrency: 'USD'},
  author: {'@type': 'Organization', name: 'wvlet.org', url: 'https://wvlet.org/'},
};

const config: Config = {
  title: 'Wvlet',
  tagline: 'Wvlet: A New Flow-Style Query Language',
  favicon: 'img/favicon.ico',
  // Vector favicon for browsers that support it; `favicon` above stays the ICO fallback.
  // headTags hrefs are used verbatim, so the baseUrl is included explicitly.
  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'icon',
        type: 'image/svg+xml',
        href: '/wvlet/img/logos/wvlet-icon.svg',
      },
    },
    {
      tagName: 'script',
      attributes: {type: 'application/ld+json'},
      innerHTML: JSON.stringify(jsonLd),
    },
  ],

  // Set the production url of your site here
  url: 'https://wvlet.org',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/wvlet/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'wvlet', // Usually your GitHub org/user name.
  projectName: 'wvlet', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  plugins: [
    [
      'docusaurus-plugin-llms',
      {
        title: 'Wvlet',
        description: siteDescription,
        version: wvletVersion,
        // Also write a .md copy of every doc page and link to those from llms.txt.
        generateMarkdownFiles: true,
        // MDX imports (e.g., Tabs) are build-time only and mean nothing to a reader.
        excludeImports: true,
        removeDuplicateHeadings: true,
        // Contributor docs stay on the site but are kept out of the agent-facing files.
        ignoreFiles: ['development/**'],
        includeOrder: ['for-ai-agents.md', 'index.md', 'syntax/**', 'usage/**', 'bindings/**'],
        rootContent:
          'Start with the [Wvlet for AI Agents](https://wvlet.org/wvlet/docs/for-ai-agents.md) page: ' +
          'it has a syntax cheat sheet, a SQL-to-Wvlet mapping table, and common mistakes to avoid.',
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // Read last-update dates from git; the sitemap uses them for <lastmod>.
          // CI checks out full history (fetch-depth: 0) so these are accurate.
          showLastUpdateTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/wvlet/wvlet/tree/main/website/',
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/wvlet/wvlet/tree/main/website/',
          // Useful options to enforce blogging best practices
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          lastmod: 'date',
          changefreq: null,
          priority: null,
        },
        gtag: {
          trackingID: 'G-55G099WF5N',
          anonymizeIP: true,
        }
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/logos/wvlet-banner-1280x640.png',
    navbar: {
      title: 'Wvlet',
      logo: {
        alt: 'Wvlet',
        src: 'img/logos/wvlet-icon.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          href: '/docs/syntax/',
          position: 'left',
          label: 'Query Syntax',
        },
        {
          to: '/docs/for-ai-agents',
          position: 'left',
          label: 'For AI Agents',
        },
        {
          href: 'pathname:///playground/',
          position: 'left',
          label: 'Playground',
        },
        {to: '/blog', label: 'Blog', position: 'left'},
        {
          href: 'https://github.com/orgs/wvlet/projects/2/views/2',
          label: 'Roadmap',
          position: 'right',
        },
        {
          href: 'https://github.com/wvlet/wvlet',
          label: 'GitHub',
          position: 'right',
          className: 'header-github-link',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Documentation',
              to: '/docs/',
            },
            {
              href: 'https://github.com/wvlet/wvlet/releases',
              label: 'Release Notes',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Stack Overflow',
              href: 'https://stackoverflow.com/questions/tagged/wvlet',
            },
            {
              label: 'Discord',
              href: 'https://discord.gg/vJBXRfEeNQ',
            },
            {
              label: 'X',
              href: 'https://x.com/wvlet_lang',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: '/blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/wvlet/wvlet',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} wvlet.org. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.okaidia,
      darkTheme: prismThemes.okaidia,
      additionalLanguages: ['sql', 'antlr4', 'bash'],
    },
    algolia: {
      // The application ID provided by Algolia
      appId: 'XY6KZ8HC00',

      // Public API key: it is safe to commit it
      apiKey: '67fdbb927f084c7d029b3db8108202b6',

      indexName: 'wvlet',

      // Optional: see doc section below
      contextualSearch: true,

      // Optional: Algolia search parameters
      searchParameters: {},

      // Optional: path for search page that enabled by default (`false` to disable it)
      searchPagePath: 'search',

      // Optional: whether the insights feature is enabled or not on Docsearch (`false` by default)
      insights: false,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
