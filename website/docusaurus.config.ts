import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Wvlet',
  tagline: 'Wvlet: A New Flow-Style Query Language',
  favicon: 'img/favicon.ico',

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

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
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
      title: 'wvlet',
      logo: {
        alt: 'wvlet',
        src: 'img/logos/android-chrome-512x512.png',
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
