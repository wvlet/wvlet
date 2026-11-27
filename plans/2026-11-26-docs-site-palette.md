# Unify the docs site styling with the top page

## Goals

- Docs, blog, and the top page should read as one site: same teal palette, backgrounds, navbar, and code blocks in light and dark mode.
- Docs must stay as readable as today (contrast, code legibility, sidebar) at desktop and phone widths.

## Background

- The top page (#2510) defines HSL tokens scoped to `.home` in `website/src/pages/index.module.css`, following the Wvlet icon's teals; the comment there says the rest of the site "keeps Infima's defaults".
- Docs pages use Infima defaults with primary `#2e7685` (light) / `#25a7c1` (dark) in `website/src/css/custom.css`, plain white / grey-black backgrounds, the default opaque navbar, and Infima's grey `dark` footer.
- Code blocks use `prismThemes.okaidia` for both modes, so light docs pages show dark Monokai blocks with off-palette pinks and yellows.
- The top page's code windows are always dark (terminal look), in light mode too; prism-react-renderer paints token colors as inline styles from the active theme, so a plain light prism theme would put light-theme colors on those dark windows.
- Other sessions edit doc content in parallel; this PR touches only CSS and the prism / footer theme config.

## Design

- Lane: **UI** — restyling existing screens; no new pages, settings, or content.
- **One token set for the site:** move the base tokens (`--wv-bg`, `--wv-bg-2`, `--wv-card`, `--wv-ink`, `--wv-ink-2`, `--wv-line`, `--wv-primary`, `--wv-term-bg`) from `.home` to `:root` / `[data-theme='dark']` in `custom.css`; `.home` keeps its decoration tokens (glows, shimmer, waves) and reads the base ones from there.
- **Infima mapped to the tokens:** primary shades from the top page's primary (light `190 48% 34%`, dark `186 100% 52%`), page background, surface, text, borders, and the sidebar / TOC hover colors follow the same palette. The light primary barely moves (`#2e7685` → `190 48% 34%`); in dark mode, docs links and headings use a softer cyan (`186 70% 56%`) than the top page's `186 100% 52%` accent, which glared across whole blog headlines.
- **Navbar:** the translucent, blurred navbar now applies on every page, not just the top page; the navbar is solid below 996px, where the phone menu can open, since `backdrop-filter` would confine the fixed menu (which holds the docs sidebar) to the navbar's height.
- **Code blocks:** a small custom prism theme whose colors are CSS variables (`var(--wv-code-keyword)` …); light pages get a light teal-tinted code palette on `--wv-bg-2`, dark pages a dark teal one on `--wv-card`. One theme object serves both modes, so there is no color flash on hydration; it maps every token type the Wvlet, SQL, bash, and ANTLR grammars emit. The top page's code windows set the dark variables, so they stay dark in both modes; the highlighted-line background derives from the primary.
- **Inline code and footer:** inline code uses the top page's tinted chip; the footer uses the dark teal background instead of Infima's grey.
- Verified with light / dark screenshots of a docs page, a syntax page with code, the blog, and the top page, at desktop and 390px widths; attached to the PR.

## Alternatives and Why Not?

- `prismThemes.github` / `vsDark` pair: fixes light docs but turns the top page's dark code windows into light-theme colors on dark backgrounds.
- Swizzling CodeBlock to force a theme per page: more code to maintain on Docusaurus upgrades than CSS variables.
- Keep okaidia for both: leaves the main visual mismatch the item asks to fix.
