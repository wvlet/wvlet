/**
 * Adds the SVG gradients that diagram boxes use as fills (see the flowchart rules in custom.css).
 * Mermaid has no gradient syntax, but its shapes can fill with url(#id) of a gradient anywhere on
 * the page. The holder stays rendered at zero size, since gradients inside display:none fail in
 * some browsers.
 */
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';

const gradients: Record<string, [string, string]> = {
  'wv-grad-stage': ['#22c3d6', '#0a7d8e'],
  'wv-grad-lib': ['#3a72c4', '#163d78'],
  'wv-grad-target': ['#ffffff', '#e6eef0'],
};

if (ExecutionEnvironment.canUseDOM && !document.getElementById('wv-diagram-gradients')) {
  const stops = Object.entries(gradients)
    .map(
      ([id, [top, bottom]]) =>
        `<linearGradient id="${id}" x1="0" y1="0" x2="0" y2="1">` +
        `<stop offset="0%" stop-color="${top}"/><stop offset="100%" stop-color="${bottom}"/>` +
        `</linearGradient>`,
    )
    .join('');
  const holder = document.createElement('div');
  holder.id = 'wv-diagram-gradients';
  holder.setAttribute('aria-hidden', 'true');
  holder.style.cssText = 'position:absolute;width:0;height:0;overflow:hidden';
  holder.innerHTML = `<svg width="0" height="0" focusable="false"><defs>${stops}</defs></svg>`;
  document.body.appendChild(holder);
}
