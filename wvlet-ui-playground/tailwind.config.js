export default {
  content: [
    "./index.html",
    "./target/**/*.js"
  ],
  theme: {
    extend: {
      fontFamily: {
        mono: [
          /* Consolas is most reliable mono-space font, even on Windows */
          'Consolas',
          'ui-monospace',
          'SFMono-Regular',
          'Menlo',
          'Monaco',
          "Liberation Mono",
          "Courier New",
          'monospace'
        ],
      },
    },
  },
  plugins: [
      "@tailwindcss/postcss",
  ],
}

