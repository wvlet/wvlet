/** @type {import('tailwindcss').Config} */
const defaultTheme = require('tailwindcss/defaultTheme')

export default {
  content: [
    "./index.html",
    "./target/**/*.js"
  ],
  theme: {
    extend: {
      fontFamily: {
        mono: ['Consolas', ...defaultTheme.fontFamily.mono],
      },
    },
  },
  plugins: [],
}

