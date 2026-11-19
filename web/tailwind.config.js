/** @type {import('tailwindcss').Config} */
// Only Tailwind's base reset and the sr-only utility are used; styles live in src/index.css.
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
}
