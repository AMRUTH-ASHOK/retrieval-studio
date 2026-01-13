/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        databricks: {
          primary: '#FF3621',
          blue: '#1B3B78',
          'blue-light': '#3B5DAA',
          'blue-dark': '#0A1F44',
          gray: {
            50: '#F7F7F7',
            100: '#F0F0F0',
            200: '#E0E0E0',
            300: '#C4C4C4',
            400: '#999999',
            500: '#6B6B6B',
            600: '#4A4A4A',
            700: '#333333',
            800: '#1F1F1F',
            900: '#0D0D0D',
          },
          success: '#2E7D32',
          warning: '#F57C00',
          error: '#D32F2F',
        },
      },
      fontFamily: {
        sans: ['-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'Helvetica Neue', 'Arial', 'sans-serif'],
      },
      boxShadow: {
        'db-sm': '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
        'db': '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06)',
        'db-md': '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
        'db-lg': '0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)',
      },
    },
  },
  plugins: [],
}
