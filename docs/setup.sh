npm create vite@latest . -- --template react-ts
npm install -D tailwindcss postcss autoprefixer



# If you haven't done this yet
npx tailwindcss init -p

# Edit tsconfig.json to include:
# "baseUrl": ".", "paths": { "@/*": ["src/*"] }

# Then proceed with Shadcn initialization
npx shadcn@latest init
# Follow the prompts, confirm Tailwind, choose component dir (e.g., src/components/ui)


# Step 1: Create Vite + React + TypeScript project
npm create vite@latest . -- --template react-ts

# Step 2: Install Tailwind CSS + PostCSS + Autoprefixer
npm install -D tailwindcss postcss autoprefixer
npx tailwindcss init -p

# Step 3: Install Shadcn CLI and initialize
npx shadcn@latest init

# Step 4: Add required Shadcn components
npx shadcn@latest add button
npx shadcn@latest add input
npx shadcn@latest add dialog
npx shadcn@latest add tabs
npx shadcn@latest add calendar

# Step 5: Install AG Grid and its styles
npm install ag-grid-react ag-grid-enterprise ag-grid-community

# Step 6: Install remaining app dependencies
npm install @rjsf/core @rjsf/validator-ajv8 json-schema-ref-parser axios clsx lucide-react class-variance-authority tailwind-variants

# # Step 7: Configure tailwind.config.js (example content)
# module.exports = {
#   content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
#   theme: { extend: {} },
#   plugins: []
# }

# # Step 8: Create src/index.css with Tailwind layers
# @tailwind base;
# @tailwind components;
# @tailwind utilities;