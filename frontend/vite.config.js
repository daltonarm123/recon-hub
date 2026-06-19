import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig(({ mode }) => {
  const backendBuild = mode === "backend";

  return {
    plugins: [react(), tailwindcss()],
    build: {
      outDir: backendBuild ? "../backend/static" : "dist",
      emptyOutDir: false,
    },
  };
});