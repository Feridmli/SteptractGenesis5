import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  root: ".",
  resolve: {
    alias: {
      buffer: "buffer",
    },
  },
  define: {
    // Browser mühiti üçün global obyektini window-a bağlayırıq
    global: "window", 
  },
  build: {
    outDir: "dist",
    target: "esnext", // Modern JS dəstəyi
    chunkSizeWarningLimit: 5000, 
    rollupOptions: {
      input: path.resolve(__dirname, "index.html"),
    },
  },
  server: {
    port: 5173,
  },
});
