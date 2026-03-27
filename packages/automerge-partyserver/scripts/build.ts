import { build } from "tsdown";

await build({
  entry: [
    "src/server/index.ts",
    "src/provider/index.ts",
  ],
  external: ["cloudflare:workers"],
  sourcemap: true,
  clean: true,
  format: ["esm"],
  dts: true,
  skipNodeModulesBundle: true,
  fixedExtension: false,
});

process.exit(0);
