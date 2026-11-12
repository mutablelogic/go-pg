// Builds the web UI into dist: the TypeScript, bundled, with its styles and
// fonts, and the assets, such as index.html, which embed.go embeds in a
// server built with the "frontend" tag. dist is emptied first, so it has
// only what's built.

import esbuild from 'esbuild';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { cpSync, readFileSync, rmSync } from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const distDir = resolve(__dirname, 'dist');
rmSync(distDir, { recursive: true, force: true });

// carbonFonts strips the fonts which Carbon's prebuilt styles load from
// IBM's CDN, as the UI bundles its own (see src/fonts.css)
const carbonFonts = {
    name: 'carbon-fonts',
    setup(build) {
        build.onLoad({ filter: /@carbon[\\/]styles[\\/]css[\\/]styles(\.min)?\.css$/ }, (args) => ({
            contents: readFileSync(args.path, 'utf8').replace(/@font-face\s*\{[^}]*\}/g, ''),
            loader: 'css',
        }));
    },
};

await esbuild.build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outdir: distDir,
    format: 'esm',
    target: 'es2022',
    minify: true,
    sourcemap: true,
    loader: { '.woff2': 'file' },
    assetNames: 'fonts/[name]-[hash]',
    plugins: [carbonFonts],
}).catch(() => process.exit(1));

cpSync(resolve(__dirname, 'assets/dist'), distDir, { recursive: true });
console.log(`Built → ${distDir}`);
