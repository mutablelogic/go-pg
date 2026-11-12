// Builds the web UI into build/frontend: the TypeScript, bundled, with its
// styles and fonts, into build/frontend/dist, with the assets, such as
// index.html, and embed.go, so a server built with the "frontend" tag
// embeds it

import esbuild from 'esbuild';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { cpSync, readFileSync } from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const buildDir = resolve(__dirname, '../build/frontend');

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
    outdir: `${buildDir}/dist`,
    format: 'esm',
    target: 'es2022',
    minify: true,
    sourcemap: true,
    loader: { '.woff2': 'file' },
    assetNames: 'fonts/[name]-[hash]',
    plugins: [carbonFonts],
}).catch(() => process.exit(1));

cpSync(resolve(__dirname, 'assets/dist'), `${buildDir}/dist`, { recursive: true });
cpSync(resolve(__dirname, 'assets/embed.go'), `${buildDir}/embed.go`);
console.log(`Built → ${buildDir}`);
