// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { defineConfig, type Plugin } from "vite";

/**
 * Escapes a bundle so it can sit inside an inline <script> element.
 *
 * The HTML parser ends a script at the first `</script`, wherever it appears,
 * including inside a JavaScript string or regex. `<\/` is a valid escape for `/`
 * in both, so the rewrite is safe for the contexts the sequence can legally
 * occur in. `<!--` gets the same treatment: it is legacy HTML-comment-open
 * syntax in scripts and can shadow the rest of the line.
 */
function escapeForInlineScript(code: string): string {
   return code.replace(/<\/script/gi, "<\\/script").replace(/<!--/g, "<\\!--");
}

/**
 * Inlines the JS and CSS into the HTML so the built widget is ONE
 * self-contained file with no external asset references.
 *
 * This is the whole reason the widget needs no configuration. An MCP App is
 * delivered over `resources/read`, which returns only the HTML, so relative
 * asset paths have no origin to resolve against and the server would otherwise
 * have to rewrite them to absolute URLs. That means knowing its own public URL,
 * which a Publisher generally does not: it runs on an arbitrary local port, in
 * Docker, under `npx`, or behind a proxy that terminates elsewhere. Inlining
 * removes the question, along with the static asset route and the CSP
 * resource-domain metadata a host would need to allow those loads.
 *
 * The cost is a large HTML file, which is the deliberate trade.
 */
function inlineWidgetAssets(): Plugin {
   return {
      name: "publisher-inline-widget-assets",
      enforce: "post",
      generateBundle(_options, bundle) {
         const htmlKey = Object.keys(bundle).find((key) =>
            key.endsWith(".html"),
         );
         if (!htmlKey) {
            this.error(
               "inlineWidgetAssets: no HTML asset in the bundle; nothing to inline into",
            );
            return;
         }
         const htmlAsset = bundle[htmlKey];
         if (htmlAsset.type !== "asset") {
            this.error(`inlineWidgetAssets: ${htmlKey} is not an asset`);
            return;
         }
         let html = String(htmlAsset.source);

         for (const [key, output] of Object.entries(bundle)) {
            if (key === htmlKey) continue;

            if (output.type === "chunk") {
               // Match the tag by its filename so a hashed name still resolves.
               const tag = new RegExp(
                  `<script[^>]*src="[^"]*${escapeRegExp(key)}"[^>]*></script>`,
               );
               if (!tag.test(html)) {
                  this.error(
                     `inlineWidgetAssets: no <script> tag references ${key}; ` +
                        "the bundle was split, so inlining would drop code. " +
                        "Set build.rollupOptions.output.inlineDynamicImports.",
                  );
                  return;
               }
               // A replacer FUNCTION, never a replacement string. In a string,
               // `$&` and friends are substitution patterns, and minified
               // dependency code contains them: passing the bundle as a string
               // spliced 22 copies of this very <script> tag into the middle of
               // the JavaScript. A function receives the text verbatim.
               html = html.replace(
                  tag,
                  () =>
                     `<script type="module">${escapeForInlineScript(output.code)}</script>`,
               );
               delete bundle[key];
            } else if (key.endsWith(".css")) {
               const tag = new RegExp(
                  `<link[^>]*href="[^"]*${escapeRegExp(key)}"[^>]*>`,
               );
               // Function replacer, for the same reason as the script above.
               html = html.replace(
                  tag,
                  () => `<style>${String(output.source)}</style>`,
               );
               delete bundle[key];
            }
         }

         const dangling = html.match(/(?:src|href)="[^"]*\/?assets\/[^"]*"/g);
         if (dangling) {
            this.error(
               "inlineWidgetAssets: the built HTML still references external " +
                  `assets, so it is not self-contained: ${dangling.join(", ")}. ` +
                  "The widget would render blank in a host that cannot fetch them.",
            );
            return;
         }

         htmlAsset.source = html;
      },
   };
}

function escapeRegExp(value: string): string {
   return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export default defineConfig({
   // The widget is delivered as a document, not served from a path.
   base: "./",
   plugins: [inlineWidgetAssets()],
   build: {
      outDir: "dist",
      emptyOutDir: true,
      target: "esnext",
      minify: true,
      // Sourcemaps would be inlined into the HTML along with everything else,
      // multiplying its size for a build nobody debugs from the chat client.
      sourcemap: false,
      // The bundle is inlined into one <script>, so it must be one chunk.
      // Without this, a dynamic import inside @malloydata/render splits the
      // build and the split chunk would be dropped.
      rollupOptions: {
         input: "execute-query.html",
         output: {
            inlineDynamicImports: true,
         },
      },
      // The whole point is a self-contained file, so inline every referenced
      // asset regardless of size rather than emitting it alongside.
      assetsInlineLimit: Number.MAX_SAFE_INTEGER,
      // One large inlined chunk is the design, not an accident worth warning
      // about on every build.
      chunkSizeWarningLimit: 10_000,
   },
});
