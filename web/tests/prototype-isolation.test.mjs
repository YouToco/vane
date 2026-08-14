import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");

test("P0-A owner preview is built but absent from app routing and navigation", async () => {
  const [main, app, vite, prototypeHtml, packageJson, gitignore, p0aVite, headers] =
    await Promise.all([
      readFile(resolve(root, "src/app/main.tsx"), "utf8"),
      readFile(resolve(root, "src/app/App.tsx"), "utf8"),
      readFile(resolve(root, "vite.config.ts"), "utf8"),
      readFile(
        resolve(
          root,
          "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html",
        ),
        "utf8",
      ),
      readFile(resolve(root, "package.json"), "utf8"),
      readFile(resolve(root, ".gitignore"), "utf8"),
      readFile(resolve(root, "vite.p0a.config.ts"), "utf8"),
      readFile(resolve(root, "public/_headers"), "utf8"),
    ]);

  assert.doesNotMatch(main, /p0a-task-brief|VANE_P0A_OWNER_PREVIEW/);
  assert.doesNotMatch(app, /p0a-task-brief|VANE_P0A_OWNER_PREVIEW/);
  assert.match(vite, /ownerPreview/);
  assert.match(vite, /p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/);
  assert.match(prototypeHtml, /prototypes\/p0a-task-brief\/main\.tsx/);
  assert.match(prototypeHtml, /connect-src 'none'/);
  assert.match(prototypeHtml, /noindex, nofollow, noarchive/);
  const scripts = JSON.parse(packageJson).scripts;
  assert.match(scripts.build, /verify-p0a-production-build\.mjs/);
  assert.match(
    scripts["prototype:p0a:build"],
    /vite build --config vite\.p0a\.config\.ts/,
  );
  assert.match(gitignore, /^\.prototype-dist\/$/m);
  assert.match(p0aVite, /publicDir: false/);
  assert.match(p0aVite, /outDir: path\.resolve\(root, "\.prototype-dist\/p0a"\)/);
  assert.match(p0aVite, /p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/);
  assert.doesNotMatch(p0aVite, /src\/main\.tsx|api\.vane\.zhuoqidev\.com/);
  assert.match(headers, /Cache-Control: no-store/);
  assert.match(headers, /X-Robots-Tag: noindex, nofollow, noarchive/);
  assert.match(headers, /connect-src 'none'/);
  assert.match(headers, /\/_preview\/\*/);
  assert.doesNotMatch(headers, /p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/);
});
