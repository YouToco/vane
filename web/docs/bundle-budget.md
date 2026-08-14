# Authenticated bundle budget

The production Vite build emits `.vite/manifest.json` and a companion
`.vite/bundle-modules.json`. `npm run bundle:check` reconstructs each
authenticated route as bootstrap + `AuthenticatedApp` shell + the selected
page and all static imports, then gzips every unique file at zlib level 9.
The budget is enforced against the default Home route because it is requested
immediately after authentication; the shell alone is diagnostic, not a usable
screen.

| Measurement | Gzip bytes |
| --- | ---: |
| Baseline default route at `f65b91ae43000cb17a9a2a7a2ff8ea4055eadce6` | 277,379 |
| Lazy authenticated shell only (diagnostic) | 198,277 |
| Default Home usable route | 200,665 |
| Default Home reduction | 27.66% |

The hard budget in `config/bundle-budget.json` requires the default Home route
to stay at least 25% below the pinned baseline. The current value below is an
observed checkpoint, not a zero-tolerance ceiling, so normal shared-code
changes can use the remaining budget without weakening the target.

| Authenticated shell + route | Gzip bytes |
| --- | ---: |
| Home | 200,665 |
| Tasks | 206,514 |
| Task detail | 217,074 |
| History | 203,824 |
| Sources | 220,686 |
| Settings | 218,104 |
| Admin | 219,488 |

The gate also checks the shell and every authenticated route graph separately:
Landing, Login, their marketing components, `motion`, `three`, and
`@react-three/fiber` must not enter any of them. Route chunks count only when
the Vite manifest proves they are direct dynamic imports of the expected
boundary.
