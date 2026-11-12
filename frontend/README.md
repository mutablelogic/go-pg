# Web UI

The web UI of go-pg's servers, starting with pgmanager's databases, their schemas and objects, roles, and the server's connections, settings, tablespaces, replication slots and statements.

- It's built on [`@mutablelogic/go-server`](https://github.com/mutablelogic/go-server/tree/main/frontend), from GitHub Packages, with Lit and Carbon's web components.
- `make frontend` builds it into `build/frontend`. A command built after that, such as with `make`, embeds it with the `frontend` tag, and serves it at `/`, with `/config.json`.
- pgmanager serves it unless `--no-ui` is given, and serves go-server's broadcaster at `/api/changes`, so the UI follows the server.

## Layout

| Folder | What it has | Where it belongs |
|---|---|---|
| `src/core` | Pieces every app of a server needs | **go-server**, in `@mutablelogic/go-server` |
| `src/pgmanager` | pgmanager's API client, types, tiles, table columns and panel bodies | here |
| `src/index.ts` | Wires the app: the configuration, the changes, notifications, the server's status, the tables and the panel | here |
| `assets/dist` | `index.html`, the Carbon shell: a header, a side navigation, the pages, the panel and the notifications | here |
| `cmd` | The Go package which serves the UI, and `/config.json`, behind the `frontend` tag | go-server, later |

## Candidates for go-server

Each file in `src/core` starts with where it belongs. Some are already in go-server:

| File | What it is | In go-server |
|---|---|---|
| `request.ts` | `request`, a JSON request of an API, beside `requestStream` | `/api`, in #202 |
| `list.ts` | `ListController`, of a list an API returns a page at a time (`count` and `body`), which follows the changes, keeps the opened item current, and is sorted, by the server | `/list`, in #202 |
| `table.ts` | `gs-table`, a Carbon data table of a list controller's page, with columns, which sort it when they have a key, and pagination, whose rows open their items | `/list/view`, in #202 |
| `tiles.ts` | `gs-tiles`, a row of tiles of a list controller's page, of which one is chosen, such as a schema, after one for all of them, to show what's in it below | `/list/tiles`, in #202 |
| `router.ts` | `Router`, which shows the page of the location's hash | `/router`, in #202 |
| `nav.ts`, `shell.ts` | `NavController`, and `gs-header` and `gs-side-nav`, the menu bar of its sections, and the sidebar of its pages, with the server's status at its bottom | a new `/shell` |
| `header.ts` | `gs-page-header`, the dark header at the top of a page, with an icon, a title, a subtitle, and actions on the right, such as Create | a new `/page/view` |

Once go-server is released with #202, the UI uses those from `@mutablelogic/go-server`, and its copies here are deleted. go-server's have fixes these don't: out-of-date responses are ignored, and rows open from the keyboard.

Also likely to be shared, once a second app uses them:

- **`cmd`:** serving the embedded UI with `/config.json`, as a `frontend` tag.
- **`formatBytes`,** in `pgmanager/views.ts`, with other servers' formatting.

## pgmanager's own

- **`httpclient.ts`:** the lists, and the items, of the databases, schemas, objects, roles, connections, settings (and their categories), tablespaces, replication slots and statements.
- **`schema.ts`:** their types, as pgmanager's Go schema has them.
- **`views.ts`:** a schema's tile, the objects' type buttons, the settings' categories, the statements' sorts, and each table's columns, and panel's body.

pgqueue's queues and tasks come next, as components which go-pg exports, as `@mutablelogic/go-pg`, for other servers which run pgqueue.

## Developing

| Command | Description |
|---|---|
| `make frontend` | Installs from `package-lock.json`, type checks, and builds into `build/frontend` |
| `make frontend-test` | Type checks, and runs the tests |
| `make` | Builds the UI, and then the commands, which embed it |
| `npm test` | Runs the tests, from the TypeScript, with Node's test runner |

- **Token:** installing needs a token which can read GitHub Packages, in `GITHUB_TOKEN`, as `.npmrc` says, such as `$(gh auth token)`.
- **Node:** the tests need Node 22.18 or later.
- **Docker:** the Docker image builds `cmd/pgmanager` without the UI, as it has no npm.
