# Web UI

The web UI of go-pg's servers, starting with pgmanager's databases, their schemas and objects, roles, and the server's connections, settings, tablespaces, replication slots and statements.

- It's built on [`@mutablelogic/go-server`](https://github.com/mutablelogic/go-server/tree/main/frontend), from GitHub Packages, with Lit and Carbon's web components.
- `make frontend` builds it into `frontend/dist`. A command built after that, such as with `make`, embeds it with the `frontend` tag, and serves it at `/`, with `/config.json`.
- pgmanager serves it unless `--no-ui` is given, and serves go-server's broadcaster at `/api/changes`, so the UI follows the server.

## Layout

| Folder | What it has | Where it belongs |
|---|---|---|
| `src/core` | Pieces every app of a server needs, which go-server doesn't have yet | **go-server**, in `@mutablelogic/go-server` |
| `src/pgmanager` | pgmanager's API client, types, tiles, table columns and panel bodies | here |
| `src/index.ts` | Wires the app: the configuration, the changes, notifications, the server's status, the lists, their tables and tiles, and the panel | here |
| `assets/dist` | `index.html`, the Carbon shell: a header, a side navigation, the pages, the panel and the notifications | here |
| `dist` | The UI, which `make frontend` builds, and which isn't tracked | here |
| `embed.go` | The `frontend` package, which embeds `dist` with the `frontend` tag. It's tracked, so `go mod tidy` finds it before the UI is built. | here |
| `cmd` | The Go package which serves the UI, and `/config.json`, behind the `frontend` tag | go-server, later |

## From go-server

The UI uses go-server's `@mutablelogic/go-server` for its configuration, changes, notifications and panel, and for its lists:
- `request`, a JSON request (`/api`);
- `ListController` (`/list`), with `gs-table` (`/list/view`) and `gs-tiles` (`/list/tiles`);
- `Router` (`/router`).

## Candidates for go-server

Each file in `src/core` starts with where it belongs:

| File | What it is | In go-server |
|---|---|---|
| `nav.ts`, `shell.ts` | `NavController`, and `gs-header` and `gs-side-nav`, the menu bar of its sections, and the sidebar of its pages, with the server's status at its bottom | a new `/shell` |
| `header.ts` | `gs-page-header`, the dark header at the top of a page, with an icon, a title, a subtitle, and actions on the right, such as Create | a new `/page/view` |

Also likely to be shared, once a second app uses them:
- **`cmd`:** serving the embedded UI with `/config.json`, as a `frontend` tag.
- **`formatBytes`,** in `pgmanager/views.ts`, with other servers' formatting.
- **`listAll`,** in `pgmanager/httpclient.ts`, which reads every page of a list.

## pgmanager's own

- **`httpclient.ts`:** the lists, and the items, of the databases, schemas, objects, roles, connections, settings (and their categories), tablespaces, replication slots and statements.
- **`schema.ts`:** their types, as pgmanager's Go schema has them, and `ListLimits`, the most of each list it returns at once, which caps the tables' page sizes.
- **`views.ts`:** a schema's tile, the objects' type buttons, the settings' categories, and each table's columns, and panel's body.

pgqueue's queues and tasks come next, as components which go-pg exports, as `@mutablelogic/go-pg`, for other servers which run pgqueue.

## Developing

| Command | Description |
|---|---|
| `make frontend` | Installs from `package-lock.json`, type checks, and builds into `frontend/dist` |
| `make frontend-test` | Type checks, and runs the tests |
| `make` | Builds the UI, and then the commands, which embed it |
| `npm test` | Runs the tests, from the TypeScript, with Node's test runner |

- **Token:** installing needs a token which can read GitHub Packages, in `GITHUB_TOKEN`, as `.npmrc` says, such as `$(gh auth token)`.
- **Node:** the tests need Node 22.18 or later.
- **Docker:** the Docker image builds `cmd/pgmanager` without the UI, as it has no npm.
