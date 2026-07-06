# pulsar-operator docs site

This is the [Docusaurus](https://docusaurus.io/) site for `pulsar-operator`,
published to GitHub Pages at
[andrew01234567890.github.io/pulsar-operator](https://andrew01234567890.github.io/pulsar-operator/).

## Installation

```bash
pnpm install
```

## Local development

```bash
pnpm start
```

Starts a local dev server and opens a browser window. Most changes reload
live without restarting the server.

## Build

```bash
pnpm build
```

Generates static content into the `build` directory.

## Deployment

Deployment is handled by the `.github/workflows/docs.yml` GitHub Actions
workflow on every push to `main` that touches `docs/**` (or via manual
`workflow_dispatch`) — it builds the site and publishes it to GitHub Pages
through `actions/deploy-pages`. There is no manual `docusaurus deploy` step
and no `gh-pages` branch.
