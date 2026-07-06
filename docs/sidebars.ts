import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    'quickstart',
    'autoscaling',
    'high-availability',
    'backup-and-dr',
    'metadata-store-oxia',
    'crd-api-reference',
    'kaap-comparison',
  ],
};

export default sidebars;
