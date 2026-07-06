import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/intro">
            Introduction
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/quickstart"
            style={{marginLeft: '1rem'}}>
            Quickstart
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Kubernetes operator for Apache Pulsar"
      description="A Kubernetes operator that manages the full lifecycle of Apache Pulsar clusters and autoscales them based on real workload signals.">
      <HomepageHeader />
      <main>
        <div className="container padding-vert--lg">
          <div className="row">
            <div className="col col--4">
              <Heading as="h3">Full lifecycle via CRDs</Heading>
              <p>
                One umbrella <code>PulsarCluster</code> CRD decomposes into
                per-component child resources — Oxia, BookKeeper, Broker,
                Proxy, AutoRecovery, and FunctionsWorker — each reconciled
                independently.
              </p>
            </div>
            <div className="col col--4">
              <Heading as="h3">Autoscale on real signals</Heading>
              <p>
                Brokers scale on Pulsar load-report CPU; bookies scale on
                ledger-disk watermarks. Scale-down is guarded and never
                trades away data safety for elasticity.
              </p>
            </div>
            <div className="col col--4">
              <Heading as="h3">Oxia-native</Heading>
              <p>
                Built around <a href="https://github.com/oxia-db/oxia">Oxia</a>,
                the metadata store that replaces ZooKeeper — no legacy ZK CRD,
                no dual-stack complexity.
              </p>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}
