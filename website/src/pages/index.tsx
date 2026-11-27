import clsx from 'clsx';
import Head from '@docusaurus/Head';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import GitHubButton from 'react-github-btn';

import styles from './index.module.css';

const heroQuery = `from orders
where o_orderdate >= '2024-01-01'
group by o_orderpriority
agg _.count as orders, o_totalprice.sum as revenue
order by revenue desc
limit 5`;

// Output of `wvlet compile` for heroQuery, kept in sync by hand.
const heroSql = `select o_orderpriority, count(*) as orders, sum(o_totalprice) as revenue
from orders
where o_orderdate >= '2024-01-01'
group by o_orderpriority
order by revenue desc
limit 5`;

const engines = ['DuckDB', 'Trino', 'Snowflake', 'Hive', 'Wvlet engine'];

/** Soft, slowly drifting color blobs behind a section. */
function Aurora(): React.ReactNode {
  return (
    <div className={styles.aurora} aria-hidden="true">
      <span />
      <span />
      <span />
      <span />
    </div>
  );
}

/** A dark editor-style window frame with traffic-light dots and a title. */
function Window({title, badge, children}: {title: string; badge?: string; children: React.ReactNode}) {
  return (
    <div className={styles.window}>
      <div className={styles.windowBar}>
        <i />
        <i />
        <i />
        <span className={styles.windowTitle}>{title}</span>
        {badge && <span className={styles.windowBadge}>{badge}</span>}
      </div>
      {children}
    </div>
  );
}

function HomepageHero(): React.ReactNode {
  return (
    <header className={styles.hero}>
      <Aurora />
      <div className={styles.heroText}>
        <div className={styles.brand}>
          <img
            className={styles.brandLogo}
            src={useBaseUrl('/img/logos/wvlet-icon.svg')}
            alt=""
            width={72}
            height={72}
          />
          <span className={styles.brandName}>Wvlet</span>
        </div>
        <p className={styles.eyebrow}>
          <span className={styles.dot} />
          Open source · Apache 2.0 · <Link to="/docs/for-ai-agents">Built for AI agents too</Link>
        </p>
        <h1 className={styles.heroTitle}>
          Write queries <br />
          <em className={styles.shimmer}>in the order data flows</em>
        </h1>
        <p className={styles.lede}>
          Wvlet is a cross-SQL, <Link to="/docs/syntax/">flow-style query language</Link> for
          functional data modeling and interactive data exploration. Start from a table, pipe it
          through operators, and compile to SQL for DuckDB, Trino, Snowflake, and Hive.
        </p>
        <div className={styles.ctas}>
          <Link className={clsx(styles.btn, styles.btnSolid)} to="pathname:///playground/">
            Try the playground →
          </Link>
          <Link className={clsx(styles.btn, styles.btnGhost)} to="/docs/">
            Read the docs
          </Link>
          <span className={styles.star}>
            <GitHubButton
              href="https://github.com/wvlet/wvlet"
              data-color-scheme="no-preference: light; light: light; dark: dark;"
              data-size="large"
              data-show-count="true"
              aria-label="Star wvlet/wvlet on GitHub">
              Star
            </GitHubButton>
          </span>
        </div>
      </div>
      <div className={styles.heroVisual}>
        <div className={styles.glow} aria-hidden="true" />
        <Window title="revenue.wv" badge="wvlet">
          <CodeBlock language="wvlet" className={styles.code}>
            {heroQuery}
          </CodeBlock>
        </Window>
        <div className={styles.compiles} aria-hidden="true">
          <span>↓</span> compiles to SQL
        </div>
        <Window title="revenue.sql" badge="duckdb · trino · snowflake">
          <CodeBlock language="sql" className={styles.code}>
            {heroSql}
          </CodeBlock>
        </Window>
      </div>
    </header>
  );
}

function EngineStrip(): React.ReactNode {
  return (
    <section className={styles.engines} aria-label="Supported engines">
      <span className={styles.kicker}>Runs on</span>
      <ul>
        {engines.map((e) => (
          <li key={e}>{e}</li>
        ))}
      </ul>
    </section>
  );
}

function FlowSection(): React.ReactNode {
  return (
    <section className={styles.flow}>
      <div className={styles.sectionHead}>
        <p className={styles.kicker}>Why flow-style</p>
        <h2>
          SQL's syntax order <br />
          <em>isn't the order of your data</em>
        </h2>
        <p>
          A SQL query starts with <code>SELECT</code>, but the engine starts with <code>FROM</code>.
          Wvlet follows the data: each line takes the table from the line above, so you can stop
          anywhere, look at the result, and keep going, like a UNIX pipe.
        </p>
        <ol className={styles.steps}>
          <li>
            <b>01</b>
            <div>
              <h3>Analyze as you write</h3>
              <p>
                Run any prefix of a query in the <Link to="/docs/usage/repl">REPL</Link> or the
                playground to check intermediate results.
              </p>
            </div>
          </li>
          <li>
            <b>02</b>
            <div>
              <h3>Debug in the middle</h3>
              <p>Insert <code>test</code>, <code>debug</code>, or a filter between any two lines without restructuring.</p>
            </div>
          </li>
          <li>
            <b>03</b>
            <div>
              <h3>Same query, any engine</h3>
              <p>Switch from a laptop DuckDB to a Trino cluster without rewriting dialect details.</p>
            </div>
          </li>
        </ol>
      </div>
      <Window title="wv — interactive REPL">
        <img className={styles.demo} src={useBaseUrl('/img/demo.gif')} alt="Wvlet REPL demo query" />
      </Window>
    </section>
  );
}

function ClosingCta(): React.ReactNode {
  return (
    <section className={styles.closing}>
      <Aurora />
      <img
        className={styles.closingLogo}
        src={useBaseUrl('/img/logos/wvlet-icon.svg')}
        alt=""
        width={88}
        height={88}
      />
      <h2>
        Start writing <em className={styles.shimmer}>flow-style</em> queries
      </h2>
      <div className={styles.ctas}>
        <Link className={clsx(styles.btn, styles.btnSolid, styles.btnLg)} to="/docs/usage/install">
          Install Wvlet
        </Link>
        <Link className={clsx(styles.btn, styles.btnGhost, styles.btnLg)} to="pathname:///playground/">
          Open the playground
        </Link>
        <Link
          className={clsx(styles.btn, styles.btnGhost, styles.btnLg)}
          to="https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet">
          VS Code extension
        </Link>
      </div>
    </section>
  );
}

export default function Home(): React.ReactNode {
  return (
    <Layout
      title={`Flow-Style Query Language`}
      description="Wvlet is a new cross-SQL flow-style query language">
      <Head>
        <html className="wvlet-home" />
      </Head>
      <div className={styles.home}>
        <HomepageHero />
        <main>
          <EngineStrip />
          <FlowSection />
          <HomepageFeatures />
          <ClosingCta />
        </main>
      </div>
    </Layout>
  );
}
