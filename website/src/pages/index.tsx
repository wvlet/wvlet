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

const engines = ['DuckDB', 'Trino', 'Snowflake', 'Hive'];

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

const WAVE_W = 1440;
const WAVE_H = 320;

/**
 * Path of a sine wave spanning two tiles (2 * WAVE_W). Every period divides WAVE_W, so shifting the
 * path left by exactly one tile loops seamlessly.
 */
function wavePath(baseline: number, amplitude: number, periods: number, phase: number): string {
  const pts: string[] = [];
  for (let x = 0; x <= WAVE_W * 2; x += 16) {
    const y = baseline + amplitude * Math.sin((x / WAVE_W) * periods * 2 * Math.PI + phase);
    pts.push(`${x === 0 ? 'M' : 'L'}${x},${y.toFixed(1)}`);
  }
  return pts.join(' ');
}

/** Thin stream lines drifting left at a constant speed, to suggest data flowing through a query. */
function Waves({lines = 12}: {lines?: number}): React.ReactNode {
  return (
    <div className={styles.waves} aria-hidden="true">
      <svg viewBox={`0 0 ${WAVE_W * 2} ${WAVE_H}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id="wv-wave" gradientUnits="userSpaceOnUse" x1="0" y1="0" x2={WAVE_W * 2} y2="0">
            <stop offset="0" stopColor="hsl(186 90% 45%)" />
            <stop offset="0.25" stopColor="hsl(160 70% 45%)" />
            <stop offset="0.5" stopColor="hsl(186 90% 45%)" />
            <stop offset="0.75" stopColor="hsl(160 70% 45%)" />
            <stop offset="1" stopColor="hsl(186 90% 45%)" />
          </linearGradient>
        </defs>
        {/* Translucent filled bands give the ribbon body; the lines on top give it texture. */}
        {[0, 1].map((i) => (
          <path
            key={`band-${i}`}
            className={styles.waveBand}
            d={`${wavePath(190 + i * 40, 52 - i * 12, 2, 0.4 + i * 0.9)} L${WAVE_W * 2},${WAVE_H} L0,${WAVE_H} Z`}
            fill="url(#wv-wave)"
            fillOpacity={0.1 - i * 0.03}
          />
        ))}
        {Array.from({length: lines}, (_, i) => (
          <path
            key={i}
            d={wavePath(170 + i * 9, 56 - i * 3, 2, i * 0.16)}
            stroke="url(#wv-wave)"
            strokeOpacity={0.75 - i * (0.55 / lines)}
            strokeWidth={i < 3 ? 2.5 : 1.5}
            vectorEffect="non-scaling-stroke"
          />
        ))}
      </svg>
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
      <Waves />
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
        <li className={styles.ownEngine}>
          Wvlet engine
          <small>JVM · Node.js · Browser · Native</small>
        </li>
      </ul>
    </section>
  );
}

function FlowSection(): React.ReactNode {
  return (
    <section className={styles.flow}>
      <div className={styles.sectionHead}>
        <p className={styles.kicker}>Why Wvlet</p>
        <h2>
          Redesigning <span className={styles.nowrap}>50-year-old</span> SQL <br />
          <em>for modern data analytics</em>
        </h2>
        <p>
          SQL was born in the 1970s. Its syntax order doesn't match how data flows through a query,
          which makes deeply nested queries hard to read and debug, even for experts, and the
          standard has no way to reuse, test, or debug queries. Wvlet modernizes SQL with
          software engineering practices built in.
        </p>
        <ol className={styles.steps}>
          <li>
            <b>01</b>
            <div>
              <h3>Syntax that follows the data</h3>
              <p>
                Start from <code>from</code>, then pipe the table through <code>where</code>,{' '}
                <code>group by</code>, and <code>select</code>. Run any prefix of a query in the{' '}
                <Link to="/docs/usage/repl">REPL</Link> to check intermediate results.
              </p>
            </div>
          </li>
          <li>
            <b>02</b>
            <div>
              <h3>Reusable, composable queries</h3>
              <p>
                Define <Link to="/docs/syntax/data-models">data models</Link> and functions once,
                then build on them across queries and pipelines.
              </p>
            </div>
          </li>
          <li>
            <b>03</b>
            <div>
              <h3>Debugging and testing built in</h3>
              <p>
                Insert <code>debug</code> or <code>test</code> between any two lines without
                restructuring the query.
              </p>
            </div>
          </li>
        </ol>
        <p className={styles.readMore}>
          <Link to="/blog/release-2024-9">
            Read: Wvlet: Redesigning 50-Year-Old SQL for Modern Data Analytics →
          </Link>
        </p>
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
      <div className={styles.closingPanel}>
        <Waves lines={9} />
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
          <div className={styles.band}>
            <HomepageFeatures />
          </div>
          <ClosingCta />
        </main>
      </div>
    </Layout>
  );
}
