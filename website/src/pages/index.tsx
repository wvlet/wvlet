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
where o_orderdate >= '1997-01-01'
group by o_orderpriority
agg _.count as orders, o_totalprice.sum as revenue
order by revenue desc
limit 5`;

// Output of `wvlet compile` for heroQuery, kept in sync by hand.
const heroSql = `select o_orderpriority, count(*) as orders, sum(o_totalprice) as revenue
from orders
where o_orderdate >= '1997-01-01'
group by o_orderpriority
order by revenue desc
limit 5`;

// Result of heroQuery on TPC-H scale factor 0.01 (DuckDB dbgen), kept in sync by hand.
const heroRows: [string, string, string][] = [
  ['4-NOT SPECIFIED', '781', '109,207,683.80'],
  ['1-URGENT', '744', '104,483,591.84'],
  ['3-MEDIUM', '700', '100,134,880.58'],
  ['2-HIGH', '715', '99,758,813.31'],
  ['5-LOW', '693', '93,976,264.81'],
];

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

/** A closed band between two waves, so filled areas have soft wave edges on both sides. */
function ribbonPath(top: number, bottom: number, amplitude: number, phase: number): string {
  const upper = wavePath(top, amplitude, 2, phase);
  const lower: string[] = [];
  for (let x = WAVE_W * 2; x >= 0; x -= 16) {
    const y = bottom + amplitude * 0.8 * Math.sin((x / WAVE_W) * 2 * 2 * Math.PI + phase + 0.5);
    lower.push(`L${x},${y.toFixed(1)}`);
  }
  return `${upper} ${lower.join(' ')} Z`;
}

/** Thin stream lines drifting left at a constant speed, to suggest data flowing through a query. */
function Waves({lines = 12, className}: {lines?: number; className?: string}): React.ReactNode {
  return (
    <div className={clsx(styles.waves, className)} aria-hidden="true">
      <svg viewBox={`0 0 ${WAVE_W * 2} ${WAVE_H}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id="wv-wave" gradientUnits="userSpaceOnUse" x1="0" y1="0" x2={WAVE_W * 2} y2="0">
            <stop offset="0" style={{stopColor: 'var(--wave-a)'}} />
            <stop offset="0.25" style={{stopColor: 'var(--wave-b)'}} />
            <stop offset="0.5" style={{stopColor: 'var(--wave-a)'}} />
            <stop offset="0.75" style={{stopColor: 'var(--wave-b)'}} />
            <stop offset="1" style={{stopColor: 'var(--wave-a)'}} />
          </linearGradient>
        </defs>
        {/* Translucent filled bands give the ribbon body; the lines on top give it texture. */}
        {[0, 1].map((i) => (
          <path
            key={`band-${i}`}
            className={styles.waveBand}
            d={ribbonPath(165 + i * 20, 250 + i * 25, 52 - i * 10, 0.3 + i * 0.8)}
            fill="url(#wv-wave)"
            fillOpacity={0.12 - i * 0.04}
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

/**
 * The query's output: its result on the Wvlet engine by default, or the SQL it compiles to.
 * Tabs are CSS-only radio inputs, so the page needs no client state.
 */
function OutputWindow(): React.ReactNode {
  return (
    <div className={clsx(styles.window, styles.outputWindow)}>
      <input type="radio" name="wv-output" id="wv-output-result" defaultChecked />
      <input type="radio" name="wv-output" id="wv-output-sql" />
      <div className={styles.windowBar}>
        <i />
        <i />
        <i />
        <div className={styles.tabs}>
          <label htmlFor="wv-output-result" className={styles.tabResult}>
            Result
          </label>
          <label htmlFor="wv-output-sql" className={styles.tabSql}>
            Compiled SQL
          </label>
        </div>
        <span className={clsx(styles.windowBadge, styles.badgeResult)}>wvlet engine · 5 rows</span>
        <span className={clsx(styles.windowBadge, styles.badgeSql)}>duckdb · trino · snowflake</span>
      </div>
      <div className={styles.output}>
        <div className={styles.panelResult}>
          <table className={styles.resultTable}>
            <thead>
              <tr>
                <th>o_orderpriority</th>
                <th>orders</th>
                <th>revenue</th>
              </tr>
            </thead>
            <tbody>
              {heroRows.map(([p, o, r]) => (
                <tr key={p}>
                  <td>{p}</td>
                  <td>{o}</td>
                  <td>{r}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className={styles.panelSql}>
          <CodeBlock language="sql" className={styles.code}>
            {heroSql}
          </CodeBlock>
        </div>
      </div>
    </div>
  );
}

function HomepageHero(): React.ReactNode {
  return (
    <header className={styles.hero}>
      <Aurora />
      <Waves className={styles.heroWaves} />
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
          Wvlet is a <Link to="/docs/syntax/">flow-style query language</Link> with a columnar
          query engine built in. It runs every TPC-H and TPC-DS query in-process, on the JVM, as a
          native binary, in Node.js, or in the browser. The
          same query also compiles to SQL for DuckDB, Trino, Snowflake, and Hive.
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
          <span>↓</span> runs on the Wvlet engine
        </div>
        <OutputWindow />
      </div>
    </header>
  );
}

function EngineStrip(): React.ReactNode {
  return (
    <section className={styles.engines} aria-label="Where Wvlet runs">
      <div className={styles.engineGroup}>
        <span className={styles.kicker}>Built-in engine</span>
        <ul>
          <li className={styles.ownEngine}>
            Wvlet engine
            <small>JVM · Node.js · Browser · Native</small>
          </li>
        </ul>
      </div>
      <div className={clsx(styles.engineGroup, styles.engineGroupSql)}>
        <span className={styles.kicker}>Compiles to SQL for</span>
        <ul>
          {engines.map((e) => (
            <li key={e}>{e}</li>
          ))}
        </ul>
      </div>
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
          <em className={styles.shimmer}>for modern data analytics</em>
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
          <li>
            <b>04</b>
            <div>
              <h3>Bring your SQL along</h3>
              <p>
                Wvlet reads SQL too: <code>.sql</code> files go through the same compiler as{' '}
                <code>.wv</code> files, and <code>wvlet to_wvlet</code> converts existing SQL into
                flow-style queries, so you can adopt Wvlet one query at a time.
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

type EngineCard = {
  name: string;
  tags: string[];
  badge?: string;
  description: React.ReactNode;
};

const engineCards: EngineCard[] = [
  {
    name: 'Columnar CPU engine',
    tags: ['JVM', 'Native'],
    description: (
      <>
        Vectorized, multi-threaded execution over Parquet, Iceberg, JSON, and CSV, planned by a
        cost-based optimizer. On par with DuckDB on TPC-H and TPC-DS at scale factor 10;{' '}
        <Link to="/docs/engines/wvlet-engine-performance">see the benchmark</Link>.
      </>
    ),
  },
  {
    name: 'Browser & Node.js',
    tags: ['Browser', 'Node.js'],
    description: (
      <>
        The same engine compiled to JavaScript. The{' '}
        <Link to="pathname:///playground/">playground</Link> runs queries in your browser with no
        server.
      </>
    ),
  },
  {
    name: 'GPU acceleration',
    tags: ['NVIDIA', 'cudf'],
    badge: 'Experimental',
    description: (
      <>
        Offload joins and aggregations to the GPU while the rest of the plan stays on the CPU. A
        subtree the device can't run, or that fails on it, runs on the CPU instead.
      </>
    ),
  },
  {
    name: 'SQL engines',
    tags: ['DuckDB', 'Trino', 'Snowflake', 'Hive'],
    description: (
      <>
        When the data lives in a warehouse, compile your query to that engine's SQL dialect and
        run it there.
      </>
    ),
  },
];

const embedSnippet = `import duckdb
from wvlet import compile

sql = compile("""
from 'sales.parquet'
group by region
agg amount.sum as total
""", target="duckdb")

duckdb.connect().execute(sql).fetchdf()`;

const bindings = [
  {name: 'Python', detail: 'pip install wvlet'},
  {name: 'TypeScript / JavaScript', detail: 'npm install wvlet'},
  {name: 'C API', detail: 'libwvlet native library'},
];

/** One query language, a choice of engines to run it on. */
function EnginesSection(): React.ReactNode {
  return (
    <section className={styles.multiEngine}>
      <div className={styles.multiEngineHead}>
        <div>
          <p className={styles.kicker}>Choice of engines</p>
          <h2>
            One size doesn't fit all. <br />
            <em className={styles.shimmer}>Choose the engine that fits.</em>
          </h2>
          <p>
            No single engine fits every workload. An in-process columnar engine suits local files
            and apps, a GPU suits heavy joins and aggregations, and a warehouse suits data that
            already lives there. Wvlet gives you all of these choices in one language, so each
            workload can run where it runs best.
          </p>
        </div>
        <figure className={styles.quote}>
          <blockquote>
            “One size fits all”: an idea whose time has come and gone.
          </blockquote>
          <figcaption>
            Michael Stonebraker &amp; Uğur Çetintemel,{' '}
            <Link to="https://doi.org/10.1109/ICDE.2005.1">ICDE 2005</Link>
          </figcaption>
        </figure>
      </div>
      <div className={styles.engineGrid}>
        {engineCards.map((e) => (
          <article key={e.name} className={styles.engineCard}>
            <div className={styles.engineCardHead}>
              <h3>{e.name}</h3>
              {e.badge && <span className={styles.engineBadge}>{e.badge}</span>}
            </div>
            <p>{e.description}</p>
            <ul className={styles.engineTags}>
              {e.tags.map((t) => (
                <li key={t}>{t}</li>
              ))}
            </ul>
          </article>
        ))}
      </div>
      <div className={styles.embed}>
        <div className={styles.embedText}>
          <p className={styles.kicker}>Embedded</p>
          <h3>Call Wvlet from your own code</h3>
          <p>
            Wvlet is also a library. Compile queries inside your application with the Python and
            TypeScript SDKs, or call the C-compatible native library from other languages, then run
            the SQL on the engine your app already uses.
          </p>
          <ul className={styles.bindings}>
            {bindings.map((b) => (
              <li key={b.name}>
                <b>{b.name}</b>
                <code>{b.detail}</code>
              </li>
            ))}
          </ul>
          <Link className={styles.readMore} to="/docs/bindings/">
            Language bindings →
          </Link>
        </div>
        <Window title="app.py" badge="python">
          <CodeBlock language="python" className={styles.code}>
            {embedSnippet}
          </CodeBlock>
        </Window>
      </div>
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
      description="Wvlet is a flow-style query language with its own query engine that also compiles to SQL">
      <Head>
        <html className="wvlet-home" />
      </Head>
      <div className={styles.home}>
        <HomepageHero />
        <main>
          <EngineStrip />
          <FlowSection />
          <EnginesSection />
          <div className={styles.band}>
            <HomepageFeatures />
          </div>
          <ClosingCta />
        </main>
      </div>
    </Layout>
  );
}
