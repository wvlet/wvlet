import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  description: React.ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Analyze As You Write',
    description: (
      <>
        The flow-style query enables you to analyze data as you write queries for interactive data exploration. 
        Use our <a href='./docs/usage/repl'>interactive REPL</a> or write queries in <a href='https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet'>VS Code</a> with 
        full syntax highlighting.
      </>
    ),
  },
  {
    title: 'Functional Data Modeling',
    description: (
      <>
        Wvlet enables to build functional (i.e., reusable and composable) data models, which
          can be shared and extended to build reliable data pipelines as in <a href='https://www.getdbt.com'>dbt</a>.
      </>
    ),
  },
  {
        title: 'Cross-SQL Engine Support',
        description: (
            <>
                Wvlet consumes the differences between SQL engines,
                such as <a href='https://duckdb.org'>DuckDB</a>, <a href='https://trino.io/'>Trino</a>, <a href={'https://apache.hive.org/'}>Hive</a>, etc.
                You can switch the SQL engines without changing the query,
                depending on the data size and the expected query performance.
            </>
        ),
  },
  {
      title: 'Extensible',
      description: (
         <>
              Wvlet is extensible with table-value functions or compiler-plugins to add your custom
             query rewrite rules.
          </>
      ),
  },
    {
        title: 'Runs Anywhere',
        description: (
            <>
                Wvlet's built-in columnar query engine runs in-process on the JVM, Node.js, web browsers,
                and as a native binary, so you can query local data files without setting up a database.
            </>
        )
    },
    {
        title: 'Free and Open Source',
        description: (
            <>
                We believe in the power of open-source software and want to help you to exploit more values from your data and queries written with Wvlet.
                Wvlet is free to use and open-sourced under the <a href='https://www.apache.org/licenses/LICENSE-2.0'>Apache License, Version 2.0 (APL2)</a> license.
            </>
        )
    },
];

// Hue offsets around the Wvlet teal, one per card's accent band.
const bandHues = [186, 196, 176, 206, 168, 216];

function Feature({title, description, idx}: FeatureItem & {idx: number}) {
  return (
    <article className={styles.card} style={{'--c': `${bandHues[idx % bandHues.length]} 80% 42%`} as React.CSSProperties}>
      <span className={styles.num}>{String(idx + 1).padStart(2, '0')}</span>
      <Heading as="h3">{title}</Heading>
      <p>{description}</p>
    </article>
  );
}

export default function HomepageFeatures(): React.ReactNode {
  return (
    <section className={styles.features}>
      <div className={styles.head}>
        <p className={styles.kicker}>What you get</p>
        <h2>
          Queries as <em>reusable building blocks</em>
        </h2>
      </div>
      <div className={styles.grid}>
        {FeatureList.map((props, idx) => (
          <Feature key={idx} idx={idx} {...props} />
        ))}
      </div>
    </section>
  );
}
