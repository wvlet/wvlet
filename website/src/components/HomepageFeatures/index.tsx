import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Analyze As You Write',
    description: (
      <>
        The flow-style query enables you to analyze data as you write queries for interactive data exploration. 
        Use our <a href='/docs/usage/repl'>interactive REPL</a> or write queries in <a href='https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet'>VS Code</a> with 
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
        title: 'GitHub Integration',
        description: (
            <> Save your queries in GitHub repositories and call them from other queries to ensure reproducibility with
            version control.
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

function Feature({title, Svg, description}: FeatureItem) {
  return (
    <div className={clsx('col col--6')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3" className={'text--left'}>{title}</Heading>
        <p className={'text--left'}>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
