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
        Flow-style query enables you to analyze data as you write queries for interactive data exploration.
      </>
    ),
  },
  {
    title: 'Functional Data Modeling',
    description: (
      <>
        Wvlet enables to build functional (i.e., reusable and composable) data models, which
          can be shared and extended to build reliable data pipelines.
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
                depending on your data size and the expected query performance.
            </>
        ),
  },
  {
      title: 'Exensible',
      description: (
         <>
              You can extend wvlet through table-value functions or compiler-plugins to add your custom
             query rewrite rules.
          </>
      ),
    },
];

function Feature({title, Svg, description}: FeatureItem) {
  return (
    <div className={clsx('col col--6')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
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
