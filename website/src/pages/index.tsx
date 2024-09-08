import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
        <div className="container">
            <div className="row">
                <div className={clsx('col col--6')}>
                    <div>
                        <div className='row row--align-center'>
                            <div className='col col--3'>
                               <img src="./img/logo.png" alt="wvlet logo" width={100}/>
                               <h1 lassName={'margin--none'}>wvlet</h1>
                            </div>
                            <div className='col'>
                                <em className={'text--left'}>Wvlet is a cross-SQL flow-style query language for
                                    functional data modeling and interactive data analysis.</em>
                                <div className={styles.buttons}>
                                    <Link className="button button--primary" to="/docs/intro">
                                        Installation
                                    </Link>
                                    <Link className="button button--secondary margin--sm" to="/docs/intro">
                                        Documentation
                                    </Link>
                                </div>

                            </div>
                        </div>
                    </div>
                </div>
                <div className={clsx('col col--6')}>
                    <img className={'shadow--md'} src='./img/demo-query.png' alt='wvlet demo query' width={550}/>
                </div>
            </div>
        </div>
    </header>
  );
}

export default function Home(): JSX.Element {
    const {siteConfig} = useDocusaurusContext();
    return (
        <Layout
            title={`wvlet: Flow-Style Query`}
            description="wvlet: A new flow-style query language">
            <HomepageHeader/>
            <main>
                <HomepageFeatures/>
            </main>
        </Layout>
    );
}
