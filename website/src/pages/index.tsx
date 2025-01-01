import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';
import GitHubButton from 'react-github-btn'

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
        <div className="container">
            <div className="row row--align-center">
                <div className={clsx('col col--6')}>
                    <div>
                        <div className='row row--align-center'>
                            <div className='col col--3'>
                               <img src="./img/logo.png" alt="wvlet logo" width={100}/>
                               <h1>wvlet</h1>
                            </div>
                            <div className='col'>
                                <p className={'text--left'}>
                                <em>Wvlet is a cross-SQL <a href='./docs/syntax/'>flow-style query language</a> for
                                    functional data modeling and interactive data analysis.</em>
                                </p>
                                <div className={styles.buttons}>
                                    <Link className="button button--primary" to="pathname:///playground/">
                                        Try Wvlet!
                                    </Link>
                                    <Link className="button button--secondary margin--sm" to="/docs/">
                                        Documentation
                                    </Link>
                                    <span style={{
                                        width: '110px',
                                        paddingTop: '5px',
                                    }}>
                                        <GitHubButton href="https://github.com/wvlet/wvlet" data-color-scheme="no-preference: light; light: light; dark: dark;" data-size="large" data-show-count="true" aria-label="Star wvlet/wvlet on GitHub">Star</GitHubButton>
                                    </span>
                                </div>

                            </div>
                        </div>
                    </div>
                </div>
                <div className={clsx('col col--6')}>
                <img className={'shadow--md'} src='./img/demo.gif' alt='wvlet demo query' width={650}/>
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
            title={`Flow-Style Query Language`}
            description="Wvlet is a new cross-SQL flow-style query language">
            <HomepageHeader/>
            <main>
                <HomepageFeatures/>
            </main>
        </Layout>
    );
}
