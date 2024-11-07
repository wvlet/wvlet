# Building Wvlet


## Building from Source 

To build Wvlet, you will need at least JDK17 or later. To test Trino connector, JDK23 or later is required (as of November 2024).  

```bash
# Clone the source code repository
git clone git@github.com:wvlet/wvlet.git
cd wvlet
./sbt
## This will install wv command to your ~/local/bin
sbt:wvlet> cli/packInstall
```

You can find the wvlet command in `~/local/bin/wv`. For convenience, you can set `PATH` environment variable like this:
```bash title='~/.bashenv'
export PATH=$HOME/local/bin:$PATH
```

:::info
Mac users can install wvlet with Homebrew: [`brew install wvlet/wvlet/wvlet`](../usage/install.md)
:::


## Building Documentation 

Wvlet Documentation https://wvlet.org/wvlet is built with Docusaurus and GitHub Pages. To edit the documentation, you can start a local server to preview the documentation changes: 

```bash
cd website
```

Then start a documentation server at localhost:3000:
```bash
npm start
```

The server will be reloaded automatically when you update .md files.


`website/docs/` directory contains the markdown files for the documentation. Once your change is merged to the main branch, GitHub Action will update the public website automatically.


