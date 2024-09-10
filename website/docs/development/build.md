# Building Wvlet

To build wvlet, you will need JDK17 or later. To test Trino connector, JDK22 or later is required (as of September 2024).  

```bash
$ gh repo clone wvlet/wvlet
$ cd wvlet
$ ./sbt
## This will install wv command to your ~/local/bin
> runner/packInstall
```



## Building Documentation Site

Wvlet Documentation https://wvlet.org/wvlet is built with Docusaurus and GitHub Pages. 

```bash
$ cd website
# Start a documentation server, which will be 
# reloaded automatically when you update the documentation
$ npm start
```

`website/docs/` directory contains the markdown files for the documentation. Once your change is merged to the main branch, GitHub Action will update the web site automatically.


