wvlet-ui-main
---

Web UI module for Wvlet.

## Development Notes

To develop Wvlet UI, you need to open three terminals:

__RPC Server__ 

```sh
$ ./sbt
sbt:wvlet> ~server/reStart server
```

__Building UI with Scala.js__

```sh
$ ./sbt
sbt:wvlet> ~uiMain/fastLinkJS
```

When adding a new RPC method to the interface, you need to run the following command to regenerate an RPC client (via sbt-airframe plugin): 

```sh
sbt:wvlet> clientJS/airframeHttpReload
```

__Run a local web server for the UI__
```sh
$ npm run ui
```
