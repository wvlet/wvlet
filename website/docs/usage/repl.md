---
sidebar_label: Interactive Shell (wv)
sidebar_position: 2
---

# Interactive Shell

Wvlet Shell (`wv` command) launches an interactive query editor for the console. In the shell, you can write SQL queries, run them, and see the results interactively.

![wvlet shell](/img/demo.gif)

## Commands

By typing `help` in the shell, you can see the list of available commands:

```bash
$ wv 
wv> help
[commands]
 help       : Show this help message
 quit/exit  : Exit the REPL
 clear      : Clear the screen
 context    : Show current database context (catalog and schema)
 clip       : Clip the last query and result to the clipboard
 clip-result: Clip the last result to the clipboard in TSV format
 rows       : Set the maximum number of query result rows to display (default: 40)
 col-width  : Set the maximum column width to display (default: 150)
 git        : Run a git command in the shell
 gh         : Run a GitHub command in the shell
```

## Shortcut Keys

Wvlet shell basically uses [GNU readline](https://readline.kablamo.org/emacs.html)-style shortcut keys. Here is the list of shortcut keys that are most frequently used in the shell: 

### Navigation

| Keys | Description                                                                      | 
|---|----------------------------------------------------------------------------------|
| ctrl-a | Home: Move to the beginning of the line                                          |
| ctrl-e | End: Move to the end of the line                                                 |
| ctrl-j a | Move to the beginning of the query                                               |
| ctrl-j e | Move to the end of the query                                                     |
| ctrl-p | Up                                                                               | 
| ctrl-n | Down                                                                             |
| ctrl-f | Right                                                                            |
| ctrl-b | Left                                                                             |
| ctrl-r | Search the history. Type ctrl+r again to continue to search the previous history | 
| ctrl-k | Delete until the end of line from the cursor position                            | 

### Running Queries

| Keys | Description |
|---|---|
| ctrl-j ctrl-r | Run the whole query | 
| ctrl-j ctrl-t | Test run the query fragment up to the line with debug mode |
| ctrl-j ctrl-d | Describe the schema of the current line of the query fragment |
| ctrl-c | Cancel the current query, or exit the shell |

## Switching Database Context

You can switch the database schema context in the REPL using the `use` statement:

```sql
-- Switch to a different schema
wv> use schema analytics

-- Switch to a schema in a different catalog
wv> use schema production.analytics

-- Show current context
wv> context
Current context: catalog=memory, schema=analytics
```

The context switch affects all subsequent queries in the REPL session. Note that catalog switching support may be limited depending on the database backend.

