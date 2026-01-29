# Plan: Declarative Table Schema Resolution for New Typer

**Date**: 2026-01-28
**Issue**: #392 - Redesign Typer
**Goal**: Enable compilation without live database connections by using explicit Type definitions as table schemas.

## Summary

Instead of querying catalogs at compile time, define table schemas as `type` definitions in `.wv` files. The Typer resolves `from tableName` by looking up the corresponding type definition. A pre-scan/sync mechanism generates these type definitions from catalog metadata.

## Key Design Principle

**Before (current)**: `from users` → query live catalog → get schema
**After (new)**: `from users` → lookup `type users` in symbol table → use pre-defined schema

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   Pre-Compilation Phase                     │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Catalog Sync (wv catalog sync)                       │  │
│  │  - Connect to DB, fetch table metadata                │  │
│  │  - Generate .wv files with type definitions           │  │
│  │  - Store in catalog/ or .wvlet/schemas/ folder        │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Compilation Phase                        │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Typer.tableRefRules                                  │  │
│  │  from users → lookup type users → TableScan(schema)   │  │
│  │  No catalog connection needed!                        │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Generated Schema Files

**Location**: `target/.cache/wvlet/schemas/` (in build output folder, regenerated as needed)

```wvlet
-- target/.cache/wvlet/schemas/main.wv (auto-generated from catalog sync)
-- Schema: main, synced at 2026-01-28T10:00:00Z

type users = {
  id: long
  name: string
  email: string
  created_at: timestamp
}

type orders = {
  id: long
  user_id: long
  amount: decimal(10,2)
  status: string
}
```

## Implementation Steps

### Step 1: Table Type Resolution in Typer

**File**: `wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/TyperRules.scala`

Add rule to resolve TableRef via type lookup:

```scala
def tableRefRules(using ctx: Context): PartialFunction[Relation, Relation] =
  case ref: TableRef if !ref.relationType.isResolved =>
    resolveTableRef(ref)
  case ref: FileRef if !ref.relationType.isResolved =>
    resolveFileRef(ref)
  case call: TableFunctionCall if !call.relationType.isResolved =>
    resolveTableFunctionCall(call)

private def resolveTableRef(ref: TableRef)(using ctx: Context): Relation =
  val name = ref.name

  // 1. Look up as Model definition
  lookupSymbol(name.toTermName, ctx) match
    case Some(sym) if sym.tree.isInstanceOf[ModelDef] =>
      val m = sym.tree.asInstanceOf[ModelDef]
      return ModelScan(TableName(name.fullName), Nil, m.child.relationType, ref.span)
    case _ => ()

  // 2. Look up as Type definition (NEW: table schema from type)
  lookupType(Name.typeName(name.leafName), ctx) match
    case Some(sym) =>
      sym.symbolInfo.dataType match
        case schema: SchemaType =>
          // Found type definition - use as table schema
          val tableName = TableName.parse(name.fullName)
          return TableScan(tableName, schema, schema.fields, ref.span)
        case _ => ()
    case None => ()

  // 3. Fallback to catalog lookup (for migration compatibility)
  val tableName = TableName.parse(name.fullName)
  ctx.catalog.findTable(
    tableName.schema.getOrElse(ctx.defaultSchema),
    tableName.name
  ) match
    case Some(tbl) =>
      TableScan(tableName, tbl.schemaType, tbl.schemaType.fields, ref.span)
    case None =>
      // Leave unresolved
      ref
```

### Step 2: Integrate in Typer.typeRelation()

**File**: `wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/Typer.scala`

```scala
private def typeRelation(r: Relation)(using ctx: Context): Unit =
  // Type children first (bottom-up)
  r.children.foreach {
    case child: Relation => typeRelation(child)
    case child: LogicalPlan => typePlan(child)
  }

  // Apply table reference rules
  r match
    case ref: TableRef if !ref.relationType.isResolved =>
      val resolved = TyperRules.tableRefRules.applyOrElse(r, identity[Relation])
      if resolved ne r then
        resolved.copyMetadataFrom(r)
        resolved.tpe = resolved.relationType
        // Note: parent update handled by tree transformation
    case _ => ()

  // Continue with expression typing...
  r.tpe = r.relationType
```

### Step 3: Local File Schema Caching

For local files (JSON, Parquet, CSV), cache inferred schemas based on mtime:

**File**: `wvlet-lang/src/main/scala/wvlet/lang/compiler/WorkEnv.scala`

```scala
case class CachedFileSchema(
    filePath: String,
    schema: SchemaType,
    lastModified: Long
)

// Add to WorkEnv trait
def loadFileSchemaCache(filePath: String): Option[CachedFileSchema]
def saveFileSchemaCache(filePath: String, schema: SchemaType, lastModified: Long): Unit
```

**File**: `wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/TyperRules.scala`

```scala
private def resolveFileRef(ref: FileRef)(using ctx: Context): Relation =
  val filePath = ctx.dataFilePath(ref.filePath)
  val mtime = SourceIO.lastUpdatedAt(filePath)

  // Check cache
  ctx.workEnv.loadFileSchemaCache(filePath) match
    case Some(cached) if cached.lastModified == mtime =>
      FileScan(SingleQuoteString(filePath, ref.span), cached.schema, cached.schema.fields, ref.span)
    case _ =>
      // Infer and cache
      val schema = inferFileSchema(filePath)
      ctx.workEnv.saveFileSchemaCache(filePath, schema, mtime)
      FileScan(SingleQuoteString(filePath, ref.span), schema, schema.fields, ref.span)
```

### Step 4: Catalog Sync Command (Future CLI Integration)

**Concept** (implementation in wvlet-cli module):

```bash
# Sync catalog metadata to target/.cache/wvlet/schemas/
wv catalog sync --profile mydb

# Generated files:
# target/.cache/wvlet/schemas/mydb/public.wv
# target/.cache/wvlet/schemas/mydb/analytics.wv
```

The sync command would:
1. Connect to database using profile
2. Query `information_schema.columns`
3. Generate `.wv` files with type definitions
4. Store timestamp for incremental sync

**Note**: Generated files in `target/` are gitignored but can be checked in if desired for reproducible offline builds.

## Files to Modify

| File | Changes |
|------|---------|
| `wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/TyperRules.scala` | Add `tableRefRules` with type-based resolution |
| `wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/Typer.scala` | Integrate table rules in `typeRelation()` |
| `wvlet-lang/src/main/scala/wvlet/lang/compiler/WorkEnv.scala` | Add `CachedFileSchema` and cache methods |
| `wvlet-lang/.jvm/src/main/scala/wvlet/lang/compiler/WorkEnvCompat.scala` | Implement file schema cache persistence |
| `wvlet-lang/.js/src/main/scala/wvlet/lang/compiler/WorkEnvCompat.scala` | In-memory cache (no persistence) |
| `wvlet-lang/.native/src/main/scala/wvlet/lang/compiler/WorkEnvCompat.scala` | File schema cache persistence |

## Testing Strategy

1. **TyperTest.scala**: Test type-based table resolution
   ```scala
   test("resolve table ref via type definition") {
     val source = """
       type users = { id: long, name: string }
       from users
     """
     // Verify TableScan has schema from type users
   }
   ```

2. **File schema caching tests**:
   - Cache hit when file unchanged
   - Cache invalidation when mtime changes

3. **Spec tests** in `spec/basic/`:
   - Add test `.wv` files with type definitions as table schemas

## Verification Commands

```bash
# Run typer tests
./sbt "langJVM/testOnly *TyperTest"

# Run validation tests
./sbt "langJVM/testOnly *TyperValidationTest"

# Full test suite
./sbt "langJVM/test"

# Cross-platform compilation
./sbt "langJS/compile" && ./sbt "langNative/compile"
```

## Migration Path

1. **Phase 1 (This PR)**: Type-based resolution in Typer
   - `from tableName` looks up `type tableName` first
   - Falls back to live catalog if type not found (gradual migration)
   - Both paths produce same TableScan output

2. **Phase 2**: File schema caching for local files
   - JSON/Parquet/CSV schemas cached by mtime in `target/.cache/wvlet/`

3. **Phase 3**: `wv catalog sync` CLI command
   - Generate type definitions from live catalog to `target/.cache/wvlet/schemas/`
   - Enable fully offline compilation when schemas are pre-synced

4. **Phase 4 (Optional)**: Strict offline mode
   - Compiler flag to disable catalog fallback
   - Fail fast if type definition not found

## Benefits

1. **Offline Compilation**: No database connection needed once schemas are synced
2. **Version Control**: Schema definitions in `.wv` files can be committed to git
3. **Reproducible Builds**: Same schema used across all developers
4. **IDE Support**: Type definitions enable better autocompletion
5. **Schema Evolution**: Track schema changes in git history
