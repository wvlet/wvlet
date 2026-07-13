import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
// The go-to-definition API is exposed directly on the Scala.js WvletJS module used by the LSP server.
import { WvletJS } from '../lib/main.js';
import type { LspDefinition } from '../src/types.js';

function definition(
  content: string,
  line: number,
  column: number
): LspDefinition | null {
  return JSON.parse(WvletJS.getDefinition(content, line, column));
}

describe('WvletJS.getDefinition', () => {
  it('should jump from a model reference to its definition', () => {
    const src =
      'model my_model = {\n  from [[1, "alice", 10]] as person(id, name, age)\n}\nfrom my_model';
    // Cursor on `my_model` in the last line (line 4, column 7)
    const result = definition(src, 4, 7);
    expect(result).not.toBeNull();
    expect(result?.startLine).toBe(1);
    expect(result?.startColumn).toBe(1);
  });

  it('should jump from a type reference to its definition', () => {
    const src =
      'type point = {\n  x: long\n  y: long\n}\ntype line = {\n  start: point\n  stop: point\n}';
    // Cursor on the `point` reference in the `line` definition (line 6, column 10)
    const result = definition(src, 6, 10);
    expect(result).not.toBeNull();
    expect(result?.startLine).toBe(1);
  });

  it('should return null when the cursor is on the definition itself', () => {
    const src =
      'model my_model = {\n  from [[1, "alice", 10]] as person(id, name, age)\n}\nfrom my_model';
    // Cursor on `my_model` within its own definition (line 1, column 7)
    expect(definition(src, 1, 7)).toBeNull();
  });

  it('should return null for an unknown reference', () => {
    expect(definition('from unknown_model', 1, 7)).toBeNull();
  });

  it('should not throw on incomplete input', () => {
    expect(definition('from ', 1, 6)).toBeNull();
  });
});

// Cross-file navigation: once setWorkspacePath points at a project folder, definitions in
// other workspace files resolve and carry the defining file's path.
describe('WvletJS.getDefinition across workspace files', () => {
  const defsSource =
    'model shared_model = {\n  from [[1, "alice", 10]] as person(id, name, age)\n}\n';

  let projectDir: string;
  let defsPath: string;

  beforeAll(() => {
    projectDir = mkdtempSync(join(tmpdir(), 'wvlet-definition-'));
    defsPath = join(projectDir, 'defs.wv');
    writeFileSync(defsPath, defsSource);
    WvletJS.setWorkspacePath(projectDir);
  });

  afterAll(() => {
    // Restore the default so other test files keep seeing an empty workspace
    WvletJS.setWorkspacePath('.');
    if (projectDir) {
      rmSync(projectDir, { recursive: true, force: true });
    }
  });

  it('should report the defining file path for a model in another workspace file', () => {
    // Cursor on `shared_model` (line 1, column 7)
    const result = definition('from shared_model', 1, 7);
    expect(result).not.toBeNull();
    expect(result?.path).toBe(defsPath);
    // `model shared_model` starts on line 1 of defs.wv
    expect(result?.startLine).toBe(1);
    expect(result?.startColumn).toBe(1);
  });

  it('should not report a path for a same-document definition', () => {
    const src =
      'model my_model = {\n  from shared_model\n}\nfrom my_model';
    // Cursor on `my_model` in the last line (line 4, column 7)
    const result = definition(src, 4, 7);
    expect(result).not.toBeNull();
    expect(result?.startLine).toBe(1);
    expect(result?.path ?? null).toBeNull();
  });
});
