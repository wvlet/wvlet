import { describe, it, expect } from 'vitest';
// The completion API is exposed directly on the Scala.js WvletJS module used by the LSP server.
import { WvletJS } from '../lib/main.js';
import type { LspCompletionItem } from '../src/types.js';

function complete(
  content: string,
  line: number,
  column: number
): LspCompletionItem[] {
  return JSON.parse(WvletJS.getCompletionItems(content, line, column));
}

describe('WvletJS.getCompletionItems', () => {
  it('should always offer keyword candidates', () => {
    const labels = complete('', 1, 1).map((i) => i.label);
    expect(labels).toContain('from');
    expect(labels).toContain('select');
    expect(labels).toContain('where');
  });

  it('should complete column names of a well-formed query', () => {
    const src = 'from [[1, "alice", 10]] as person(id, name, age)';
    const items = complete(src, 1, src.length + 1);
    const columns = items.filter((i) => i.kind === 5).map((i) => i.label);
    expect(columns).toContain('id');
    expect(columns).toContain('name');
    expect(columns).toContain('age');
  });

  it('should complete in-file model names', () => {
    const src = 'model m1 = {\n  from [[1]] as t(x)\n}\nfrom m1';
    const items = complete(src, 4, 8);
    const model = items.find((i) => i.label === 'm1');
    expect(model).toBeDefined();
    expect(model?.kind).toBe(7);
  });

  it('should not throw on incomplete input', () => {
    const labels = complete('from ', 1, 6).map((i) => i.label);
    expect(labels).toContain('select');
  });
});
