import { describe, it, expect } from 'vitest';
// The hover API is exposed directly on the Scala.js WvletJS module used by the LSP server.
import { WvletJS } from '../lib/main.js';
import type { LspHover } from '../src/types.js';

function hover(
  content: string,
  line: number,
  column: number
): LspHover | null {
  return JSON.parse(WvletJS.getHover(content, line, column));
}

describe('WvletJS.getHover', () => {
  it('should show the model schema for a model reference', () => {
    const src =
      'model my_model = {\n  from [[1, "alice", 10]] as person(id, name, age)\n}\nfrom my_model';
    // Cursor on `my_model` in the last line (1-based line 4, column ~7)
    const result = hover(src, 4, 7);
    expect(result).not.toBeNull();
    expect(result?.content).toContain('model my_model');
    expect(result?.content).toContain('name: string');
  });

  it('should show name and type for a column reference', () => {
    const src = 'from [[1, "alice", 10]] as person(id, name, age)\nselect name';
    // Cursor on `name` in the select (line 2, column 8)
    const result = hover(src, 2, 8);
    expect(result).not.toBeNull();
    expect(result?.content).toContain('name: string');
  });

  it('should return null when hovering an empty position', () => {
    expect(hover('', 1, 1)).toBeNull();
  });

  it('should not throw on incomplete input', () => {
    expect(hover('from ', 1, 6)).toBeNull();
  });
});
