import { describe, it, expect } from 'vitest';
import { WvletCompiler, CompilationError } from '../src/index.js';

describe('WvletCompiler', () => {
  it('should compile a simple query', () => {
    const compiler = new WvletCompiler();
    const sql = compiler.compile('from users select name, email');
    
    expect(sql).toContain('select name, email');
    expect(sql).toContain('from users');
  });

  it('should compile with select all', () => {
    const compiler = new WvletCompiler();
    const sql = compiler.compile('from users select *');
    
    expect(sql).toContain('select *');
    expect(sql).toContain('from users');
  });

  it('should support different targets', () => {
    const compiler = new WvletCompiler({ target: 'trino' });
    const sql = compiler.compile('from users select name');
    
    expect(sql).toContain('select name');
    expect(sql).toContain('from users');
  });

  it('should handle compilation errors', () => {
    const compiler = new WvletCompiler();
    
    expect(() => compiler.compile('invalid query syntax')).toThrow(CompilationError);
  });

  it('should get compiler version', () => {
    const version = WvletCompiler.getVersion();
    expect(version).toBeTruthy();
    expect(typeof version).toBe('string');
  });

  it('should compile with limit clause', () => {
    const compiler = new WvletCompiler();
    const sql = compiler.compile('from users select * limit 10');
    
    expect(sql).toContain('limit 10');
  });

  it('should compile with where clause', () => {
    const compiler = new WvletCompiler();
    const sql = compiler.compile('from users where age > 18 select name');
    
    expect(sql).toContain('where age > 18');
  });
});