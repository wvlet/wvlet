const Prism = require('prismjs');
require('../components/prism-wvlet');

describe('Prism Wvlet Grammar', () => {
  test('should highlight keywords', () => {
    const code = 'model User = { from "users.json" }';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    // Check for keyword tokens (they might be in function-definition or keyword)
    const keywordTokens = tokens.filter(t => t.type === 'keyword' || t.type === 'function-definition');
    expect(keywordTokens.length).toBeGreaterThan(0);
    
    // Check that we have model or from keywords
    const hasModelOrFrom = tokens.some(t => 
      (t.type === 'keyword' && (t.content === 'model' || t.content === 'from')) ||
      (t.type === 'function-definition' && t.content && t.content.includes('model'))
    );
    expect(hasModelOrFrom).toBe(true);
  });

  test('should handle string interpolation', () => {
    const code = '"Hello ${name}!"';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const stringToken = tokens.find(t => t.type === 'string');
    expect(stringToken).toBeDefined();
    
    // Check that interpolation is parsed (it will be a nested structure)
    expect(Array.isArray(stringToken.content)).toBe(true);
  });

  test('should recognize model definitions', () => {
    const code = 'model UserProfile(id: Int) = { from "users.json" }';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const functionDefToken = tokens.find(t => t.type === 'function-definition');
    expect(functionDefToken).toBeDefined();
  });

  test('should highlight numbers', () => {
    const code = '42 3.14 100L 2.5f 1e10';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const numberTokens = tokens.filter(t => t.type === 'number');
    expect(numberTokens.length).toBe(5);
  });

  test('should handle comments', () => {
    const code = '-- This is a comment\nmodel Test = {}';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const commentToken = tokens.find(t => t.type === 'comment');
    expect(commentToken).toBeDefined();
    expect(commentToken.content).toBe('-- This is a comment');
  });

  test('should handle doc comments', () => {
    const code = '---\nThis is a doc comment\n@param id User ID\n---';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const docCommentToken = tokens.find(t => t.type === 'comment');
    expect(docCommentToken).toBeDefined();
  });

  test('should treat both doc comments and line comments as comment type', () => {
    const code = `-- Line comment
---
Doc comment with @param
---
model Test = {}`;
    
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    // Both comment types should be tokenized as 'comment'
    const commentTokens = tokens.filter(t => t.type === 'comment');
    expect(commentTokens.length).toBe(2);
    
    // Verify line comment
    const lineComment = commentTokens.find(t => 
      t.content === '-- Line comment'
    );
    expect(lineComment).toBeDefined();
    
    // Verify doc comment and its inner doctag
    const docComment = commentTokens.find(t => Array.isArray(t.content));
    expect(docComment).toBeDefined();
    const doctagToken = docComment.content.find(t => t.type === 'doctag');
    expect(doctagToken).toBeDefined();
    expect(doctagToken.content).toBe('@param');
  });

  test('should recognize boolean literals', () => {
    const code = 'true false null';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const booleanTokens = tokens.filter(t => t.type === 'boolean');
    expect(booleanTokens.length).toBe(3);
  });

  test('should handle operators', () => {
    const code = '!= <= >= == && ||';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const operatorTokens = tokens.filter(t => t.type === 'operator');
    expect(operatorTokens.length).toBe(6);
  });

  test('should recognize function calls', () => {
    const code = 'count(distinct user_id)';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    // 'distinct' and 'count' might be keywords, check for any function-like patterns
    const hasFunction = tokens.some(t => 
      t.type === 'function' || 
      (t.type === 'keyword' && (t.content === 'count' || t.content === 'distinct'))
    );
    expect(hasFunction).toBe(true);
  });

  test('should handle backquoted identifiers', () => {
    const code = '`column with spaces`';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const symbolToken = tokens.find(t => t.type === 'symbol');
    expect(symbolToken).toBeDefined();
    expect(symbolToken.content).toBe('`column with spaces`');
  });

  test('should support wv alias', () => {
    // Test that wv language exists and is the same as wvlet
    expect(Prism.languages.wv).toBeDefined();
    expect(Prism.languages.wv).toBe(Prism.languages.wvlet);
  });

  test('should handle triple-quoted strings', () => {
    const code = '"""Multi\nline\nstring"""';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    const templateStringToken = tokens.find(t => t.type === 'template-string');
    expect(templateStringToken).toBeDefined();
  });

  test('should recognize test assertions', () => {
    const code = 'test _.size should be > 0';
    const tokens = Prism.tokenize(code, Prism.languages.wvlet);
    
    // Test keywords might be recognized as keywords instead of annotation
    const hasTestKeywords = tokens.some(t => 
      t.type === 'annotation' || 
      (t.type === 'keyword' && (t.content === 'test' || t.content === 'should' || t.content === 'be'))
    );
    expect(hasTestKeywords).toBe(true);
  });
});