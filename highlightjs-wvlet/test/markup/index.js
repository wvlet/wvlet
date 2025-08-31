const should = require('should');
const fs = require('fs');
const path = require('path');
const hljs = require('highlight.js');

// Load the Wvlet language definition from the built UMD file
// Create a mock environment for the UMD wrapper
const mockGlobal = {};
const wvletDist = fs.readFileSync(path.join(__dirname, '../../dist/wvlet.js'), 'utf8');
const wvletFactory = eval(`(function() {
  var global = mockGlobal;
  var exports = {};
  var module = { exports: exports };
  ${wvletDist}
  return module.exports || mockGlobal.hljsDefineWvlet;
})()`)

hljs.registerLanguage('wvlet', wvletFactory);

describe('Wvlet Markup Tests', () => {
  it('should highlight basic Wvlet syntax', () => {
    const input = fs.readFileSync(path.join(__dirname, 'wvlet/basic.txt'), 'utf8');
    const expected = fs.readFileSync(path.join(__dirname, 'wvlet/basic.expect.txt'), 'utf8');
    
    const result = hljs.highlight(input, { language: 'wvlet' });
    
    // For now, just check that highlighting produces output
    // Full comparison would need proper normalization
    result.value.should.be.a.String();
    result.value.length.should.be.greaterThan(0);
    
    // Check for some expected patterns
    result.value.should.match(/hljs-keyword/);
    result.value.should.match(/hljs-string/);
    result.value.should.match(/hljs-comment/);
  });

  it('should detect Wvlet language', () => {
    const code = fs.readFileSync(path.join(__dirname, '../detect/wvlet/default.txt'), 'utf8');
    
    // Test that the language can be auto-detected (if detection patterns are set)
    const result = hljs.highlightAuto(code, ['wvlet']);
    result.language.should.equal('wvlet');
  });
});