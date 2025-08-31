const should = require('should');
const fs = require('fs');
const path = require('path');

describe('Wvlet Language Definition', () => {
  it('should have the language definition file', () => {
    const langPath = path.join(__dirname, '../src/languages/wvlet.js');
    fs.existsSync(langPath).should.be.true();
  });

  it('should export a function for highlight.js', () => {
    const langPath = path.join(__dirname, '../src/languages/wvlet.js');
    const content = fs.readFileSync(langPath, 'utf8');
    content.should.match(/export default function/);
    content.should.match(/hljs/);
  });

  it('should have dist files', () => {
    const distPath = path.join(__dirname, '../dist/wvlet.js');
    const minPath = path.join(__dirname, '../dist/wvlet.min.js');
    fs.existsSync(distPath).should.be.true();
    fs.existsSync(minPath).should.be.true();
  });

  it('should have test fixtures', () => {
    const detectPath = path.join(__dirname, 'detect/wvlet/default.txt');
    const markupPath = path.join(__dirname, 'markup/wvlet/basic.txt');
    fs.existsSync(detectPath).should.be.true();
    fs.existsSync(markupPath).should.be.true();
  });
});