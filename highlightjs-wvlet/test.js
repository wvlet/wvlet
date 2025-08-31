const { expect } = require('chai');
const fs = require('fs');
const path = require('path');

describe('Wvlet Language Package', () => {
  describe('Source Files', () => {
    it('should have the main language definition file', () => {
      const langPath = path.join(__dirname, 'src/languages/wvlet.js');
      expect(fs.existsSync(langPath)).to.be.true;
    });

    it('should have valid ES6 module syntax', () => {
      const langPath = path.join(__dirname, 'src/languages/wvlet.js');
      const content = fs.readFileSync(langPath, 'utf8');
      expect(content).to.include('export default function');
      expect(content).to.include('hljs');
    });

    it('should define all major Wvlet keywords', () => {
      const langPath = path.join(__dirname, 'src/languages/wvlet.js');
      const content = fs.readFileSync(langPath, 'utf8');
      // Check for important keywords
      expect(content).to.include('from');
      expect(content).to.include('select');
      expect(content).to.include('where');
      expect(content).to.include('model');
      expect(content).to.include('pivot');
      expect(content).to.include('agg');
    });
  });

  describe('Build Output', () => {
    it('should have generated dist/wvlet.js', () => {
      const distPath = path.join(__dirname, 'dist/wvlet.js');
      expect(fs.existsSync(distPath)).to.be.true;
    });

    it('should have generated dist/wvlet.min.js', () => {
      const minPath = path.join(__dirname, 'dist/wvlet.min.js');
      expect(fs.existsSync(minPath)).to.be.true;
    });

    it('should have valid UMD wrapper in dist/wvlet.js', () => {
      const distPath = path.join(__dirname, 'dist/wvlet.js');
      const content = fs.readFileSync(distPath, 'utf8');
      expect(content).to.include('typeof exports');
      expect(content).to.include('typeof module');
      expect(content).to.include('typeof define');
      expect(content).to.include('hljsDefineWvlet');
    });

    it('should have properly minified dist/wvlet.min.js', () => {
      const minPath = path.join(__dirname, 'dist/wvlet.min.js');
      const minContent = fs.readFileSync(minPath, 'utf8');
      
      // Should have the comment header
      expect(minContent).to.include('Wvlet syntax highlighting');
      
      // Should be actually minified (minimal newlines)
      expect(minContent.split('\n').length).to.be.lessThan(10);
      
      // Should contain the UMD wrapper
      expect(minContent).to.include('typeof exports');
      expect(minContent).to.include('typeof module');
      expect(minContent).to.include('hljsDefineWvlet');
      
      // Should contain key Wvlet keywords
      expect(minContent).to.include('model');
      expect(minContent).to.include('select');
      expect(minContent).to.include('from');
    });
  });

  describe('Test Files', () => {
    it('should have test files for highlight.js integration', () => {
      const detectPath = path.join(__dirname, 'test/detect/wvlet/default.txt');
      const markupPath = path.join(__dirname, 'test/markup/wvlet/basic.txt');
      const expectPath = path.join(__dirname, 'test/markup/wvlet/basic.expect.txt');
      
      expect(fs.existsSync(detectPath)).to.be.true;
      expect(fs.existsSync(markupPath)).to.be.true;
      expect(fs.existsSync(expectPath)).to.be.true;
    });

    it('should have valid Wvlet code in test files', () => {
      const markupPath = path.join(__dirname, 'test/markup/wvlet/basic.txt');
      const content = fs.readFileSync(markupPath, 'utf8');
      
      // Check for Wvlet syntax patterns
      expect(content).to.include('model');
      expect(content).to.include('from');
      expect(content).to.include('--'); // comments
      expect(content).to.include('"""'); // triple quotes
    });
  });

  describe('Package Configuration', () => {
    it('should have correct package.json configuration', () => {
      const pkgPath = path.join(__dirname, 'package.json');
      const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
      
      expect(pkg.name).to.equal('highlightjs-wvlet');
      expect(pkg.main).to.equal('src/languages/wvlet.js');
      expect(pkg.scripts.build).to.exist;
      expect(pkg.scripts.test).to.exist;
      expect(pkg.peerDependencies['highlight.js']).to.exist;
    });

    it('should have required documentation files', () => {
      expect(fs.existsSync(path.join(__dirname, 'README.md'))).to.be.true;
      expect(fs.existsSync(path.join(__dirname, 'LICENSE'))).to.be.true;
    });
  });
});