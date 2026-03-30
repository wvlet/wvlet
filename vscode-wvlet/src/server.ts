import {
  createConnection,
  TextDocuments,
  Diagnostic,
  DiagnosticSeverity,
  ProposedFeatures,
  InitializeParams,
  InitializeResult,
  TextDocumentSyncKind,
  DocumentSymbol,
  SymbolKind,
  Range,
  Position,
} from 'vscode-languageserver/node';

import { TextDocument } from 'vscode-languageserver-textdocument';

import type { WvletJSType } from '../../sdks/typescript/lib/main';
import type { LspDiagnostic, LspSymbol } from '../../sdks/typescript/src/types';

// The Scala.js module exports WvletJS as a named export
interface WvletJSModule {
  WvletJS: WvletJSType;
}

// Create the LSP connection
const connection = createConnection(ProposedFeatures.all);

// Create a text document manager
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);

// Cached reference to the Scala.js compiler module
let wvletModule: WvletJSModule | null = null;
let moduleLoadFailed = false;

async function getWvletJS(): Promise<WvletJSModule | null> {
  if (wvletModule) {
    return wvletModule;
  }
  if (moduleLoadFailed) {
    return null;
  }
  try {
    // Dynamic import of the Scala.js ESModule output
    wvletModule = await import('../../sdks/typescript/lib/main.js');
    connection.console.log(
      `Wvlet compiler loaded (version: ${wvletModule!.WvletJS.getVersion()})`
    );
    return wvletModule;
  } catch (e) {
    moduleLoadFailed = true;
    connection.console.error(
      `Failed to load Wvlet compiler: ${e instanceof Error ? e.message : String(e)}`
    );
    return null;
  }
}

connection.onInitialize((_params: InitializeParams): InitializeResult => {
  return {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      documentSymbolProvider: true,
    },
  };
});

connection.onInitialized(async () => {
  // Pre-load the compiler module
  await getWvletJS();
});

// Debounce map for document validation
const validationTimers = new Map<string, ReturnType<typeof setTimeout>>();

function scheduleValidation(document: TextDocument): void {
  const uri = document.uri;

  // Clear existing timer for this document
  const existing = validationTimers.get(uri);
  if (existing) {
    clearTimeout(existing);
  }

  // Schedule validation after 300ms debounce
  validationTimers.set(
    uri,
    setTimeout(async () => {
      validationTimers.delete(uri);
      await validateDocument(document);
    }, 300)
  );
}

async function validateDocument(document: TextDocument): Promise<void> {
  const module = await getWvletJS();
  if (!module) {
    return;
  }

  const diagnostics: Diagnostic[] = [];

  try {
    const resultJson = module.WvletJS.analyzeDiagnostics(document.getText());
    const lspDiagnostics: LspDiagnostic[] = JSON.parse(resultJson);

    for (const d of lspDiagnostics) {
      // Convert from 1-based (Wvlet) to 0-based (LSP)
      const range: Range = {
        start: Position.create(d.line - 1, d.column - 1),
        end: Position.create(d.endLine - 1, d.endColumn - 1),
      };

      const severity =
        d.severity === 'warning'
          ? DiagnosticSeverity.Warning
          : DiagnosticSeverity.Error;

      diagnostics.push({
        range,
        message: d.message,
        severity,
        source: 'wvlet',
        code: d.statusCode,
      });
    }
  } catch (e) {
    connection.console.error(
      `Validation error: ${e instanceof Error ? e.message : String(e)}`
    );
  }

  connection.sendDiagnostics({ uri: document.uri, diagnostics });
}

connection.onDocumentSymbol(async (params) => {
  const document = documents.get(params.textDocument.uri);
  if (!document) {
    return [];
  }

  const module = await getWvletJS();
  if (!module) {
    return [];
  }

  try {
    const resultJson = module.WvletJS.getDocumentSymbols(document.getText());
    const lspSymbols: LspSymbol[] = JSON.parse(resultJson);

    return lspSymbols.map((s): DocumentSymbol => {
      // Convert from 1-based (Wvlet) to 0-based (LSP)
      const range: Range = {
        start: Position.create(s.startLine - 1, s.startColumn - 1),
        end: Position.create(s.endLine - 1, s.endColumn - 1),
      };

      return {
        name: s.name,
        kind: s.kind as SymbolKind,
        range,
        selectionRange: range,
      };
    });
  } catch (e) {
    connection.console.error(
      `Document symbol error: ${e instanceof Error ? e.message : String(e)}`
    );
    return [];
  }
});

// Validate on open and change
documents.onDidOpen((event) => {
  scheduleValidation(event.document);
});

documents.onDidChangeContent((event) => {
  scheduleValidation(event.document);
});

// Clear diagnostics when a document is closed
documents.onDidClose((event) => {
  const timer = validationTimers.get(event.document.uri);
  if (timer) {
    clearTimeout(timer);
    validationTimers.delete(event.document.uri);
  }
  connection.sendDiagnostics({ uri: event.document.uri, diagnostics: [] });
});

// Listen on the connection
documents.listen(connection);
connection.listen();
