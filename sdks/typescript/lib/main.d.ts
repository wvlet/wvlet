/**
 * Type definitions for Scala.js generated WvletJS module
 */

export interface WvletJSType {
  /**
   * Compile a Wvlet query and return the result as JSON
   * @param query The Wvlet query string
   * @param options JSON string with compilation options
   * @returns JSON string with compilation result
   */
  compile(query: string, options?: string): string;
  
  /**
   * Get the version of the Wvlet compiler
   * @returns The version string
   */
  getVersion(): string;

  /**
   * Analyze a Wvlet source and return diagnostics as JSON
   * @param content The Wvlet source code
   * @returns JSON array of diagnostics
   */
  analyzeDiagnostics(content: string): string;

  /**
   * Extract document symbols from a Wvlet source as JSON
   * @param content The Wvlet source code
   * @returns JSON array of symbols
   */
  getDocumentSymbols(content: string): string;
}

export const WvletJS: WvletJSType;
export const log: any;