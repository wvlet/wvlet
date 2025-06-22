/**
 * Wvlet TypeScript SDK
 * 
 * A TypeScript/JavaScript SDK for the Wvlet query language compiler.
 * Compile Wvlet queries to SQL for various database engines.
 * 
 * @packageDocumentation
 */

export { WvletCompiler } from './compiler.js';
export { 
  CompileOptions,
  CompileResponse,
  CompileError,
  ErrorLocation,
  CompilationError
} from './types.js';

// Re-export the default compiler function for convenience
import { WvletCompiler } from './compiler.js';

/**
 * Compile a Wvlet query to SQL using default options
 * @param query The Wvlet query string
 * @returns The compiled SQL string
 * @throws {CompilationError} If compilation fails
 */
export async function compile(query: string): Promise<string> {
  const compiler = new WvletCompiler();
  return compiler.compile(query);
}