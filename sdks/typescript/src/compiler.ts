import { WvletJS } from '../lib/main.js';
import { 
  CompileOptions, 
  CompileResponse, 
  CompilationError 
} from './types.js';

/**
 * Wvlet compiler for TypeScript/JavaScript
 */
export class WvletCompiler {
  private options: CompileOptions;

  /**
   * Create a new Wvlet compiler instance
   * @param options Default compilation options
   */
  constructor(options: CompileOptions = {}) {
    this.options = {
      target: 'duckdb',
      ...options
    };
  }

  /**
   * Compile a Wvlet query to SQL
   * @param query The Wvlet query string
   * @param options Override compilation options for this query
   * @returns The compiled SQL string
   * @throws {CompilationError} If compilation fails
   */
  compile(query: string, options?: CompileOptions): string {
    const compileOptions = { ...this.options, ...options };
    const optionsJson = JSON.stringify(compileOptions);
    
    const responseJson = WvletJS.compile(query, optionsJson);
    const response: CompileResponse = JSON.parse(responseJson);
    
    if (response.success && response.sql) {
      return response.sql;
    } else if (response.error) {
      throw new CompilationError(
        response.error.message,
        response.error.statusCode,
        response.error.location
      );
    } else {
      throw new Error('Invalid response from compiler');
    }
  }

  /**
   * Get the version of the Wvlet compiler
   * @returns The version string
   */
  static getVersion(): string {
    return WvletJS.getVersion();
  }
}