/**
 * Compilation options for Wvlet queries
 */
export interface CompileOptions {
  /**
   * Target SQL dialect
   * @default 'duckdb'
   */
  target?: 'duckdb' | 'trino';
  
  /**
   * Profile name for configuration
   */
  profile?: string;
}

/**
 * Compilation response from the Wvlet compiler
 */
export interface CompileResponse {
  /**
   * Whether the compilation was successful
   */
  success: boolean;
  
  /**
   * The compiled SQL query (if successful)
   */
  sql?: string;
  
  /**
   * Error information (if failed)
   */
  error?: CompileError;
}

/**
 * Compilation error information
 */
export interface CompileError {
  /**
   * Error status code
   */
  statusCode: string;
  
  /**
   * Human-readable error message
   */
  message: string;
  
  /**
   * Source location where the error occurred
   */
  location?: ErrorLocation;
}

/**
 * Source location information for errors
 */
export interface ErrorLocation {
  /**
   * Relative file path
   */
  path: string;
  
  /**
   * File name only
   */
  fileName: string;
  
  /**
   * Line number (1-based)
   */
  line: number;
  
  /**
   * Column number (1-based)
   */
  column: number;
  
  /**
   * Content of the source line
   */
  lineContent?: string;
}

/**
 * Error thrown when compilation fails
 */
export class CompilationError extends Error {
  constructor(
    message: string,
    public statusCode: string,
    public location?: ErrorLocation
  ) {
    super(message);
    this.name = 'CompilationError';
  }
}