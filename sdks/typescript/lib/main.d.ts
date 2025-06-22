/**
 * Type definitions for Scala.js generated WvletJS module
 */

export interface WvletJS {
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
}

export const WvletJS: WvletJS;