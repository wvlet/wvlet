package wvlet.lang.compiler.analyzer

import wvlet.lang.model.RelationType

object DuckDBAnalyzer extends DuckDBSchemaAnalyzerCompat:
  def guessSchema(path: String): RelationType = analyzeFileSchema(path)

trait DuckDBSchemaAnalyzerCompat:
  protected def analyzeFileSchema(path: String): RelationType =
    throw new UnsupportedOperationException(
      "File schema analysis is not yet supported on Scala Native"
    )

end DuckDBSchemaAnalyzerCompat
