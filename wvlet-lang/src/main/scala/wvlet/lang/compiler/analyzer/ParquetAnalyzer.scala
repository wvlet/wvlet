package wvlet.lang.compiler.analyzer

import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType

object ParquetAnalyzer extends ParquetAnalyzerCompat:
  def guessSchema(path: String): RelationType = guessSchemaInternal(path)
