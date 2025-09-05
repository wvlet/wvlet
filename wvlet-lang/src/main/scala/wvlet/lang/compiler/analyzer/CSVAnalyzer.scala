package wvlet.lang.compiler.analyzer

import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType

object CSVAnalyzer extends CSVAnalyzerCompat:
  def guessSchema(path: String): RelationType = guessSchemaInternal(path)
