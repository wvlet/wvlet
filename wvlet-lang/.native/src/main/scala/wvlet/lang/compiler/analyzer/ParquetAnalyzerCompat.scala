package wvlet.lang.compiler.analyzer

import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType

trait ParquetAnalyzerCompat:
  protected def guessSchemaInternal(path: String): RelationType = EmptyRelationType
