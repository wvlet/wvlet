package wvlet.lang.compiler.analyzer

import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.RelationType

trait CSVAnalyzerCompat:
  protected def guessSchemaInternal(path: String): RelationType =
    throw new UnsupportedOperationException("CSV reading is not yet supported on Scala Native")

end CSVAnalyzerCompat
