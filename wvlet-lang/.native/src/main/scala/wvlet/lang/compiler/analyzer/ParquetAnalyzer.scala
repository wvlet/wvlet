package wvlet.lang.compiler.analyzer

import wvlet.lang.model.DataType.EmptyRelationType

object ParquetAnalyzer:
  def guessSchema(path: String): wvlet.lang.model.RelationType = EmptyRelationType
