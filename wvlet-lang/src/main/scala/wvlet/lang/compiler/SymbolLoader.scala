package wvlet.lang.compiler

import wvlet.lang.model.Type.LazyType

/**
  * A lazy type that can be loaded later with [[load]] method.
  */
abstract class SymbolLoader extends LazyType:
  def load(root: SymbolInfo)(using Context): Unit
