package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.model.Type.LazyType

/**
  * A lazy type that can be loaded later with [[load]] method.
  */
abstract class SymbolLoader extends LazyType:
  def load(root: SymbolInfo)(using Context): Unit
