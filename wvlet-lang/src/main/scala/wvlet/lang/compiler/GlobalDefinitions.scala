package wvlet.lang.compiler

class GlobalDefinitions(using rootContext: Context):

  val RootPackageName: Name = Name.termName("<root>")
  val RootPackage: Symbol =
    val sym = Symbol.newPackageSymbol(owner = Symbol.NoSymbol, name = RootPackageName)
    sym
