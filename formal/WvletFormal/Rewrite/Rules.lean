/-
Licensed under the Apache License, Version 2.0.

The rules of `RewriteExpr.rewriteRules` and the phase that applies them. Each rule is a node-level
function, as the Scala partial function is; `bottomUp` is `transformUpExpression`.
-/
import WvletFormal.Rewrite.Expr

namespace Wvlet.Rewrite

mutual
/-- Expressions that can never be NULL on any engine: non-NULL literals, casts of them, `concat`
of such arguments, and `coalesce` with such an argument. -/
def nonNull : Expr → Bool
  | .lit v => !v.isNull
  | .cast e => nonNull e
  | .call f args =>
    if f = "concat" then allNonNull args
    else if f = "coalesce" then anyNonNull args
    else false
  | _ => false

def allNonNull : Args → Bool
  | .nil => true
  | .cons e rest => nonNull e && allNonNull rest

def anyNonNull : Args → Bool
  | .nil => false
  | .cons e rest => nonNull e || anyNonNull rest
end

def args2 (a b : Expr) : Args := .cons a (.cons b .nil)

def concat2 (a b : Expr) : Expr := .call "concat" (args2 a b)

/-- Cast a non-string expression to varchar. -/
def asVarchar (e : Expr) : Expr := if typeOf e = .string then e else .cast e

/-- The text of an expression as a non-NULL string: NULL becomes `''`. -/
def toText (e : Expr) : Expr :=
  if nonNull e then asVarchar e else .call "coalesce" (args2 (asVarchar e) (.lit (.str "")))

/-- `RewriteStringConcat`: `l + r` with a string side becomes `concat` of both texts. -/
def concatRule : Expr → Expr
  | .add l r => if isStrAdd (typeOf l) (typeOf r) then concat2 (toText l) (toText r) else .add l r
  | e => e

/-- An interpolation part as a non-NULL string. -/
def quotePart : Expr → Expr
  | .part s => .lit (.str s)
  | e => toText e

def foldRest (acc : Expr) : Args → Expr
  | .nil => acc
  | .cons p rest => foldRest (concat2 acc (quotePart p)) rest

/-- `parts.map(quote)`, folded from the left with `concat`. -/
def foldParts : Args → Expr
  | .nil => .lit (.str "")
  | .cons p rest => foldRest (quotePart p) rest

/-- `RewriteStringInterpolation`: `s"..."` becomes a `concat` of its quoted parts. -/
def interpRule : Expr → Expr
  | .interp pfx parts => if pfx = "s" then foldParts parts else .interp pfx parts
  | e => e

/-- `RewriteIfExpr`: `if(c, t, e)` becomes `IfExpr(c, t, e)`, and `if(c, t)` gets a NULL else
branch. Other arities are left alone. -/
def ifRule : Expr → Expr
  | .call f (.cons c (.cons t .nil)) =>
    if f = "if" then .ifExpr c t (.lit .null) else .call f (args2 c t)
  | .call f (.cons c (.cons t (.cons e .nil))) =>
    if f = "if" then .ifExpr c t e else .call f (.cons c (.cons t (.cons e .nil)))
  | e => e

mutual
/-- Apply a node-level rule bottom-up (`transformUpExpression`). -/
def bottomUp (r : Expr → Expr) : Expr → Expr
  | .lit v => r (.lit v)
  | .col n t => r (.col n t)
  | .add l rr => r (.add (bottomUp r l) (bottomUp r rr))
  | .interp p ps => r (.interp p (bottomUpArgs r ps))
  | .part s => r (.part s)
  | .call f as => r (.call f (bottomUpArgs r as))
  | .ifExpr c t e => r (.ifExpr (bottomUp r c) (bottomUp r t) (bottomUp r e))
  | .cast e => r (.cast (bottomUp r e))

def bottomUpArgs (r : Expr → Expr) : Args → Args
  | .nil => .nil
  | .cons e rest => .cons (bottomUp r e) (bottomUpArgs r rest)
end

/-- The `rewrite-expr` phase on one expression: each rule of `rewriteRules`, in order, as a
bottom-up pass. -/
def phase (e : Expr) : Expr :=
  bottomUp ifRule (bottomUp interpRule (bottomUp concatRule e))

/-! The rules as they shipped before this model (kept to state the findings). -/

namespace Legacy

/-- `+` became `concat(l, r)` only when the left side was a string, with no cast or NULL
handling. -/
def concatRule : Expr → Expr
  | .add l r => if typeOf l = .string then concat2 l r else .add l r
  | e => e

def quote : Expr → Expr
  | .part s => .lit (.str s)
  | e => asVarchar e

def foldRest (acc : Expr) : Args → Expr
  | .nil => acc
  | .cons p rest => foldRest (concat2 (quote acc) (quote p)) rest

/-- `Expression.concat(parts)(merger)`: a single part was returned as is. -/
def interpRule : Expr → Expr
  | .interp pfx (.cons p rest) => if pfx = "s" then foldRest p rest else .interp pfx (.cons p rest)
  | e => e

def phase (e : Expr) : Expr :=
  bottomUp ifRule (bottomUp interpRule (bottomUp concatRule e))

end Legacy

end Wvlet.Rewrite
