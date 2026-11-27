/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine.plan

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * The ORDER BY keys of [[Planner]]: sort items resolved against the select items, and bound to
  * sort keys over the input
  */
trait PlannerSortKeys:
  this: Planner =>

  private[engine] def sortKeys(
      binder: Binder,
      items: List[SortItem],
      schema: Schema
  ): Seq[SortKey] = items.map { s =>
    val e =
      s.sortKey match
        case l: LongLiteral =>
          val i = l.value.toInt - 1
          ColRef(binder.scope.offset + i, schema(i).ptype, schema(i).name)
        case other =>
          binder.bind(other)
    sortKey(s, e)
  }

  private[engine] def sortKey(s: SortItem, e: VExpr): SortKey =
    // DuckDB's default: NULLS LAST for both directions
    val nullsFirst = s.nullOrdering.contains(NullOrdering.NullIsFirst)
    SortKey(e, !s.ordering.contains(SortOrdering.Descending), nullsFirst)

  /** SQL text of an expression, used to match ORDER BY keys with select items */
  private[plan] def exprText(e: Expression): String =
    e match
      case p: ParenthesizedExpression =>
        exprText(p.child)
      case other =>
        printSql(other)

  private[plan] def itemExpr(a: Attribute): Option[Expression] =
    a match
      case s: SingleColumn =>
        Some(s.expr)
      case al: Alias =>
        Some(al.expr)
      case _ =>
        None

  /**
    * Rewrite ORDER BY keys that repeat a select item expression (e.g., `dt.d_year`,
    * `substring(s_city, 1, 30)`, `count(*)`) or name a column of a select item (`i_brand_id` for
    * `item.i_brand_id as brand_id`) into positional references to the select output. Keys naming an
    * output column are kept as is
    */
  private[engine] def resolveSortItems(s: Sort): List[SortItem] =
    def selectItems(r: Relation): Option[List[Attribute]] =
      r match
        case p: Project =>
          Some(p.selectItems)
        case d: Distinct =>
          selectItems(d.child)
        case _ =>
          None
    selectItems(s.child) match
      case Some(items) if !items.exists {
            case _: AllColumns =>
              true
            case i =>
              itemExpr(i).exists(isStar)
          } =>
        val outNames = items.map(i => nameOf(i).toLowerCase).toSet
        val texts    = items.map(i => itemExpr(i).map(exprText))
        s.orderBy
          .map { k =>
            k.sortKey match
              case _: LongLiteral =>
                k
              case i: Identifier if outNames.contains(i.leafName.toLowerCase) =>
                k
              case key =>
                val text   = exprText(key)
                val byExpr = texts.indexWhere(_.contains(text))
                val idx    =
                  if byExpr >= 0 then
                    byExpr
                  else
                    key match
                      case i: Identifier =>
                        // An unqualified column of a qualified select item
                        val leaf = i.leafName.toLowerCase
                        val hits = items
                          .indices
                          .filter { j =>
                            itemExpr(items(j)) match
                              case Some(d: DotRef) =>
                                d.name.leafName.toLowerCase == leaf
                              case _ =>
                                false
                          }
                        if hits.size == 1 then
                          hits.head
                        else
                          -1
                      case _ =>
                        -1
                if idx >= 0 then
                  k.copy(sortKey = LongLiteral(idx + 1, (idx + 1).toString, key.span))
                else
                  k
          }
      case _ =>
        s.orderBy
    end match

  end resolveSortItems

end PlannerSortKeys
