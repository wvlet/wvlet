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
package wvlet.lang.catalog

import wvlet.airframe.codec.{MessageCodec, MessageCodecFactory}
import wvlet.lang.model.DataType
import wvlet.log.LogSupport

/**
  * Handles serialization and deserialization of catalog metadata to/from JSON format
  */
object CatalogSerializer extends LogSupport:

  // Codec for table definitions
  private val tableDefCodec = MessageCodec.of[List[Catalog.TableDef]]

  // Codec for SQL functions
  private val sqlFunctionCodec = MessageCodec.of[List[SQLFunction]]

  // Codec for schema metadata
  private val schemaCodec = MessageCodec.of[List[Catalog.TableSchema]]

  /**
    * Catalog metadata structure that includes all catalog information
    */
  case class CatalogMetadata(
      catalogName: String,
      dbType: String,
      schemas: List[Catalog.TableSchema],
      // Map of schema name to table definitions
      tables: Map[String, List[Catalog.TableDef]],
      functions: List[SQLFunction],
      version: String = "1.0"
  )

  private val catalogMetadataCodec = MessageCodec.of[CatalogMetadata]

  def serializeTables(tables: List[Catalog.TableDef]): String = tableDefCodec.toJson(tables)

  def deserializeTables(json: String): List[Catalog.TableDef] = tableDefCodec.fromJson(json)

  def serializeFunctions(functions: List[SQLFunction]): String = sqlFunctionCodec.toJson(functions)

  def deserializeFunctions(json: String): List[SQLFunction] = sqlFunctionCodec.fromJson(json)

  def serializeSchemas(schemas: List[Catalog.TableSchema]): String = schemaCodec.toJson(schemas)

  def deserializeSchemas(json: String): List[Catalog.TableSchema] = schemaCodec.fromJson(json)

  def serializeCatalog(metadata: CatalogMetadata): String = catalogMetadataCodec.toJson(metadata)

  def deserializeCatalog(json: String): CatalogMetadata = catalogMetadataCodec.fromJson(json)

end CatalogSerializer
