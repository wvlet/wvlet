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
package wvlet.lang.embedded;

/** The Java usage shown in website/docs/bindings/jvm.md, compiled and run by EmbeddedEngineTest */
public class EmbeddedEngineJavaUsage {
  public static long countRegions(String dataDir) {
    try (EmbeddedEngine engine = EmbeddedEngine.open(dataDir)) {
      engine.registerTable("region", "region.parquet");
      EmbeddedResult result = engine.query("from region agg _.count as n");
      for (int i = 0; i < result.columnCount(); i++) {
        System.out.println(result.columnName(i) + ": " + result.columnType(i));
      }
      return (Long) result.get(0, 0);
    }
  }
}
