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

import wvlet.airframe.codec.{MessageCodec, MessageContext}
import wvlet.airframe.msgpack.spi.{MessagePack, Packer, Unpacker}
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.log.LogSupport

/**
  * Custom codec for DataType that serializes to/from string representation
  */
object DataTypeCodec extends MessageCodec[DataType] with LogSupport:

  override def pack(p: Packer, v: DataType): Unit =
    // Serialize DataType as its string representation
    p.packString(v.toString)

  override def unpack(u: Unpacker, v: MessageContext): Unit =
    try
      val typeStr = u.unpackString
      val dataType = 
        try
          DataType.parse(typeStr)
        catch
          case e: Exception =>
            // Fallback to GenericType for types that can't be parsed
            warn(s"Failed to parse DataType '$typeStr', using GenericType as fallback: ${e.getMessage}")
            DataType.GenericType(Name.typeName(typeStr))
      v.setObject(dataType)
    catch
      case e: Exception =>
        v.setError(new IllegalArgumentException(s"Failed to parse DataType from string: ${e.getMessage}", e))

end DataTypeCodec