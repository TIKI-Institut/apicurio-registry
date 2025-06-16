package io.apicurio.registry.utils.converter.avro.sap;

import io.apicurio.registry.utils.converter.avro.AvroData;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.connect.data.Schema;

/**
 * Handles decimal conversions from a SAP BCD Type where the precision property is missing
 * and we have to calculate the precision from the length attribute.
 *
 * <pre>
 * {@code
 *  {
 * 	"name": "PAMNG",
 * 	"type": {
 * 		"type": "bytes",
 * 		"scale": 3,
 * 		"connect.version": 1,
 * 		"connect.parameters": {
 * 			"scale": "3",
 * 			"jcoType": "JCoBCDType",
 * 			"length": "7",
 * 			"decimals": "3"
 * 		        },
 * 		"connect.name": "org.apache.kafka.connect.data.Decimal",
 * 		"logicalType": "decimal"    * 	}
 * 	}
 * }
 * </pre>
 *
 * @see <a href="https://help.sap.com/doc/abapdocu_750_index_htm/7.50/en-US/abenbcd_glosry.htm">SAP BCD Glossary</a>
 * @see <a href="https://help.sap.com/doc/abapdocu_752_index_htm/7.52/en-US/abennumber_types.htm#@@ITOC@@ABENNUMBER_TYPES_2">SAP Packed Numbers</a>
 */
public class JCoBCDTypeToAvroDecimal extends AvroData.ConnectDecimalToAvro {
    static final String CONNECT_PARAM_JCO_TYPE = "jcoType";
    static final String CONNECT_PARAM_JCO_BCD_TYPE_VALUE = "JCoBCDType";
    static final String CONNECT_PARAM_JCO_BCD_LENGTH = "length";

    @Override
    public LogicalType avroLogicalType(Schema schema) {
        LogicalTypes.Decimal baseDecimalType = (LogicalTypes.Decimal) super.avroLogicalType(schema);

        if (!(schema.parameters().containsKey(CONNECT_PARAM_JCO_BCD_LENGTH)
                && schema.parameters().containsKey(CONNECT_PARAM_JCO_TYPE)
                && schema.parameters().get(CONNECT_PARAM_JCO_TYPE).equals(CONNECT_PARAM_JCO_BCD_TYPE_VALUE))) {
            return baseDecimalType;
        }

        int length = Integer.parseInt(schema.parameters().get(CONNECT_PARAM_JCO_BCD_LENGTH));
        int jCoBCDPrecision = length * 2 - 1 + baseDecimalType.getScale();

        return LogicalTypes.decimal(jCoBCDPrecision, baseDecimalType.getScale());
    }

    @Override
    public Object convert(Schema schema, Object value) {
        return super.convert(schema, value);
    }
}
