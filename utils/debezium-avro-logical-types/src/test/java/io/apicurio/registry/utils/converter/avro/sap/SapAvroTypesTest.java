package io.apicurio.registry.utils.converter.avro.sap;

import io.apicurio.registry.serde.avro.NonRecordContainer;
import io.apicurio.registry.utils.converter.avro.AvroData;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class SapAvroTypesTest {

    @Test
    public void testJCoBCDTypeLengthToPrecisionConversion() {
        BigDecimal decimal = BigDecimal.valueOf(322.22);
        Schema connectSchema = Decimal.builder(2)
                .parameter(JCoBCDTypeToAvroDecimal.CONNECT_PARAM_JCO_BCD_LENGTH, "2")
                .parameter(JCoBCDTypeToAvroDecimal.CONNECT_PARAM_JCO_TYPE, JCoBCDTypeToAvroDecimal.CONNECT_PARAM_JCO_BCD_TYPE_VALUE)
                .build();

        Conversions.DecimalConversion avroDecimalConversion = new Conversions.DecimalConversion();

        SchemaAndValue connectValue = new SchemaAndValue(connectSchema, decimal);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(LogicalTypes.decimal(5, 2), result.getSchema().getLogicalType());
        Assertions.assertEquals(decimal, avroDecimalConversion.fromBytes((ByteBuffer) result.getValue(), result.getSchema(), result.getSchema().getLogicalType()));
    }

    @Test
    public void testJCoBCDTypeLengthToPrecisionConversion_MissingType() {
        BigDecimal decimal = BigDecimal.valueOf(322.22);
        Schema connectSchema = Decimal.builder(2)
                .parameter(JCoBCDTypeToAvroDecimal.CONNECT_PARAM_JCO_BCD_LENGTH, "2")
                .build();

        Conversions.DecimalConversion avroDecimalConversion = new Conversions.DecimalConversion();

        SchemaAndValue connectValue = new SchemaAndValue(connectSchema, decimal);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(LogicalTypes.decimal(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT, 2), result.getSchema().getLogicalType());
        Assertions.assertEquals(decimal, avroDecimalConversion.fromBytes((ByteBuffer) result.getValue(), result.getSchema(), result.getSchema().getLogicalType()));
    }

    @Test
    public void testDefaultDecimalConversion() {
        BigDecimal decimal = BigDecimal.valueOf(322.22);
        Schema connectSchema = Decimal.builder(2)
                .parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "7")
                .build();

        Conversions.DecimalConversion avroDecimalConversion = new Conversions.DecimalConversion();

        SchemaAndValue connectValue = new SchemaAndValue(connectSchema, decimal);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(LogicalTypes.decimal(7, 2), result.getSchema().getLogicalType());
        Assertions.assertEquals(decimal, avroDecimalConversion.fromBytes((ByteBuffer) result.getValue(), result.getSchema(), result.getSchema().getLogicalType()));
    }
}
