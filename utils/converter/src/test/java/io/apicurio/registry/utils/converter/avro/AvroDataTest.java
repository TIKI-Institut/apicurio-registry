package io.apicurio.registry.utils.converter.avro;

import io.apicurio.registry.serde.avro.NonRecordContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;

public class AvroDataTest {

    @Test
    public void testIntWithConnectDefault() {
        final String s = "{" + "  \"type\": \"record\"," + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\"," + "  \"fields\": [" + "    {"
                + "      \"name\": \"prop\"," + "      \"type\": {" + "        \"type\": \"int\","
                + "        \"connect.default\": 42," + "        \"connect.version\": 1" + "      }" + "    }"
                + "  ]" + "}";

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);

        AvroData avroData = new AvroData(0);
        Schema schema = avroData.toConnectSchema(avroSchema);

        Assertions.assertEquals(42, schema.field("prop").schema().defaultValue());
    }

    @Test
    public void testLongWithConnectDefault() {
        final String s = "{" + "  \"type\": \"record\"," + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\"," + "  \"fields\": [" + "    {"
                + "      \"name\": \"prop\"," + "      \"type\": {" + "        \"type\": \"long\","
                + "        \"connect.default\": 42," + "        \"connect.version\": 1" + "      }" + "    }"
                + "  ]" + "}";

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);

        AvroData avroData = new AvroData(0);
        Schema schema = avroData.toConnectSchema(avroSchema);

        Assertions.assertEquals(42L, schema.field("prop").schema().defaultValue());
    }

    @Test
    public void testAvroInt64WithInteger() {
        final String s = "{" + "  \"type\": \"record\"," + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\"," + "  \"fields\": [" + "    {"
                + "      \"name\": \"someprop\"," + "      \"type\": [\"long\",\"null\"]" + "    }" + "  ]"
                + "}";

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);
        GenericRecord outputRecord = new GenericRecordBuilder(avroSchema).set("someprop", (long) 42).build();
        AvroData avroData = new AvroData(0);
        Assertions.assertDoesNotThrow(() -> avroData.toConnectData(avroSchema, outputRecord));
    }

    @Test
    public void testDecimal() {
        final String s = "{" + "  \"type\": \"record\"," + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\"," + "  \"fields\": [" + "    {"
                + "      \"name\": \"somedecimal\"," + "      \"type\": [\n" + "          {\n"
                + "            \"type\": \"bytes\",\n" + "            \"scale\": 4,\n"
                + "            \"precision\": 4,\n" + "            \"connect.version\": 1,\n"
                + "            \"connect.parameters\": {\n" + "              \"scale\": \"4\",\n"
                + "              \"connect.decimal.precision\": \"4\"\n" + "            },\n"
                + "            \"connect.default\": \"AA==\",\n"
                + "            \"connect.name\": \"org.apache.kafka.connect.data.Decimal\",\n"
                + "            \"logicalType\": \"decimal\"\n" + "          },\n" + "          \"null\"\n"
                + "       ],\n" + "       \"default\": \"AA==\"" + "    }" + "  ],"
                + "\"connect.name\":\"io.apicurio.sample\"\n" + "}";

        org.apache.avro.Schema bSchema = new org.apache.avro.Schema.Parser().parse(s);
        AvroData avroData = new AvroData(0);
        org.apache.avro.Schema aSchema = avroData.fromConnectSchema(avroData.toConnectSchema(bSchema));
        Assertions.assertEquals(bSchema.toString(), aSchema.toString());
    }

    @Test
    public void testDebeziumTimestampType() {
        long debeziumTimestamp = io.debezium.time.Timestamp.toEpochMillis(new Date(), null);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.Timestamp.SCHEMA_NAME,
                "long",
                "timestamp-millis");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.Timestamp.schema(), debeziumTimestamp);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(debeziumTimestamp, (long) result.getValue());
    }

    @Test
    public void testDebeziumDateType() {
        int epochDays = io.debezium.time.Date.toEpochDay(new Date(), null);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.Date.SCHEMA_NAME,
                "int",
                "date");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.Date.schema(), epochDays);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(epochDays, (int) result.getValue());
    }

    @Test
    public void testDebeziumMicroTimeType() {
        long microTime = io.debezium.time.MicroTime.toMicroOfDay(Duration.ofMinutes(5), true);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.MicroTime.SCHEMA_NAME,
                "long",
                "time-micros");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.MicroTime.schema(), microTime);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(microTime, (long) result.getValue());
    }

    @Test
    public void testDebeziumMicroTimestampType() {
        long microTimestamp = io.debezium.time.MicroTimestamp.toEpochMicros(new Date(), null);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.MicroTimestamp.SCHEMA_NAME,
                "long",
                "timestamp-micros");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.MicroTimestamp.schema(), microTimestamp);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(microTimestamp, (long) result.getValue());
    }

    @Test
    public void testDebeziumTimeType() {
        int milliTime = io.debezium.time.Time.toMilliOfDay(Duration.ofMinutes(5), true);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.Time.SCHEMA_NAME,
                "int",
                "time-millis");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.Time.schema(), milliTime);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(milliTime, (int) result.getValue());
    }

    private org.apache.avro.Schema expectedAvroTypeSchema(String connectName, String primitiveType, String logicalType) {
        String typeSchemaString =
                "{" +
                "  \"type\" : \"" + primitiveType + "\"," +
                "  \"connect.version\" : 1," +
                "  \"connect.name\" : \"" + connectName + "\"," +
                "  \"logicalType\" : \"" + logicalType + "\"" +
                "}";
        return new org.apache.avro.Schema.Parser().parse(typeSchemaString);
    }
}
