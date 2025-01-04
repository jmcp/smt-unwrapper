/*
 * Copyright (c) 2023, 2025 James C. McPherson. All rights reserved.
 *
 * This work is covered by the Hippocratic License v3, as documented
 * in [LICENSE.md](LICENSE.md)
 */

package com.jmcpdotcom.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Double;
import java.lang.Long;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Unwrapper<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger theLogger = (Logger) LoggerFactory.getLogger(Unwrapper.class);

    public static final String OVERVIEW_DOC = "Unwraps hierarchical elements in a record's value element. The returned object field names are the node names joined with `_`. The only configurable option is the log level to use for the class (which defaults to INFO).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.LOGLEVEL,
                    ConfigDef.Type.STRING,
                    "INFO",
                    null,
                    ConfigDef.Importance.LOW, OVERVIEW_DOC);

    public interface ConfigName {
        String LOGLEVEL = "LOGLEVEL";
    }

    private Level loglevel;

    private Map<String, Schema> schemaFields;
    private Schema outputSchema;


    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        loglevel = Level.toLevel(config.getString(ConfigName.LOGLEVEL), Level.INFO);
        theLogger.setLevel(loglevel);
    }

    @Override
    public R apply(R record) {

        theLogger.atDebug().log("Input record topic '{}' partition '{}' keySchema '{}' valueSchema '{}'",
                ((record.topic() != null) ? record.topic() : "null"),
                ((record.kafkaPartition() != null) ? record.kafkaPartition() : "null"),
                ((record.keySchema() != null) ? record.keySchema() : "null"),
                ((record.valueSchema() != null) ? record.valueSchema() : "null"));
        theLogger.atDebug().log("Input record key '{}' value '{}'",
                ((record.key() != null) ? record.key() : "null"),
                ((record.value() != null) ? record.value() : "null"));

        /* Check for a tombstone record and eject if necessary */
        if (record.value() == null) {
            return record;
        }

        schemaFields = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        SchemaAndValue outputRecord;
        Map<String, Object> unwrapped;

        if (record.valueSchema() == null) {
            /* Assume that we've got a JSON string */
            JsonNode nodes = null;

            try {
                nodes = mapper.readTree(record.value().toString());

            } catch (JsonProcessingException jpe) {
                theLogger.atWarn().log("Record has no schema (topic {}/partition {}/timestamp {}/key {})",
                        record.topic(), record.kafkaPartition(), record.timestamp(), record.key());
                theLogger.atWarn().log("Unable to successfully readTree({})", record.value().toString());
                theLogger.atWarn().log("Record is unchanged.");
                return record;
            }
            /* Updates class variable schemaFields */
            unwrapped = unwrap(nodes, "");
        } else {
            /* Assume that we've got an object with a schema.
             * This works for Avro, and _might_ work for Protobuf
             * but is untested.
             */
            theLogger.atTrace().log("\ncalling decompose(schema={}, struct={})\n",
                    record.valueSchema(), record.value());
            /* Also updates class variable schemaFields */
            unwrapped = decompose(record.valueSchema(), (Struct) record.value(), "");
        }
        theLogger.atDebug().log("after conversion, record value is\n\t{}", unwrapped);

        if (theLogger.isDebugEnabled()) {
            theLogger.atDebug().log("unwrapped record is");
            for (Map.Entry<String, Object> flat : unwrapped.entrySet()) {
                theLogger.atDebug().log("\t{} :: {}", flat, flat.getValue().toString());
            };
        };
        theLogger.atDebug().log("schemaFields:\n\t{}", schemaFields);

        outputSchema = generateConnectSchema();
        outputRecord = addDataToStruct(outputSchema, unwrapped);

        theLogger.atDebug().log("outputRecord.schema\n{}\n", outputRecord.schema());
        theLogger.atDebug().log("outputRecord.value:\n{}\n", outputRecord.value());

        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                outputRecord.schema(), outputRecord.value(),
                record.timestamp());
    }

    /*
     * Iterate over the nodes, simple elements get added directly ("fieldName": "field value"),
     * *record* objects are expanded ("objectName_subObjectName_subSubObjectName_fieldName": "field value")
     * *array* objects are also expanded
     *
     * "data": {
     *   "element_name": {
     *      "second_level": [
     *          {
     *              "inner_name_1": integerValue,
     *              "inner_name_2": "stringValue"
     *          },
     *          {
     *              "inner_name_1": integerValue,
     *              "inner_name_2": "stringValue"
     *          }
     *       ]
     *    }
     *  }
     *
     * is turned into
     *
     * "data_element_name_second_level_0_inner_name_1": integerValue
     * "data_element_name_second_level_0_inner_name_2": "stringValue"
     * "data_element_name_second_level_1_inner_name_1": integerValue
     * "data_element_name_second_level_1_inner_name_2": "stringValue"
     */

    private Map<String, Object> decompose(org.apache.kafka.connect.data.Schema theSchema, Struct theData, String prefix) {

        theLogger.atTrace().log("decompose(schema {}, struct {}, prefix {})", theSchema, theData, prefix);
        List<Field> schemaFieldsList = theSchema.fields();

        Map<String, Object> rmap = new HashMap<>();

        for (Field retrieval: schemaFieldsList) {
            org.apache.kafka.connect.data.Schema rSchema = retrieval.schema();
            String rname = retrieval.name();
            String newPrefix = (prefix.equals("")) ? rname : prefix.concat("_").concat(rname);
            String rtype = rSchema.type().name();
            Object rObj = theData.get(rname);

            switch (rtype) {
                case "STRUCT":
                    theLogger.atTrace().log("decompose needs to recurse a Struct");
                    Struct rStruct = theData.getStruct(rname);
                    Map<String, Object> toAdd = decompose(rStruct.schema(), rStruct, newPrefix);
                    theLogger.atTrace().log("Adding struct {} with contents {}", newPrefix, toAdd.values());
                    for (Map.Entry<String, Object> child: toAdd.entrySet()) {
                        if (child.getValue().getClass() == SchemaAndValue.class) {
                            /* Primitive, not struct, so we can add it */
                            theLogger.atTrace().log("Adding key {} value {}", child.getKey(), child.getValue());
                            rmap.put(child.getKey(), child.getValue());
                        }
                    }
                    break;

                /*
                 * For the primitive data types, we're calling the .get() method rather than the
                 * getX() so that we can avoid nulls.
                 */
                case "BOOLEAN":
                    theLogger.atTrace().log("Adding BOOLEAN {} as {}", rname, newPrefix);
                    boolean bValue = (rObj != null) ? (boolean) rObj : false;
                    rmap.put(newPrefix, new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, bValue));
                    schemaFields.put(newPrefix, Schema.OPTIONAL_BOOLEAN_SCHEMA);
                    break;

                case "BYTES":
                    theLogger.atTrace().log("Adding BYTES {} as {}", rname, newPrefix);
                    byte[] byteVal = (rObj != null) ? (byte[]) rObj : new byte[1];
                    rmap.put(newPrefix, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, byteVal));
                    schemaFields.put(newPrefix, Schema.OPTIONAL_BYTES_SCHEMA);
                    break;

                case "FLOAT32":
                case "FLOAT64":
                    theLogger.atTrace().log("Adding FLOAT {} as {}", rname, newPrefix);
                    Double thisFValue;
                    if (rObj instanceof Float) {
                        thisFValue = ((Float) rObj).doubleValue();
                    } else {
                        thisFValue = (Double) rObj;
                    }
                    if (thisFValue == null) {
                        thisFValue = 0.0D;
                    }
                    rmap.put(newPrefix, new SchemaAndValue(Schema.OPTIONAL_FLOAT64_SCHEMA, thisFValue));
                    schemaFields.put(newPrefix, Schema.OPTIONAL_FLOAT64_SCHEMA);
                    break;

                case "INT8":
                case "INT16":
                case "INT32":
                    theLogger.atTrace().log("Adding INTEGER {} as {}", rname, newPrefix);
                    Integer iVal = (rObj != null) ? (Integer) rObj : 0;
                    rmap.put(newPrefix, new SchemaAndValue(Schema.OPTIONAL_INT32_SCHEMA, iVal));
                    schemaFields.put(newPrefix, Schema.OPTIONAL_INT32_SCHEMA);
                    break;

                case "INT64":
                case Decimal.LOGICAL_NAME:
                    theLogger.atTrace().log("Adding BIGINT {} as {}", rname, newPrefix);
                    Long thisLValue;
                    if (theData.get(rname) instanceof java.util.Date) {
                        thisLValue = ((java.util.Date) rObj).getTime();
                    } else {
                        thisLValue = (Long) rObj;
                        if (thisLValue == null) {
                            thisLValue = 0L;
                        }
                    }

                    rmap.put(newPrefix, new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, thisLValue));
                    schemaFields.put(newPrefix, Schema.OPTIONAL_INT64_SCHEMA);
                    break;

                case "ARRAY":
                    theLogger.atTrace().log("Adding ARRAY {} as {}", rname, newPrefix);
                    List<Struct> rArray = theData.getArray(rname);

                    int idx = 0;
                    for (Struct elIdx: rArray) {
                        org.apache.kafka.connect.data.Schema cSchema = elIdx.schema();
                        theLogger.atTrace().log("decomposing {}, {}, {}", cSchema, elIdx, newPrefix.concat("_") + idx);
                        Map<String, Object> arryEl = decompose(cSchema, elIdx, newPrefix.concat("_") + idx);

                        /* Do we need more deconstruction ? */
                        for (Map.Entry<String, Object> decomposedEl: arryEl.entrySet()) {
                            if (decomposedEl.getValue() instanceof SchemaAndValue) {
                                if (((SchemaAndValue) decomposedEl.getValue()).schema().type().isPrimitive()) {
                                    theLogger.atTrace().log("ARRAY: adding element idx {} {} {}",
                                            idx, decomposedEl.getKey(), decomposedEl.getValue());
                                    rmap.put(newPrefix.concat("_") + idx, arryEl);
                                    schemaFields.put(newPrefix, cSchema);
                                }
                            }
                        }
                        ++idx;
                    }
                    theLogger.atTrace().log("Added %d ARRAY elements to {}", (idx + 1), newPrefix);
                    break;

                case "STRING":
                    theLogger.atTrace().log("Adding STRING {} as {}", rname, newPrefix);
                    String sVal =  (rObj != null) ? (String) rObj : "";
                    rmap.put(newPrefix, new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, sVal));
                    schemaFields.put(newPrefix, Schema.OPTIONAL_STRING_SCHEMA);
                    break;

                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                case Timestamp.LOGICAL_NAME:
                    theLogger.atTrace().log("Adding DATE/TIME {} as {}", rname, newPrefix);
                    rmap.put(newPrefix, new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, ((java.util.Date)rObj).getTime()));
                    break;

                default:
                    theLogger.atTrace().log("Don't know what this is: {}", rname);
                    theLogger.atTrace().log("decompose just putting it there (rname={})", rname);
                    rmap.put(newPrefix, rObj);
                    break;
            }
        }

        theLogger.atDebug().log("decompose returning (\n{}\n)", rmap);
        return rmap;
    }

    /*
     * Recursive function to unwrap our object hierarchy when we don't have a schema.
     */
    private Map<String, Object> unwrap(JsonNode jsonTree, String prefix) {

        Map<String, Object> rval = new HashMap<>();

        for (Iterator<Map.Entry<String, JsonNode>> it = jsonTree.fields(); it.hasNext(); ) {

            Map.Entry<String, JsonNode> entry = it.next();

            /* Simple (aka "primitive") nodes first */
            String nodeKey = entry.getKey();
            JsonNode nodeValue = entry.getValue();
            JsonNodeType nodeType = entry.getValue().getNodeType();

            theLogger.atTrace().log("node type {}\t prefix -> '{}'\t nodeKey -> {}",
                    nodeType.toString(), prefix, nodeKey);

            SchemaAndValue nodeSchemaAndValue = new SchemaAndValue(jsonNodeTypeToConnect(nodeType, nodeValue),
                    jsonValueToConnect(nodeValue));

            String unwrappedKey = nodeKey;

            if (!prefix.equals("")) {
                unwrappedKey = prefix.concat("_").concat(nodeKey);
            }

            switch (nodeType) {
                case ARRAY:
                    theLogger.atTrace().log("ARRAY: {}", nodeValue);
                    SchemaBuilder arrayBuilder = new SchemaBuilder(Schema.Type.STRUCT);
                    int idx = 0;

                    for (JsonNode child : nodeValue) {
                        JsonNodeType childNodeType = child.getNodeType();

                        if (child.getNodeType() == JsonNodeType.ARRAY || child.getNodeType() == JsonNodeType.OBJECT) {
                            Map<String, Object> element = unwrap(child, unwrappedKey);

                            for (Map.Entry<String, Object> unwrappedElement : element.entrySet()) {
                                SchemaAndValue sav = (SchemaAndValue) unwrappedElement.getValue();

                                if (!prefix.equals("")) {
                                    String newPrefix = prefix.concat("_")
                                            .concat(unwrappedElement.getKey().concat("_") + idx);
                                    rval.put(newPrefix, sav);
                                    arrayBuilder.field(newPrefix, sav.schema());
                                } else {
                                    rval.put(unwrappedElement.getKey().concat("_") + idx, sav);
                                    arrayBuilder.field(unwrappedElement.getKey().concat("_") + idx,
                                            sav.schema());
                                }
                            }
                        } else {
                            /* It's a primitive rather than a structure */
                            Schema primSchema = jsonNodeTypeToConnect(childNodeType, child);
                            SchemaAndValue primSAV = new SchemaAndValue(primSchema, jsonValueToConnect(child));
                            rval.put(unwrappedKey.concat("_") + idx, primSAV);
                            schemaFields.put(unwrappedKey.concat("_") + idx, primSchema);
                        }
                        ++idx;
                    }

                    schemaFields.put(unwrappedKey, SchemaBuilder.array(arrayBuilder.build()));
                    break;

                case OBJECT:
                    theLogger.atTrace().log("OBJECT: {}", nodeValue);
                    schemaFields.put(unwrappedKey, new SchemaBuilder(Schema.Type.STRUCT).build());
                    Map<String, Object> lowerElements = unwrap(nodeValue, unwrappedKey);

                    for (Map.Entry<String, Object> lower : lowerElements.entrySet()) {
                        theLogger.atTrace().log("OBJECT: lower: key '{}' value type '{}' and value '{}",
                                lower.getKey(), lower.getValue().getClass(), lower.getValue());
                        SchemaAndValue sav = (SchemaAndValue) lower.getValue();
                        rval.put(lower.getKey(), sav);
                        schemaFields.put(lower.getKey(), sav.schema());
                    }
                    break;

                default:
                    theLogger.atTrace().log("DEFAULT handling for '{}'", nodeValue);
                    if (!prefix.equals("")) {
                        String newPrefix = prefix.concat("_").concat(nodeKey);
                        rval.put(newPrefix, nodeSchemaAndValue);
                        schemaFields.put(newPrefix, jsonNodeTypeToConnect(nodeType, nodeValue));
                    } else {
                        rval.put(nodeKey, nodeSchemaAndValue);
                        schemaFields.put(nodeKey, jsonNodeTypeToConnect(nodeType, nodeValue));
                    }
                    break;
            }
        }
        return rval;
    }

    private Schema generateConnectSchema() {

        SchemaBuilder outputSchemaBuilder = SchemaBuilder.struct()
                .name("com.jmcpdotcom.transform.unwrapper")
                .optional();

        for (Map.Entry<String, Schema> theField : schemaFields.entrySet()) {
            Schema tSchema = theField.getValue();
            if (tSchema.type().isPrimitive()) {
                theLogger.atTrace().log("outputSchemaBuilder: Adding Field({}) (key {}, value {})",
                        theField, theField.getKey(), theField.getValue());
                outputSchemaBuilder.field(theField.getKey(), getDefaultForType(theField.getValue()));
            } else {
                theLogger.atTrace().log("outputSchemaBuilder: NOT adding Field({})", theField);
            }
        }
        return outputSchemaBuilder.build();
    }

    private Schema getDefaultForType(Schema theSchema) {
        SchemaBuilder rval;
        String theType = theSchema.type().name();

        switch (theType) {
            case "ARRAY":
                /* We need a useful (one we can find in a log entry) non-empty default value */
                rval = SchemaBuilder.array(theSchema)
                        .defaultValue(new ArrayList<String>().add(" _ "));
                break;
            case "BOOLEAN":
                rval = SchemaBuilder.bool().defaultValue(false);
                break;
            case "BYTES":
                rval = SchemaBuilder.bytes().defaultValue(new Byte[0]);
                break;
            case "INT8":
            case "INT16":
            case "INT32":
                rval = SchemaBuilder.int32().defaultValue(0);
                break;
            case "INT64":
                rval = SchemaBuilder.int64().defaultValue(0L);
                break;
            case "FLOAT32":
            case "FLOAT64":
                rval = SchemaBuilder.float64().defaultValue(0D);
                break;
            case "STRING":
                /* We need a useful (one we can find in a log entry) non-empty default value */
                rval = SchemaBuilder.string().defaultValue(" _ ");
                break;
            case Decimal.LOGICAL_NAME:
                rval = SchemaBuilder.int64().defaultValue(0L);
                break;
            case Date.LOGICAL_NAME:
            case Time.LOGICAL_NAME:
            case Timestamp.LOGICAL_NAME:
                rval = SchemaBuilder.int64().defaultValue(new java.util.Date());
                break;
            default:
                rval = SchemaBuilder.struct();
                break;
        }
        return rval;
    }


    public static SchemaAndValue addDataToStruct(Schema recordSchema, Map<String, Object> data) {

        final Struct outputStruct = new Struct(recordSchema);;

        theLogger.atDebug().log("\nrecordSchema is {}\ndata is {}", recordSchema, data);

        for (Map.Entry<String, Object> entry: data.entrySet()) {
            /*
             * It is _possible_ that some of our Objects are in fact null. Trying to get null's value or
             * class results in a NullPointerError, so skip this entry and let the field defaults stand in.
             */
            if (entry.getValue() == null) {
                theLogger.atWarn().log("Element {} is NULL, skipping", entry.getKey());
                continue;
            }

            theLogger.atTrace().log("Adding key '{}' value class '{}' value '{}' to outputStruct",
                    ((entry.getKey() != null) ? entry.getKey() : "null"),
                    entry.getValue().getClass().toString() , entry.getValue());

            if (entry.getValue() instanceof SchemaAndValue) {
                SchemaAndValue primitive = (SchemaAndValue) entry.getValue();
                theLogger.atTrace().log("--> Adding entry {}", primitive.toString());
                outputStruct.put(entry.getKey(), primitive.value());
            } else if (entry.getValue() instanceof HashMap) {
                theLogger.atTrace().log("addDataToStruct: HashMap {}", entry.getValue());
                SchemaBuilder eSchemaBuilder = SchemaBuilder.struct().name(entry.getKey());
                Map<String, Object> children = (Map<String, Object>) entry.getValue();

                Set<String> keys = ((HashMap<String, Object>) entry.getValue()).keySet();
                theLogger.atTrace().log("addDataToStruct: keys: {}", keys);
                for (String curKey : keys) {
                    if (children.get(curKey) instanceof Struct) {
                        Schema fSchema = ((Struct) children.get(curKey)).schema();
                        eSchemaBuilder.field(curKey, fSchema);
                    }
                }
                Struct eStruct = new Struct(eSchemaBuilder.build());
                for (Map.Entry<String, Object> child : children.entrySet()) {
                    eStruct.put(child.getKey(), child.getValue());
                }
                theLogger.atTrace().log("addDataToStruct key {} eStruct: {}", entry.getKey(), eStruct);
                outputStruct.put(entry.getKey(), eStruct);
            } else {
                if (entry.getKey() != null) {
                    theLogger.atDebug().log("--> addDataToStruct adding k {} v {}", entry.getKey(), entry.getValue());
                    outputStruct.put(entry.getKey(), entry.getValue());
                } else {
                    theLogger.atWarn().log("Element has null key in map (value is {})", entry.getValue());
                }
            }
        }

        theLogger.atTrace().log("addDataToStruct returning {}\n{}", recordSchema, outputStruct);
        return new SchemaAndValue(recordSchema, outputStruct);
    }

    public static Schema objectNodeTypeToConnect(Object node) {
        Schema rval;

        if (node instanceof Boolean) {
            rval = Schema.OPTIONAL_BOOLEAN_SCHEMA;
        } else if (node instanceof Byte) {
            rval = Schema.OPTIONAL_BYTES_SCHEMA;
        } else if (node instanceof Number ||
                node instanceof Integer ||
                node instanceof Short ||
                node instanceof Long) {
            rval = Schema.OPTIONAL_INT64_SCHEMA;
        } else if (node instanceof Double || node instanceof Float) {
            rval = Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else if (node instanceof String) {
            rval = Schema.OPTIONAL_STRING_SCHEMA;
        } else {
            rval = SchemaBuilder.struct().build();
        }
        return rval;
    }
    /*
     * It's possible that a NUMBER type can be decimal/floating point, so we need to do some handwavy
     * conversion stuff.
     */
    static boolean trulyInteger(JsonNode nodeValue) {

        String numberAsString = nodeValue.asText();

        /* This insufficiently localised, sorry */
        if (numberAsString.contains(".") || numberAsString.contains(",")) {
            return false;
        }
        return true;
    }

    public static Schema jsonNodeTypeToConnect(JsonNodeType node, JsonNode value) {
        Schema rval;

        switch (node) {
            case ARRAY:
                rval = SchemaBuilder.array(SchemaBuilder.struct().build()).build();
                break;
            case BOOLEAN:
                rval = Schema.OPTIONAL_BOOLEAN_SCHEMA;
                break;
            case BINARY:
                rval = Schema.OPTIONAL_BYTES_SCHEMA;
                break;
            case NUMBER:
                if (trulyInteger(value)) {
                    rval = Schema.OPTIONAL_INT64_SCHEMA;
                } else {
                    rval = Schema.OPTIONAL_FLOAT64_SCHEMA;
                }
                break;
            case OBJECT:
                rval = SchemaBuilder.struct().optional().build();
                break;
            case STRING:
                rval = Schema.OPTIONAL_STRING_SCHEMA;
                break;
            default:
                rval = Schema.OPTIONAL_STRING_SCHEMA;
                break;
        }
        return rval;
    }

    public static Object jsonValueToConnect(JsonNode jnode) {
        Object rval;
        switch (jnode.getNodeType()) {
                case BOOLEAN:
                    rval = jnode.asBoolean();
                    break;
                case BINARY:
                    try {
                        rval = jnode.binaryValue();
                    } catch (IOException iox) {
                        theLogger.atWarn().log("Unable to decode node as binary, returning Byte[0]");
                        rval = new Byte[0];
                    }
                    break;
                case NUMBER:
                    if (trulyInteger(jnode)) {
                        rval = jnode.asLong();
                    } else {
                        rval = jnode.asDouble();
                    }
                    break;
                default:
                    rval = jnode.asText();
                    break;
            }
        return rval;
    }
    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
