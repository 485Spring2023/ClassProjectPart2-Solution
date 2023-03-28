package CSCI485ClassProject.models;


import CSCI485ClassProject.StatusCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static CSCI485ClassProject.StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
import static CSCI485ClassProject.StatusCode.SUCCESS;

/**
 * Record represents the data stored in the database
 * - for a record in a table, it maps the attribute names to the corresponding values
 * - for an index record, it maps your defined attributes to the corresponding values
 */
public class Record {

  /**
   * Value represents the valid value stored in the database. It encapsulates the type checking and value getter/setter.
   */
  public static class Value {

    /**
     * The underneath value
     */
    private Object value = null;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Value value1 = (Value) o;
      Object val = value1.value;
      return Objects.equals(value, value1.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    public Value() {
      value = null;
    }

    public Object getValue() {
      return value;
    }

    /**
     * Check if given value is of the supported type
     */
    public static boolean isTypeSupported(Object value) {
      return value == null                  // NULL
          || value instanceof Integer  // INT
          || value instanceof Long     // INT (long also regards as INT)
          || value instanceof String // VARCHAR
          || value instanceof Double;
    }

    /**
     * Set the underneath value. The given value must have the valid type.
     * @param value the value to set
     * @return StatusCode
     */
    public StatusCode setValue(Object value) {
      if (!isTypeSupported(value))
        return ATTRIBUTE_TYPE_NOT_SUPPORTED;

      this.value = value;
      return SUCCESS;
    }

    /**
     * Get the type of the underneath value
     * @return AttributeType
     */
    public AttributeType getType() {
      if (value instanceof Integer || value instanceof Long) {
        return AttributeType.INT;
      } else if (value instanceof String) {
        return AttributeType.VARCHAR;
      } else if (value instanceof Double) {
        return AttributeType.DOUBLE;
      }
      return AttributeType.NULL;
    }
  }

  /**
   * Map from the attribute name to the corresponding value
   */
  private HashMap<String, Value> mapAttrNameToValue;

  public Record() {
    mapAttrNameToValue = new HashMap<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Record record = (Record) o;
    return Objects.equals(mapAttrNameToValue, record.mapAttrNameToValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mapAttrNameToValue);
  }

  public HashMap<String, Value> getMapAttrNameToValue() {
    return mapAttrNameToValue;
  }

  /**
   * Set a pair of attrName and value. Value's type should be supported,
   * @param attrName the attribute name
   * @param value the value
   * @return StatusCode
   */
  public StatusCode setAttrNameAndValue(String attrName, Object value) {
    Value val = new Value();
    StatusCode status = val.setValue(value);
    if (status != SUCCESS) {
      return status;
    }

    mapAttrNameToValue.put(attrName, val);
    return SUCCESS;
  }

  /**
   * Set the Name-to-Value Map. Each value must be the supported type.
   * @param mapAttrNameToValue the map to be set
   * @return StatusCode
   */
  public StatusCode setMapAttrNameToValue(HashMap<String, Object> mapAttrNameToValue) {

    List<Value> values = new ArrayList<>();

    for (Map.Entry<String, Object> kv : mapAttrNameToValue.entrySet()) {
      Value val = new Value();
      StatusCode status = val.setValue(kv.getValue());
      if (status != SUCCESS) {
        return status;
      }

      values.add(val);
    }

    int i = 0;
    for (String key : mapAttrNameToValue.keySet()) {
      this.mapAttrNameToValue.put(key, values.get(i));
      i++;
    }

    return SUCCESS;
  }

  public Object getValueForGivenAttrName(String attrName) {
    if (mapAttrNameToValue.containsKey(attrName)) {
      return mapAttrNameToValue.get(attrName).getValue();
    }
    return null;
  }

  public AttributeType getTypeForGivenAttrName(String attrName) {
    if (mapAttrNameToValue.containsKey(attrName)) {
      return mapAttrNameToValue.get(attrName).getType();
    }
    return null;
  }
}
