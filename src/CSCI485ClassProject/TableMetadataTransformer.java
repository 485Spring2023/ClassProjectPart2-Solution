package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMetadataTransformer {

  private List<String> tableAttributeStorePath;

  public FDBKVPair getAttributeKVPair(String attributeName, AttributeType attributeType) {
    Tuple keyTuple = getTableAttributeKeyTuple(attributeName);
    Tuple valueTuple = new Tuple().add(attributeType.ordinal()).add(false);

    return new FDBKVPair(tableAttributeStorePath, keyTuple, valueTuple);
  }

  public static Tuple getTableAttributeKeyTuple(String attributeName) {
    return new Tuple().add(attributeName);
  }

  public TableMetadataTransformer(String tableName) {
    tableAttributeStorePath = new ArrayList<>();
    tableAttributeStorePath.add(tableName);
    tableAttributeStorePath.add(DBConf.TABLE_SCHEMA_STORE);
  }

  public List<String> getTableAttributeStorePath() {
    return tableAttributeStorePath;
  }

  public TableMetadata convertBackToTableMetadata(List<FDBKVPair> pairs) {
    TableMetadata tableMetadata = new TableMetadata();
    List<String> primaryKeys = new ArrayList<>();
    for (FDBKVPair kv : pairs) {
      Tuple key = kv.getKey();
      Tuple value = kv.getValue();

      String attributeName = key.getString(0);
      tableMetadata.addAttribute(attributeName, AttributeType.values() [Math.toIntExact((Long) value.get(0))]);
      boolean isPrimaryKey = value.getBoolean(1);
      if (isPrimaryKey) {
        primaryKeys.add(attributeName);
      }
    }

    tableMetadata.setPrimaryKeys(primaryKeys);
    return tableMetadata;
  }

  public List<FDBKVPair> convertToFDBKVPairs(TableMetadata table) {
    List<FDBKVPair> res = new ArrayList<>();

    HashMap<String, AttributeType> attributeMap = table.getAttributes();

    List<String> primaryKeys = table.getPrimaryKeys();

    // prepare kv pairs for Attribute
    for (Map.Entry<String, AttributeType> kv : attributeMap.entrySet()) {
      Tuple keyTuple = getTableAttributeKeyTuple(kv.getKey());
      boolean isPrimaryKey = primaryKeys.contains(kv.getKey());
      Tuple valueTuple = new Tuple().add(kv.getValue().ordinal()).add(isPrimaryKey);

      res.add(new FDBKVPair(tableAttributeStorePath, keyTuple, valueTuple));
    }
    return res;
  }
}