package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordsTransformer {
  private final List<String> tableRecordPath;

  private final TableMetadata tableMetadata;

  public RecordsTransformer(String tableName, TableMetadata tableMetadata) {
    tableRecordPath = new ArrayList<>();
    tableRecordPath.add(tableName);
    tableRecordPath.add(DBConf.TABLE_RECORDS_STORE);

    this.tableMetadata = tableMetadata;
  }

  public List<String> getTableRecordPath() {
    return tableRecordPath;
  }

  public boolean doesPrimaryKeyExist(Transaction tx, Tuple primaryKeyTuple) {
    if (!FDBHelper.doesSubdirectoryExists(tx, tableRecordPath)) {
      return false;
    }

    DirectorySubspace dir = FDBHelper.openSubspace(tx, tableRecordPath);

    return FDBHelper.getKVPairIterableWithPrefixInDirectory(dir, tx, primaryKeyTuple, false).iterator().hasNext();
  }

  public static String getAttributeNameFromTuples(Tuple keyTuple, Tuple valueTuple) {
    return keyTuple.getString(keyTuple.size() - 1);
  }

  public static Object getAttributeValFromTuples(Tuple keyTuple, Tuple valueTuple) {
    return valueTuple.get(0);
  }

  public static Tuple getPrimaryKeyValTuple(Tuple keyTuple) {
    Tuple primTuple = new Tuple();
    for (int i = 0; i<keyTuple.size()-1; i++) {
      Object o = keyTuple.get(i);
      primTuple = primTuple.addObject(o);
    }
    return primTuple;
  }


  public static Tuple getRecordKeyTuple(List<Object> primaryKeyValues, String attributeName, Object value) {
    Tuple keyTuple = new Tuple();
    for (Object primVal : primaryKeyValues) {
      keyTuple = keyTuple.addObject(primVal);
    }

    keyTuple = keyTuple.add(attributeName);
    return keyTuple;
  }

  public static Tuple getRecordValueTuple(Object value) {
    return new Tuple().addObject(value);
  }


  public List<FDBKVPair> convertToFDBKVPairs(Record record) {
    List<FDBKVPair> res = new ArrayList<>();

    HashMap<String, Record.Value> attrMap = record.getMapAttrNameToValue();
    List<Object> primVal = new ArrayList<>();

    List<String> primaryKeys = tableMetadata.getPrimaryKeys();
    Collections.sort(primaryKeys);

    for (String pk : tableMetadata.getPrimaryKeys()) {
      primVal.add(attrMap.get(pk).getValue());
    }

    for (Map.Entry<String, Record.Value> entry : attrMap.entrySet()) {
      String attrName = entry.getKey();
      Object value = entry.getValue().getValue();
      Tuple keyTuple = getRecordKeyTuple(primVal, attrName, value);
      Tuple valTuple = getRecordValueTuple(value);
      res.add(new FDBKVPair(tableRecordPath, keyTuple, valTuple));
    }

    return res;
  }

  public Record convertBackToRecord(List<FDBKVPair> pairs) {
    Record record = new Record();

    for (FDBKVPair kv : pairs) {
      Tuple keyTuple = kv.getKey();
      Tuple valTuple = kv.getValue();

      record.setAttrNameAndValue(getAttributeNameFromTuples(keyTuple, valTuple), getAttributeValFromTuples(keyTuple, valTuple));
    }
    return record;
  }

}
