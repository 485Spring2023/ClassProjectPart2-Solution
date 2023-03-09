package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static CSCI485ClassProject.RecordsTransformer.getPrimaryKeyValTuple;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE,
  }

  private boolean isPredicateEnabled = false;
  private String attributeName;
  private Object attributeValue;
  private ComparisonOperator operator;

  private String tableName;
  private TableMetadata tableMetadata;
  private RecordsTransformer recordsTransformer;

  private boolean isInitializedToLast = false;

  private Mode mode;

  private AsyncIterator<KeyValue> iterator = null;

  private Record currentRecord;
  // used by the col storage
  private String currentAttributeName;

  private Transaction tx;

  private DirectorySubspace directorySubspace;

  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
  }

  public Transaction getTx() {
    return tx;
  }

  public void setTx(Transaction tx) {
    this.tx = tx;
  }

  public void abort() {
    if (iterator != null) {
      iterator.cancel();
    }

    if (tx != null) {
      FDBHelper.abortTransaction(tx);
    }
  }

  public void commit() {
    if (iterator != null) {
      iterator.cancel();
    }
    if (tx != null) {
      FDBHelper.commitTransaction(tx);
    }
  }

  public final Mode getMode() {
    return mode;
  }

  public final void setMode(Mode mode) {
    this.mode = mode;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public void setTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public StatusCode enablePredicate(String attrName, Object value, ComparisonOperator operator) {
    this.attributeName = attrName;
    this.attributeValue = value;
    this.operator = operator;
    this.isPredicateEnabled = true;
    return null;
  }

  private Record seek(Transaction tx, boolean isInitializing) {

    if (isInitializing) {
      // initialize the subspace and the iterator
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      directorySubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast);
      if (fdbIterable != null)
        iterator = fdbIterable.iterator();
    }

    // no such directory, or no records under the directory
    if (directorySubspace == null || !hasNext()) {
      return null;
    }

    // reset the currentRecord
    currentRecord = null;
    List<String> recordStorePath = recordsTransformer.getTableRecordPath();
    List<FDBKVPair> fdbkvPairs = new ArrayList<>();
    if (DBConf.IS_ROW_STORAGE) {
      boolean isSavePK = false;
      Tuple pkValTuple = new Tuple();
      while (iterator.hasNext()) {
        KeyValue kv = iterator.next();
        Tuple keyTuple = directorySubspace.unpack(kv.getKey());
        Tuple valTuple = Tuple.fromBytes(kv.getValue());
        Tuple tempPkValTuple = getPrimaryKeyValTuple(keyTuple);
        if (!isSavePK) {
          pkValTuple = tempPkValTuple;
          isSavePK = true;
        } else if (!pkValTuple.equals(tempPkValTuple)){
          // when pkVal change, stop there
          pkValTuple = tempPkValTuple;
          break;
        }
        fdbkvPairs.add(new FDBKVPair(recordStorePath, keyTuple, valTuple));
      }
      if (!fdbkvPairs.isEmpty()) {
        currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
      }

      if (iterator.hasNext()) {
        // reset the iterator
        iterator = FDBHelper.getKVPairIterableWithPrefixInDirectory(directorySubspace, tx, pkValTuple, isInitializedToLast).iterator();
      }

    } else {
      // Column Storage
      // get the attribute name from the current key-value entry
      KeyValue kv = iterator.next();
      Tuple keyTuple = directorySubspace.unpack(kv.getKey());
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      String attrName = RecordsTransformer.getAttributeNameFromTuples(keyTuple, valTuple);
      if (isInitializing) {
        currentAttributeName = attrName;
      }
      if (currentAttributeName.equals(attrName)) {
        // scan the whole directory space for the records
        AsyncIterator<KeyValue> ite = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast).iterator();
        Tuple primaryVal = getPrimaryKeyValTuple(keyTuple);

        while (ite.hasNext()) {
          kv = ite.next();
          keyTuple = directorySubspace.unpack(kv.getKey());
          valTuple = Tuple.from(kv.getValue());

          List<Object> primVals = getPrimaryKeyValTuple(keyTuple).getItems();
          if (primVals.equals(primaryVal)) {
            fdbkvPairs.add(new FDBKVPair(recordStorePath, keyTuple, valTuple));
          }
        }
      }

      if (!fdbkvPairs.isEmpty()) {
        currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
      }
    }
    return currentRecord;
  }

  public Record getFirst(Transaction tx) {
    isInitializedToLast = false;
    return seek(tx, true);
  }

  public Record getLast(Transaction tx) {
    isInitializedToLast = true;
    return seek(tx, true);
  }

  public boolean hasNext() {
    return iterator != null && iterator.hasNext();
  }

  public Record next(Transaction tx) {

    return seek(tx, false);
  }
}
