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
import java.util.Set;

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

  private boolean isInitialized = false;
  private boolean isInitializedToLast = false;

  private Mode mode;

  private AsyncIterator<KeyValue> iterator = null;

  private Record currentRecord = null;
  // used by the col storage
  private String currentAttributeName;

  private final Transaction tx;

  private DirectorySubspace directorySubspace;

  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.tx = tx;
  }

  public Transaction getTx() {
    return tx;
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

  public boolean isInitialized() {
    return isInitialized;
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
    // if it is not initialized, return null;
    if (!isInitializing && !isInitialized) {
      return null;
    }

    if (isInitializing) {
      // initialize the subspace and the iterator
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      directorySubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast);
      if (fdbIterable != null)
        iterator = fdbIterable.iterator();

      isInitialized = true;
    }
    // reset the currentRecord
    currentRecord = null;

    // no such directory, or no records under the directory
    if (directorySubspace == null || !hasNext()) {
      return null;
    }

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

  public Record getFirst() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = false;
    return seek(tx, true);
  }

  public Record getLast() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = true;
    return seek(tx, true);
  }

  public boolean hasNext() {
    return isInitialized && iterator != null && iterator.hasNext();
  }

  public Record next(boolean isGetPrevious) {
    if (!isInitialized) {
      return null;
    }
    if (isGetPrevious != isInitializedToLast) {
      return null;
    }
    return seek(tx, false);
  }

  public StatusCode updateCurrentRecord(String[] attrNames, Object[] attrValues) {
    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    // update the current record with new val
    Set<String> currentAttrNames = currentRecord.getMapAttrNameToValue().keySet();

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      if (!currentAttrNames.contains(attrNameToUpdate)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }
      StatusCode setAttrStatus = currentRecord.setAttrNameAndValue(attrNameToUpdate, attrValToUpdate);
      if (setAttrStatus != StatusCode.SUCCESS) {
        return setAttrStatus;
      }
    }

    List<FDBKVPair> kvPairsToUpdate = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToUpdate) {
      FDBHelper.setFDBKVPair(directorySubspace, tx, kv);
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode deleteCurrentRecord() {
    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    List<FDBKVPair> kvPairsToDelete = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToDelete) {
      FDBHelper.removeKeyValuePair(directorySubspace, tx, kv.getKey());
    }

    return StatusCode.SUCCESS;
  }
}
