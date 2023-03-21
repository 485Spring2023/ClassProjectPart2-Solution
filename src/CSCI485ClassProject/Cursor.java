package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AttributeType;
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
import java.util.List;
import java.util.Set;

import static CSCI485ClassProject.RecordsTransformer.getPrimaryKeyValTuple;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE,
  }

  private boolean isPredicateEnabled = false;
  private String predicateAttributeName;
  private Record.Value predicateAttributeValue;
  private ComparisonOperator predicateOperator;

  private String tableName;
  private TableMetadata tableMetadata;
  private RecordsTransformer recordsTransformer;

  private boolean isInitialized = false;
  private boolean isInitializedToLast = false;

  private final Mode mode;

  private AsyncIterator<KeyValue> iterator = null;

  private Record currentRecord = null;
  // used by the col storage
  private String currentAttributeName;

  private Transaction tx;

  private DirectorySubspace directorySubspace;

  private boolean isMoved = false;
  private FDBKVPair currentKVPair = null;

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

    tx = null;
  }

  public void commit() {
    if (iterator != null) {
      iterator.cancel();
    }
    if (tx != null) {
      FDBHelper.commitTransaction(tx);
    }

    tx = null;
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

  public StatusCode enablePredicate(String attrName, Record.Value value, ComparisonOperator operator) {
    this.predicateAttributeName = attrName;
    this.predicateAttributeValue = value;
    this.predicateOperator = operator;
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

    boolean isSavePK = false;
    Tuple pkValTuple = new Tuple();
    Tuple tempPkValTuple = null;
    if (isMoved && currentKVPair != null) {
      fdbkvPairs.add(currentKVPair);
      pkValTuple = getPrimaryKeyValTuple(currentKVPair.getKey());
      isSavePK = true;
    }

    isMoved = true;
    boolean nextExists = false;

    while (iterator.hasNext()) {
      KeyValue kv = iterator.next();
      Tuple keyTuple = directorySubspace.unpack(kv.getKey());
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      FDBKVPair kvPair = new FDBKVPair(recordStorePath, keyTuple, valTuple);
      tempPkValTuple = getPrimaryKeyValTuple(keyTuple);
      if (!isSavePK) {
        pkValTuple = tempPkValTuple;
        isSavePK = true;
      } else if (!pkValTuple.equals(tempPkValTuple)){
        // when pkVal change, stop there
        currentKVPair = kvPair;
        nextExists = true;
        break;
      }
      fdbkvPairs.add(kvPair);
    }
    if (!fdbkvPairs.isEmpty()) {
      currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
    }

    if (!nextExists) {
      currentKVPair = null;
    }
    return currentRecord;
  }

  public Record getFirst() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = false;

    Record record = seek(tx, true);
    if (isPredicateEnabled) {
      while (!doesRecordMatchPredicate(record)) {
        record = seek(tx, false);
        if (record == null) {
          break;
        }
      }
    }
    return record;
  }

  private boolean doesRecordMatchPredicate(Record record) {
    Object recVal = record.getValueForGivenAttrName(predicateAttributeName);
    AttributeType recType = record.getTypeForGivenAttrName(predicateAttributeName);
    if (recVal == null || recType == null) {
      // attribute not exists in this record
      return false;
    }

    if (recType == AttributeType.INT) {
      return CursorUtils.compareTwoINT(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.DOUBLE){
      return CursorUtils.compareTwoDOUBLE(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.VARCHAR) {
      return CursorUtils.compareTwoVARCHAR(recVal, predicateAttributeValue.getValue(), predicateOperator);
    }

    return false;
  }

  public Record getLast() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = true;

    Record record = seek(tx, true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = seek(tx, false);
      }
    }
    return record;
  }

  public boolean hasNext() {
    return isInitialized && iterator != null && (iterator.hasNext() || currentKVPair != null);
  }

  public Record next(boolean isGetPrevious) {
    if (!isInitialized) {
      return null;
    }
    if (isGetPrevious != isInitializedToLast) {
      return null;
    }

    Record record = seek(tx, false);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = seek(tx, false);
      }
    }
    return record;
  }

  public StatusCode updateCurrentRecord(String[] attrNames, Object[] attrValues) {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    Set<String> currentAttrNames = currentRecord.getMapAttrNameToValue().keySet();
    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];

      if (!currentAttrNames.contains(attrNameToUpdate)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }

      if (!Record.Value.isTypeSupported(attrValToUpdate)) {
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }
    }

    // delete the current record first
    StatusCode deleteStatus = deleteCurrentRecord();
    if (deleteStatus != StatusCode.SUCCESS) {
      return deleteStatus;
    }

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      currentRecord.setAttrNameAndValue(attrNameToUpdate, attrValToUpdate);
    }

    List<FDBKVPair> kvPairsToUpdate = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToUpdate) {
      FDBHelper.setFDBKVPair(directorySubspace, tx, kv);
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode deleteCurrentRecord() {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

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
