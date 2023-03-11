package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  private Database db;

  public TableManagerImpl() {
    db = FDBHelper.initialization();
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                                String[] primaryKeyAttributeNames) {
    // your code
    // First, check if table already exists
    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);

    if (attributeNames == null || attributeType == null || primaryKeyAttributeNames == null) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    if (attributeNames.length == 0 || attributeType.length == 0 || attributeType.length != attributeNames.length) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    if (primaryKeyAttributeNames.length == 0) {
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    }
    Transaction tx = FDBHelper.openTransaction(db);
    if (FDBHelper.doesSubdirectoryExists(tx, tableSubdirectory)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_ALREADY_EXISTS;
    }

    TableMetadata tblMetadata = new TableMetadata();
    HashMap<String, AttributeType> attributes = new HashMap<>();
    for (int i = 0; i < attributeNames.length; i++) {
      attributes.put(attributeNames[i], attributeType[i]);
    }

    tblMetadata.setAttributes(attributes);
    StatusCode isPrimaryKeyAdded = tblMetadata.setPrimaryKeys(Arrays.asList(primaryKeyAttributeNames));
    if (isPrimaryKeyAdded != StatusCode.SUCCESS) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
    }

    // persist the creation
    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    DirectorySubspace tableAttrSpace = FDBHelper.createOrOpenSubspace(tx, transformer.getTableAttributeStorePath());

    List<FDBKVPair> pairs = transformer.convertToFDBKVPairs(tblMetadata);
    for (FDBKVPair kvPair : pairs) {
      FDBHelper.setFDBKVPair(tableAttrSpace, tx, kvPair);
    }
    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
    // First, check if table exists
    Transaction tx = FDBHelper.openTransaction(db);
    List<String> tableSubdirectory = new ArrayList<>();

    tableSubdirectory.add(tableName);
    if (!FDBHelper.doesSubdirectoryExists(tx, tableSubdirectory)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_NOT_FOUND;
    }
    FDBHelper.dropSubspace(tx, tableSubdirectory);
    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    Transaction readTx = FDBHelper.openTransaction(db);
    HashMap<String, TableMetadata> res = new HashMap<>();
    List<String> existingTableNames = FDBHelper.getAllDirectSubspaceName(readTx);

    for (String tblName : existingTableNames) {
      TableMetadataTransformer tblTransformer = new TableMetadataTransformer(tblName);
      List<String> tblAttributeDirPath = tblTransformer.getTableAttributeStorePath();
      List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, readTx, tblAttributeDirPath);
      TableMetadata tblMetadata = tblTransformer.convertBackToTableMetadata(kvPairs);
      res.put(tblName, tblMetadata);
    }

    FDBHelper.commitTransaction(readTx);
    return res;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    Transaction tx = FDBHelper.openTransaction(db);
    // check if the table exists
    if (!FDBHelper.doesSubdirectoryExists(tx,Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_NOT_FOUND;
    }
    // retrieve attributes of the table, check if attributes exists
    TableMetadataTransformer tblTransformer = new TableMetadataTransformer(tableName);
    List<String> tblAttributeDirPath = tblTransformer.getTableAttributeStorePath();
    DirectorySubspace tableAttrDir = FDBHelper.openSubspace(tx, tblAttributeDirPath);

    // retrieve the target attribute, check if that attribute exists
    FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(tableAttrDir, tx, TableMetadataTransformer.getTableAttributeKeyTuple(attributeName), tblAttributeDirPath);
    if (pair != null) {
      // KVPair exists
      FDBHelper.abortTransaction(tx);
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }

    FDBHelper.setFDBKVPair(tableAttrDir, tx, tblTransformer.getAttributeKVPair(attributeName, attributeType));

    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    Transaction tx = FDBHelper.openTransaction(db);
    // check if the table exists
    if (!FDBHelper.doesSubdirectoryExists(tx,Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.TABLE_NOT_FOUND;
    }

    TableMetadataTransformer transformer = new TableMetadataTransformer(tableName);
    List<String> tblAttributeDirPath = transformer.getTableAttributeStorePath();
    DirectorySubspace tableAttrDir = FDBHelper.openSubspace(tx, tblAttributeDirPath);

    // retrieve the target attribute, check if that attribute exists
    FDBKVPair pair = FDBHelper.getCertainKeyValuePairInSubdirectory(tableAttrDir, tx, TableMetadataTransformer.getTableAttributeKeyTuple(attributeName), tblAttributeDirPath);
    if (pair == null) {
      // KVPair not exists
      FDBHelper.abortTransaction(tx);
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }
    // if exists, remove the attribute corresponding kvPair
    FDBHelper.removeKeyValuePair(tableAttrDir, tx, pair.getKey());
    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    FDBHelper.clear(db);
    return StatusCode.SUCCESS;
  }
}
