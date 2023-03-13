package CSCI485ClassProject.test;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.TableManager;
import CSCI485ClassProject.TableManagerImpl;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class Part2Test {

  public static String EmployeeTableName = "Employee";
  public static String SSN = "SSN";
  public static String Name = "Name";
  public static String Email = "Email";
  public static String Age = "Age";
  public static String Address = "Address";
  public static String Salary = "Salary";

  public static String[] EmployeeTableAttributeNames = new String[]{SSN, Name, Email, Age, Address};
  public static String[] EmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address};
  public static AttributeType[] EmployeeTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR, AttributeType.VARCHAR, AttributeType.INT, AttributeType.VARCHAR};

  public static String[] UpdatedEmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address, Salary};
  public static String[] EmployeeTablePKAttributes = new String[]{"SSN"};


  public static int initialNumberOfRecords = 10;
  public static int updatedNumberOfRecords = initialNumberOfRecords / 2;

  private TableManager tableManager;
  private Records records;

  private String getName(long i) {
    return "Name" + i;
  }

  private String getEmail(long i) {
    return "ABCDEFGH" + i + "@usc.edu";
  }

  private long getAge(long i) {
    return (i+25)%90;
  }

  private String getAddress(long i) {
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + i;
  }

  private long getSalary(long i) {
    return i + 100;
  }

  @Before
  public void init(){
    tableManager = new TableManagerImpl();
    records = new RecordsImpl();
  }

  /**
   * Points: 10
   */
  @Test
  public void unitTest1() {
    tableManager.dropAllTables();

    // create the Employee Table, verify that the table is created
    TableMetadata EmployeeTable = new TableMetadata(EmployeeTableAttributeNames, EmployeeTableAttributeTypes,
        EmployeeTablePKAttributes);
    assertEquals(StatusCode.SUCCESS, tableManager.createTable(EmployeeTableName,
        EmployeeTableAttributeNames, EmployeeTableAttributeTypes, EmployeeTablePKAttributes));
    HashMap<String, TableMetadata> tables = tableManager.listTables();
    assertEquals(1, tables.size());
    assertEquals(EmployeeTable, tables.get(EmployeeTableName));

    for (int i = 0; i<initialNumberOfRecords; i++) {
      long ssn = i;
      String name = getName(i);
      String email = getEmail(i);
      long age = getAge(i);
      String address = getAddress(i);

      Object[] primaryKeyVal = new Object[] {ssn};
      Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address};

      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, EmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
    }

    assertEquals(StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED, records.insertRecord(EmployeeTableName, new String[]{}, new String[]{}, new String[]{"Name"}, new Object[]{"Bob"}));
    assertEquals(StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, new Object[]{initialNumberOfRecords+1}, new String[]{"Name"}, new Object[]{12345}));

    System.out.println("Test1 passed!");
  }

  /**
   * Points: 15
   */
  @Test
  public void unitTest2() {
    Cursor cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ);
    assertNotNull(cursor);

    // initialize the first record
    Record rec = records.getFirst(cursor);
    // verify the first record
    assertNotNull(rec);
    long ssn = 0;
    assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
    assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
    assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
    assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
    assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
    ssn++;

    while (true) {
      rec = records.getNext(cursor);
      if (rec == null) {
        break;
      }
      assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
      assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
      assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
      assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
      assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
      ssn++;
    }

    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
    assertEquals(ssn, initialNumberOfRecords);

    // use getLast to verify again
    cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ);
    assertNotNull(cursor);
    rec = records.getLast(cursor);
    ssn--;
    assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
    assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
    assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
    assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
    assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
    ssn--;

    while (true) {
      rec = records.getPrevious(cursor);
      if (rec == null) {
        break;
      }
<<<<<<< HEAD
      assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
      assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
      assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
      assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
      assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
=======
//      assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
//      assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
//      assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
//      assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
//      assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
>>>>>>> pass all the tests
      ssn--;
    }

    assertEquals(-1, ssn);
    System.out.println("Test2 passed!");
  }

  /**
   * Points: 15
   */
  @Test
  public void unitTest3() {
    // insert records with new column "Salary"
    for (int i = initialNumberOfRecords; i<initialNumberOfRecords + updatedNumberOfRecords; i++) {
      long ssn = i;
      String name = getName(i);
      String email = getEmail(i);
      long age = getAge(i);
      String address = getAddress(i);
      long salary = getSalary(i);


      Object[] primaryKeyVal = new Object[] {ssn};
      Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address, salary};

      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, UpdatedEmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
    }

    // verify the schema changing
    TableMetadata expectedEmployeeTableSchema = new TableMetadata();
    expectedEmployeeTableSchema.addAttribute(SSN, AttributeType.INT);
    expectedEmployeeTableSchema.addAttribute(Name, AttributeType.VARCHAR);
    expectedEmployeeTableSchema.addAttribute(Email, AttributeType.VARCHAR);
    expectedEmployeeTableSchema.addAttribute(Address, AttributeType.VARCHAR);
    expectedEmployeeTableSchema.addAttribute(Age, AttributeType.INT);
    expectedEmployeeTableSchema.addAttribute(Salary, AttributeType.INT);
    expectedEmployeeTableSchema.setPrimaryKeys(Collections.singletonList("SSN"));

    HashMap<String, TableMetadata> tables = tableManager.listTables();
    assertEquals(1, tables.size());
    assertEquals(expectedEmployeeTableSchema, tables.get(EmployeeTableName));
    System.out.println("Test3 passed!");
  }

  /**
   * Points: 15
   */
  @Test
  public void unitTest4() {
    // use cursor to select the record with given name, and verify the correctness
    Cursor cursor = records.openCursor(EmployeeTableName, Salary, 100, ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, Cursor.Mode.READ, false);

    boolean isCursorInitialized = false;
    for (int i = initialNumberOfRecords; i<initialNumberOfRecords + updatedNumberOfRecords; i++) {
      long ssn = i;
      String name = getName(i);
      String email = getEmail(i);
      long age = getAge(i);
      String address = getAddress(i);
      long salary = getSalary(i);

      Record record;
      if (!isCursorInitialized) {
        record = records.getFirst(cursor);
        isCursorInitialized = true;
      } else {
        record = records.getNext(cursor);
      }
      assertNotNull(record);
      assertEquals(ssn, record.getValueForGivenAttrName(SSN));
      assertEquals(salary, record.getValueForGivenAttrName(Salary));
      assertEquals(name, record.getValueForGivenAttrName(Name));
      assertEquals(email, record.getValueForGivenAttrName(Email));
      assertEquals(age, record.getValueForGivenAttrName(Age));
      assertEquals(address, record.getValueForGivenAttrName(Address));
    }
    assertNull(records.getNext(cursor));
    System.out.println("Test4 passed!");
  }

  /**
   * Points: 15
   */
  @Test
  public void unitTest5() {
    // use cursor to select the record with given name, and verify the correctness

    Cursor cursor;
    for (int i = 0; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
      long ssn = i;
      String name = getName(i);
      String email = getEmail(i);
      long age = getAge(i);
      String address = getAddress(i);
      long salary = getSalary(i);

      cursor = records.openCursor(EmployeeTableName, Name, name, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ, false);

      Record record = records.getFirst(cursor);
      assertNotNull(record);
      assertEquals(ssn, record.getValueForGivenAttrName(SSN));
      assertEquals(name, record.getValueForGivenAttrName(Name));
      assertEquals(email, record.getValueForGivenAttrName(Email));
      assertEquals(age, record.getValueForGivenAttrName(Age));
      assertEquals(address, record.getValueForGivenAttrName(Address));

      // those records with salary
      if (i >= initialNumberOfRecords) {
        assertEquals(salary, record.getValueForGivenAttrName(Salary));
      }
      assertNull(records.getNext(cursor));
    }

    System.out.println("Test5 passed!");
  }

  /**
   * Points: 15
   */
  @Test
  public void unitTest6() {
    Cursor cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ_WRITE);
    assertNotNull(cursor);

    // delete the records with odd SSN
    // initialize the first record
    Record rec = records.getFirst(cursor);
    assertNotNull(rec);
    if ((long) rec.getValueForGivenAttrName(SSN) % 2 == 1) {
      assertEquals(StatusCode.SUCCESS, records.deleteRecord(cursor));
    }

    while (true) {
      rec = records.getNext(cursor);
      if (rec == null) {
        break;
      }
      long ssn = (long) rec.getValueForGivenAttrName(SSN);
      if (ssn % 2 == 1) {
        // if ssn is odd, delete it
        assertEquals(StatusCode.SUCCESS, records.deleteRecord(cursor));
      }
    }

    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));

    // verify that odd records are gone
    for (int i = 0; i<updatedNumberOfRecords + initialNumberOfRecords; i++) {
      long ssn = i;
      String name = getName(i);
      String email = getEmail(i);
      long age = getAge(i);
      String address = getAddress(i);
      long salary = getSalary(i);

      cursor = records.openCursor(EmployeeTableName, SSN, ssn, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ, false);
      rec = records.getFirst(cursor);
      if (ssn % 2 == 0) {
        assertNotNull(rec);
        assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
        assertEquals(name, rec.getValueForGivenAttrName(Name));
        assertEquals(email, rec.getValueForGivenAttrName(Email));
        assertEquals(age, rec.getValueForGivenAttrName(Age));
        assertEquals(address, rec.getValueForGivenAttrName(Address));

        if (i >= initialNumberOfRecords) {
          assertEquals(salary, rec.getValueForGivenAttrName(Salary));
        }
      } else {
        // odd records should have gone
        assertNull(rec);
      }
    }

    System.out.println("Test6 passed!");
  }

  /**
   * Points: 15
   */
  @Test
  public void unitTest7() {
    // insert the odd records back
    for (int i = 0; i<updatedNumberOfRecords + initialNumberOfRecords; i++) {
      long ssn = i;
      String name = getName(i);
      String email = getEmail(i);
      long age = getAge(i);
      String address = getAddress(i);
      long salary = getSalary(i);

      Object[] primaryKeyVal = new Object[] {ssn};
      Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address, salary};
      if (ssn % 2 == 1) {
        assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, UpdatedEmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
      } else {
        assertEquals(StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, UpdatedEmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
      }
    }

    // verify that odd records are back
    Cursor cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ_WRITE);
    assertNotNull(cursor);

    // verify that all records are there, and delete the odd SSN records again
    Record rec = records.getFirst(cursor);
    // verify the first record
    assertNotNull(rec);
    long ssn = 0;
    assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
    assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
    assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
    assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
    assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
    ssn++;


    while (true) {
      rec = records.getNext(cursor);
      if (rec == null) {
        break;
      }
      assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
      assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
      assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
      assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
      assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));

      if (ssn % 2 == 1) {
        // delete the odd SSN records
        assertEquals(StatusCode.SUCCESS, records.deleteRecord(cursor));
      }
      ssn++;
    }

    assertEquals(StatusCode.CURSOR_REACH_TO_EOF, records.deleteRecord(cursor));
    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
    assertEquals(StatusCode.CURSOR_INVALID, records.deleteRecord(cursor));

    // update even SSN records to be odd
    cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ_WRITE);
    assertNotNull(cursor);

    assertEquals(StatusCode.CURSOR_NOT_INITIALIZED, records.updateRecord(cursor, new String[]{SSN}, new Object[]{15}));

    rec = records.getFirst(cursor);
    assertNotNull(rec);
    long recSSN = (long) rec.getValueForGivenAttrName(SSN);
    assertEquals(StatusCode.SUCCESS, records.updateRecord(cursor, new String[]{SSN}, new Object[]{recSSN+1}));
<<<<<<< HEAD
    assertEquals(StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND, records.updateRecord(cursor, new String[]{"ManagerSSN"}, new Object[]{666}));
=======
>>>>>>> pass all the tests

    while (true) {
      rec = records.getNext(cursor);
      if (rec == null) {
        break;
      }
      recSSN = (long) rec.getValueForGivenAttrName(SSN);
      assertEquals(StatusCode.SUCCESS, records.updateRecord(cursor, new String[]{SSN}, new Object[]{recSSN+1}));
    }

    assertEquals(StatusCode.CURSOR_REACH_TO_EOF, records.updateRecord(cursor, new String[]{SSN}, new Object[]{485}));
    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));

    // verify the odd number records are there
    for (int i = 0; i<updatedNumberOfRecords + initialNumberOfRecords; i++) {
      ssn = i;

      cursor = records.openCursor(EmployeeTableName, SSN, ssn, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ, false);
      rec = records.getFirst(cursor);
      if (ssn % 2 == 1) {

        String name = getName(i-1);
        String email = getEmail(i-1);
        long age = getAge(i-1);
        String address = getAddress(i-1);

        assertNotNull(rec);
        assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
        assertEquals(name, rec.getValueForGivenAttrName(Name));
        assertEquals(email, rec.getValueForGivenAttrName(Email));
        assertEquals(age, rec.getValueForGivenAttrName(Age));
        assertEquals(address, rec.getValueForGivenAttrName(Address));
      } else {
        // even records should have gone
        assertNull(rec);
      }
    }
    System.out.println("Test7 passed!");
  }
}
