package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.pck1.TestPersist;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SqliteObjectPersistorTest {
  private Path testLocation = null;
  private final String baseFilename = "rca.test.file";

  @Before
  public void init() throws IOException {
    String cwd = System.getProperty("user.dir");
    testLocation = Paths.get(cwd, "src", "test", "resources", "tmp", "file_rotate");
    Files.createDirectories(testLocation);
    FileUtils.cleanDirectory(testLocation.toFile());
  }

  @After
  public void cleanup() throws IOException {
    FileUtils.cleanDirectory(testLocation.toFile());
  }

  @Test
  public void testWriteObject() throws Exception {
    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    PersistorTestExample persistorTestExample = new PersistorTestExample();
    sqlite.write(persistorTestExample);

    PersistorTestExample persistorTestExampleOut = sqlite.read(PersistorTestExample.class);

    Assert.assertEquals(persistorTestExample.x, persistorTestExampleOut.x);
    Assert.assertEquals(persistorTestExample.name, persistorTestExampleOut.name);
    Assert.assertEquals(persistorTestExample.bObj.x, persistorTestExampleOut.bObj.x, 0.01);
  }

  /**
   * This test checks that we get a null Object in case the Database is created but it does not contain table with the object data we are
   * asking for.
   */
  @Test
  public void testNoData()
      throws IOException, SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    Assert.assertNull(sqlite.read(PersistorTestExample.class));
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void persistTwoClassesWithSameName() throws Exception {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("There is already a table in the Database with the same name");

    TestPersist testPersist1 = new TestPersist();
    com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.pck2.TestPersist testPersist2 =
        new com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.pck2.TestPersist();

    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    sqlite.write(testPersist1);
    sqlite.write(testPersist2);
  }

  @Test
  public void testObjectWithNoGetters()
      throws IOException, SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    exceptionRule.expect(NoSuchMethodException.class);
    exceptionRule.expectMessage("Could not find 'getter' for the field 'x' of class");

    class NoGetter {
      @ValueColumn
      int x;
    }

    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    sqlite.write(new NoGetter());
  }

  @Test
  public void testGetterReturnMismatch()
      throws IOException, SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    exceptionRule.expect(NoSuchMethodException.class);
    exceptionRule.expectMessage("The return type of the getter 'getX' (class java.lang.Integer) and field 'x' (int) don't match.");
    class TypeMismatch {
      @ValueColumn
      int x;

      public Integer getX() {
        return x;
      }

      public void setX(int x) {
        this.x = x;
      }
    }

    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    sqlite.write(new TypeMismatch());
  }

  @Test
  public void testSetterArgTypeMismatch()
      throws IOException, SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    exceptionRule.expect(NoSuchMethodException.class);
    exceptionRule.expectMessage("Could not find 'setter' for the field 'x' of class");
    class TypeMismatch {
      @ValueColumn
      int x;

      public int getX() {
        return x;
      }

      public void setX(Integer x) {
        this.x = x;
      }
    }

    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    sqlite.write(new TypeMismatch());
  }

  @Test
  public void testNonPublicGetterSetter()
      throws IOException, SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("Found 'getX'. But it is not public");
    class TypeMismatch {
      @ValueColumn
      int x;

      int getX() {
        return x;
      }

      public void setX(Integer x) {
        this.x = x;
      }
    }

    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    sqlite.write(new TypeMismatch());
  }

  @Test
  public void testNoPersistableFields()
      throws IOException, SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage(
        "NotPersistable was asked to be persisted but there are no fields with annotations: ValueColumn or RefColumn");
    class NotPersistable {
      int x;
    }

    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    sqlite.write(new NotPersistable());
  }

  @Test
  public void testCollectionOfPrimitives()
      throws IOException, SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("persisting Primitives or Strings as Parameterized Types is not supported");

    class CollectionOfPrimitives {
      @ValueColumn
      List<Integer> ll = new ArrayList<>();

      public List<Integer> getLl() {
        return ll;
      }

      public void setLl(List<Integer> x) {
        this.ll = x;
      }
    }

    SQLitePersistor sqlite = new SQLitePersistor(
        testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
    sqlite.write(new CollectionOfPrimitives());
  }

  static class PersistorTestExample {
    @ValueColumn
    int x;

    int y;

    @ValueColumn
    String name;

    @RefColumn
    B bObj;

    @RefColumn
    List<ITUtil> myList;

    public PersistorTestExample() {
      this.x = 10;
      this.y = 20;
      this.name = "test-name";
      this.bObj = new B();
      this.myList = new ArrayList<>();
      myList.add(new ITUtilImpl1());
      myList.add(new ITUtilImpl1());
      myList.add(new ITUtilImpl2());
    }

    public void setY(Integer y) {
      this.y = y;
    }

    public void setBObj(B bObj) {
      this.bObj = bObj;
    }

    public void setMyList(List<ITUtil> myList) {
      this.myList = myList;
    }

    public int getX() {
      return x;
    }

    public void setX(int x) {
      this.x = x;
    }

    public int getY() {
      return y;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public B getBObj() {
      return bObj;
    }

    public List<ITUtil> getMyList() {
      return myList;
    }
  }

  static class B {
    @ValueColumn
    double x = 5.55;
    int y = 7;

    public B() {
    }

    public void setX(double x) {
      this.x = x;
    }

    public void setY(Integer y) {
      this.y = y;
    }

    public double getX() {
      return x;
    }

    public int getY() {
      return y;
    }
  }

  interface ITUtil {

  }

  static class ITUtilImpl1 implements ITUtil {
    @ValueColumn
    boolean yes = true;

    public void setYes(boolean yes) {
      this.yes = yes;
    }

    public ITUtilImpl1() {
    }

    public boolean isYes() {
      return yes;
    }
  }

  static class ITUtilImpl2 implements ITUtil {
    @ValueColumn
    boolean no = false;

    public ITUtilImpl2() {
    }

    public void setNo(boolean no) {
      this.no = no;
    }

    public boolean isNo() {
      return no;
    }
  }
}
