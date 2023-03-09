package CSCI485ClassProject.fdb;

import com.apple.foundationdb.tuple.Tuple;

import java.util.List;


public class FDBKVPair {

  private List<String> subspacePath;
  private Tuple key;
  private Tuple value;

  public FDBKVPair(List<String> subspaces, Tuple key, Tuple value) {
    this.subspacePath = subspaces;
    this.key = key;
    this.value = value;
  }

  public FDBKVPair(List<String> subspaces, String key, Object value) {
    this.subspacePath = subspaces;

    this.key = new Tuple().add(key);
    this.value = new Tuple().addObject(value);
  }

  public List<String> getSubspacePath() {
    return subspacePath;
  }

  public void setSubspacePath(List<String> subspacePath) {
    this.subspacePath = subspacePath;
  }

  public Tuple getKey() {
    return key;
  }

  public void setKey(Tuple key) {
    this.key = key;
  }

  public Tuple getValue() {
    return value;
  }

  public void setValue(Tuple value) {
    this.value = value;
  }
}
