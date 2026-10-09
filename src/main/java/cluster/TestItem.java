package cluster;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TestItem {
  private BigDecimal id;
  private String name;
  private String value;

  @JsonCreator
  public TestItem(
      @JsonProperty("id") BigDecimal id,
      @JsonProperty("name") String name,
      @JsonProperty("value") String value) {
    this.id = id;
    this.name = name;
    this.value = value;
  }

  public BigDecimal getId() { return id; }
  public void setId(BigDecimal id) { this.id = id; }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  public String getValue() { return value; }
  public void setValue(String value) { this.value = value; }

  @Override
  public String toString() {
    return "TestItem{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", value='" + value + '\'' +
        '}';
  }
}
