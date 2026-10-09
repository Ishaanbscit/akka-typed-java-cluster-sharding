package cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Wrapper used by the cache to keep the cache-entry type name together
 * with its JSON representation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
public class CachedItem<DataCacheType> {
  private final String cacheEntryClassName;
  private final JsonNode jsonNode;

  @JsonCreator
  public CachedItem(
      @JsonProperty("cacheEntryClassName") String cacheEntryClassName,
      @JsonProperty("jsonNode") JsonNode jsonNode) {
    this.cacheEntryClassName = cacheEntryClassName;
    this.jsonNode = jsonNode;
  }

  public JsonNode getJsonNode() {
    return jsonNode;
  }

  public String getCacheEntryClassName() {
    return cacheEntryClassName;
  }

  /**
   * Converts the stored JSON back to the cached POJO.
   * For simple-name entries such as TestItem, the cluster package is tried.
   */
  @SuppressWarnings("unchecked")
  public DataCacheType convertToCachedObject() {
    try {
      Class<?> type;
      if (cacheEntryClassName.contains(".")) {
        type = Class.forName(cacheEntryClassName);
      } else {
        type = Class.forName("cluster." + cacheEntryClassName);
      }

      ObjectMapper mapper = new ObjectMapper();
      return (DataCacheType) mapper.treeToValue(jsonNode, type);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to convert cached item: " + cacheEntryClassName, e);
    }
  }

  @Override
  public String toString() {
    return "CachedItem{" +
        "cacheEntryClassName='" + cacheEntryClassName + '\'' +
        ", jsonNode=" + jsonNode +
        '}';
  }
}
