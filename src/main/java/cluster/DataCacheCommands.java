package cluster;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base commands used by the DPCache-style cache layer.
 *
 * Sharding rule:
 * tenantCode + "_" + terminalCode + "_CacheItemTypeShard_" + cacheEntryClassName
 *
 * Individual cache entries add "_" + id to create their entity id.
 */
public class DataCacheCommands {
  private final String tenantCode;
  private final String terminalCode;
  private final String cacheEntryClassName;
  private final String moduleCode;

  @JsonCreator
  public DataCacheCommands(
      @JsonProperty("tenantCode") String tenantCode,
      @JsonProperty("terminalCode") String terminalCode,
      @JsonProperty("cacheEntryClassName") String cacheEntryClassName,
      @JsonProperty("moduleCode") String moduleCode) {
    this.tenantCode = tenantCode;
    this.terminalCode = terminalCode;
    this.cacheEntryClassName = cacheEntryClassName;
    this.moduleCode = moduleCode;
  }

  public String getTenantCode() { return tenantCode; }
  public String getTerminalCode() { return terminalCode; }
  public String getCacheEntryClassName() { return cacheEntryClassName; }
  public String getModuleCode() { return moduleCode; }

  /**
   * Type/shard-level entity id.
   */
  public String entityId() {
    return tenantCode + "_" + terminalCode
        + "_CacheItemTypeShard_" + cacheEntryClassName;
  }

  public static class CreateDataCacheEntries<T> extends DataCacheCommands {
    private final Map<String, CachedItem<T>> cacheEntryMap;

    @JsonCreator
    public CreateDataCacheEntries(
        @JsonProperty("tenantCode") String tenantCode,
        @JsonProperty("terminalCode") String terminalCode,
        @JsonProperty("moduleCode") String moduleCode,
        @JsonProperty("cacheEntryMap") Map<String, CachedItem<T>> cacheEntryMap) {

      super(
          tenantCode,
          terminalCode,
          firstClassName(cacheEntryMap),
          moduleCode);

      this.cacheEntryMap = cacheEntryMap;
    }

    public Map<String, CachedItem<T>> getCacheEntryMap() {
      return cacheEntryMap;
    }

    private static String firstClassName(
        Map<String, ? extends CachedItem<?>> map) {
      if (map == null || map.isEmpty()) {
        throw new IllegalArgumentException("cacheEntryMap cannot be empty");
      }
      return map.values().iterator().next().getCacheEntryClassName();
    }
  }

  public static class RefreshDataCacheEntries<T>
      extends DataCacheCommands {
    private final Map<String, CachedItem<T>> cacheEntryMap;

    @JsonCreator
    public RefreshDataCacheEntries(
        @JsonProperty("tenantCode") String tenantCode,
        @JsonProperty("terminalCode") String terminalCode,
        @JsonProperty("moduleCode") String moduleCode,
        @JsonProperty("cacheEntryMap") Map<String, CachedItem<T>> cacheEntryMap) {

      super(
          tenantCode,
          terminalCode,
          firstClassName(cacheEntryMap),
          moduleCode);

      this.cacheEntryMap = cacheEntryMap;
    }

    public Map<String, CachedItem<T>> getCacheEntryMap() {
      return cacheEntryMap;
    }

    private static String firstClassName(
        Map<String, ? extends CachedItem<?>> map) {
      if (map == null || map.isEmpty()) {
        throw new IllegalArgumentException("cacheEntryMap cannot be empty");
      }
      return map.values().iterator().next().getCacheEntryClassName();
    }
  }

  public static class CreateDataCacheEntry<T>
      extends DataCacheCommands {
    private final CachedItem<T> cachedItem;
    private final String id;

    @JsonCreator
    public CreateDataCacheEntry(
        @JsonProperty("tenantCode") String tenantCode,
        @JsonProperty("terminalCode") String terminalCode,
        @JsonProperty("moduleCode") String moduleCode,
        @JsonProperty("cachedItem") CachedItem<T> cachedItem,
        @JsonProperty("id") String id) {

      super(
          tenantCode,
          terminalCode,
          cachedItem.getCacheEntryClassName(),
          moduleCode);

      this.cachedItem = cachedItem;
      this.id = id;
    }

    public CachedItem<T> getCachedItem() { return cachedItem; }
    public String getId() { return id; }

    @Override
    public String entityId() {
      return getTenantCode() + "_" + getTerminalCode()
          + "_" + getCachedItem().getCacheEntryClassName()
          + "_" + getId();
    }
  }
}
