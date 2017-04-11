/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package edu.nyu.tandon.experiments.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.10.0)", date = "2017-04-11")
public class QueryFeatures implements org.apache.thrift.TBase<QueryFeatures, QueryFeatures._Fields>, java.io.Serializable, Cloneable, Comparable<QueryFeatures> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("QueryFeatures");

  private static final org.apache.thrift.protocol.TField QUERY_FIELD_DESC = new org.apache.thrift.protocol.TField("query", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("time", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField MAXLIST1_FIELD_DESC = new org.apache.thrift.protocol.TField("maxlist1", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField MAXLIST2_FIELD_DESC = new org.apache.thrift.protocol.TField("maxlist2", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField MINLIST1_FIELD_DESC = new org.apache.thrift.protocol.TField("minlist1", org.apache.thrift.protocol.TType.I64, (short)5);
  private static final org.apache.thrift.protocol.TField MINLIST2_FIELD_DESC = new org.apache.thrift.protocol.TField("minlist2", org.apache.thrift.protocol.TType.I64, (short)6);
  private static final org.apache.thrift.protocol.TField SUMLIST_FIELD_DESC = new org.apache.thrift.protocol.TField("sumlist", org.apache.thrift.protocol.TType.I64, (short)7);
  private static final org.apache.thrift.protocol.TField SHARD_FIELD_DESC = new org.apache.thrift.protocol.TField("shard", org.apache.thrift.protocol.TType.I32, (short)8);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new QueryFeaturesStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new QueryFeaturesTupleSchemeFactory();

  public int query; // required
  public long time; // optional
  public long maxlist1; // optional
  public long maxlist2; // optional
  public long minlist1; // optional
  public long minlist2; // optional
  public long sumlist; // optional
  public int shard; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QUERY((short)1, "query"),
    TIME((short)2, "time"),
    MAXLIST1((short)3, "maxlist1"),
    MAXLIST2((short)4, "maxlist2"),
    MINLIST1((short)5, "minlist1"),
    MINLIST2((short)6, "minlist2"),
    SUMLIST((short)7, "sumlist"),
    SHARD((short)8, "shard");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // QUERY
          return QUERY;
        case 2: // TIME
          return TIME;
        case 3: // MAXLIST1
          return MAXLIST1;
        case 4: // MAXLIST2
          return MAXLIST2;
        case 5: // MINLIST1
          return MINLIST1;
        case 6: // MINLIST2
          return MINLIST2;
        case 7: // SUMLIST
          return SUMLIST;
        case 8: // SHARD
          return SHARD;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __QUERY_ISSET_ID = 0;
  private static final int __TIME_ISSET_ID = 1;
  private static final int __MAXLIST1_ISSET_ID = 2;
  private static final int __MAXLIST2_ISSET_ID = 3;
  private static final int __MINLIST1_ISSET_ID = 4;
  private static final int __MINLIST2_ISSET_ID = 5;
  private static final int __SUMLIST_ISSET_ID = 6;
  private static final int __SHARD_ISSET_ID = 7;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.TIME,_Fields.MAXLIST1,_Fields.MAXLIST2,_Fields.MINLIST1,_Fields.MINLIST2,_Fields.SUMLIST,_Fields.SHARD};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.QUERY, new org.apache.thrift.meta_data.FieldMetaData("query", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.TIME, new org.apache.thrift.meta_data.FieldMetaData("time", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.MAXLIST1, new org.apache.thrift.meta_data.FieldMetaData("maxlist1", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.MAXLIST2, new org.apache.thrift.meta_data.FieldMetaData("maxlist2", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.MINLIST1, new org.apache.thrift.meta_data.FieldMetaData("minlist1", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.MINLIST2, new org.apache.thrift.meta_data.FieldMetaData("minlist2", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SUMLIST, new org.apache.thrift.meta_data.FieldMetaData("sumlist", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SHARD, new org.apache.thrift.meta_data.FieldMetaData("shard", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(QueryFeatures.class, metaDataMap);
  }

  public QueryFeatures() {
  }

  public QueryFeatures(
    int query)
  {
    this();
    this.query = query;
    setQueryIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public QueryFeatures(QueryFeatures other) {
    __isset_bitfield = other.__isset_bitfield;
    this.query = other.query;
    this.time = other.time;
    this.maxlist1 = other.maxlist1;
    this.maxlist2 = other.maxlist2;
    this.minlist1 = other.minlist1;
    this.minlist2 = other.minlist2;
    this.sumlist = other.sumlist;
    this.shard = other.shard;
  }

  public QueryFeatures deepCopy() {
    return new QueryFeatures(this);
  }

  @Override
  public void clear() {
    setQueryIsSet(false);
    this.query = 0;
    setTimeIsSet(false);
    this.time = 0;
    setMaxlist1IsSet(false);
    this.maxlist1 = 0;
    setMaxlist2IsSet(false);
    this.maxlist2 = 0;
    setMinlist1IsSet(false);
    this.minlist1 = 0;
    setMinlist2IsSet(false);
    this.minlist2 = 0;
    setSumlistIsSet(false);
    this.sumlist = 0;
    setShardIsSet(false);
    this.shard = 0;
  }

  public int getQuery() {
    return this.query;
  }

  public QueryFeatures setQuery(int query) {
    this.query = query;
    setQueryIsSet(true);
    return this;
  }

  public void unsetQuery() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __QUERY_ISSET_ID);
  }

  /** Returns true if field query is set (has been assigned a value) and false otherwise */
  public boolean isSetQuery() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __QUERY_ISSET_ID);
  }

  public void setQueryIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __QUERY_ISSET_ID, value);
  }

  public long getTime() {
    return this.time;
  }

  public QueryFeatures setTime(long time) {
    this.time = time;
    setTimeIsSet(true);
    return this;
  }

  public void unsetTime() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __TIME_ISSET_ID);
  }

  /** Returns true if field time is set (has been assigned a value) and false otherwise */
  public boolean isSetTime() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __TIME_ISSET_ID);
  }

  public void setTimeIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __TIME_ISSET_ID, value);
  }

  public long getMaxlist1() {
    return this.maxlist1;
  }

  public QueryFeatures setMaxlist1(long maxlist1) {
    this.maxlist1 = maxlist1;
    setMaxlist1IsSet(true);
    return this;
  }

  public void unsetMaxlist1() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __MAXLIST1_ISSET_ID);
  }

  /** Returns true if field maxlist1 is set (has been assigned a value) and false otherwise */
  public boolean isSetMaxlist1() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __MAXLIST1_ISSET_ID);
  }

  public void setMaxlist1IsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __MAXLIST1_ISSET_ID, value);
  }

  public long getMaxlist2() {
    return this.maxlist2;
  }

  public QueryFeatures setMaxlist2(long maxlist2) {
    this.maxlist2 = maxlist2;
    setMaxlist2IsSet(true);
    return this;
  }

  public void unsetMaxlist2() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __MAXLIST2_ISSET_ID);
  }

  /** Returns true if field maxlist2 is set (has been assigned a value) and false otherwise */
  public boolean isSetMaxlist2() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __MAXLIST2_ISSET_ID);
  }

  public void setMaxlist2IsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __MAXLIST2_ISSET_ID, value);
  }

  public long getMinlist1() {
    return this.minlist1;
  }

  public QueryFeatures setMinlist1(long minlist1) {
    this.minlist1 = minlist1;
    setMinlist1IsSet(true);
    return this;
  }

  public void unsetMinlist1() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __MINLIST1_ISSET_ID);
  }

  /** Returns true if field minlist1 is set (has been assigned a value) and false otherwise */
  public boolean isSetMinlist1() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __MINLIST1_ISSET_ID);
  }

  public void setMinlist1IsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __MINLIST1_ISSET_ID, value);
  }

  public long getMinlist2() {
    return this.minlist2;
  }

  public QueryFeatures setMinlist2(long minlist2) {
    this.minlist2 = minlist2;
    setMinlist2IsSet(true);
    return this;
  }

  public void unsetMinlist2() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __MINLIST2_ISSET_ID);
  }

  /** Returns true if field minlist2 is set (has been assigned a value) and false otherwise */
  public boolean isSetMinlist2() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __MINLIST2_ISSET_ID);
  }

  public void setMinlist2IsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __MINLIST2_ISSET_ID, value);
  }

  public long getSumlist() {
    return this.sumlist;
  }

  public QueryFeatures setSumlist(long sumlist) {
    this.sumlist = sumlist;
    setSumlistIsSet(true);
    return this;
  }

  public void unsetSumlist() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SUMLIST_ISSET_ID);
  }

  /** Returns true if field sumlist is set (has been assigned a value) and false otherwise */
  public boolean isSetSumlist() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SUMLIST_ISSET_ID);
  }

  public void setSumlistIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SUMLIST_ISSET_ID, value);
  }

  public int getShard() {
    return this.shard;
  }

  public QueryFeatures setShard(int shard) {
    this.shard = shard;
    setShardIsSet(true);
    return this;
  }

  public void unsetShard() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SHARD_ISSET_ID);
  }

  /** Returns true if field shard is set (has been assigned a value) and false otherwise */
  public boolean isSetShard() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SHARD_ISSET_ID);
  }

  public void setShardIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SHARD_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    case QUERY:
      if (value == null) {
        unsetQuery();
      } else {
        setQuery((java.lang.Integer)value);
      }
      break;

    case TIME:
      if (value == null) {
        unsetTime();
      } else {
        setTime((java.lang.Long)value);
      }
      break;

    case MAXLIST1:
      if (value == null) {
        unsetMaxlist1();
      } else {
        setMaxlist1((java.lang.Long)value);
      }
      break;

    case MAXLIST2:
      if (value == null) {
        unsetMaxlist2();
      } else {
        setMaxlist2((java.lang.Long)value);
      }
      break;

    case MINLIST1:
      if (value == null) {
        unsetMinlist1();
      } else {
        setMinlist1((java.lang.Long)value);
      }
      break;

    case MINLIST2:
      if (value == null) {
        unsetMinlist2();
      } else {
        setMinlist2((java.lang.Long)value);
      }
      break;

    case SUMLIST:
      if (value == null) {
        unsetSumlist();
      } else {
        setSumlist((java.lang.Long)value);
      }
      break;

    case SHARD:
      if (value == null) {
        unsetShard();
      } else {
        setShard((java.lang.Integer)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case QUERY:
      return getQuery();

    case TIME:
      return getTime();

    case MAXLIST1:
      return getMaxlist1();

    case MAXLIST2:
      return getMaxlist2();

    case MINLIST1:
      return getMinlist1();

    case MINLIST2:
      return getMinlist2();

    case SUMLIST:
      return getSumlist();

    case SHARD:
      return getShard();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case QUERY:
      return isSetQuery();
    case TIME:
      return isSetTime();
    case MAXLIST1:
      return isSetMaxlist1();
    case MAXLIST2:
      return isSetMaxlist2();
    case MINLIST1:
      return isSetMinlist1();
    case MINLIST2:
      return isSetMinlist2();
    case SUMLIST:
      return isSetSumlist();
    case SHARD:
      return isSetShard();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof QueryFeatures)
      return this.equals((QueryFeatures)that);
    return false;
  }

  public boolean equals(QueryFeatures that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_query = true;
    boolean that_present_query = true;
    if (this_present_query || that_present_query) {
      if (!(this_present_query && that_present_query))
        return false;
      if (this.query != that.query)
        return false;
    }

    boolean this_present_time = true && this.isSetTime();
    boolean that_present_time = true && that.isSetTime();
    if (this_present_time || that_present_time) {
      if (!(this_present_time && that_present_time))
        return false;
      if (this.time != that.time)
        return false;
    }

    boolean this_present_maxlist1 = true && this.isSetMaxlist1();
    boolean that_present_maxlist1 = true && that.isSetMaxlist1();
    if (this_present_maxlist1 || that_present_maxlist1) {
      if (!(this_present_maxlist1 && that_present_maxlist1))
        return false;
      if (this.maxlist1 != that.maxlist1)
        return false;
    }

    boolean this_present_maxlist2 = true && this.isSetMaxlist2();
    boolean that_present_maxlist2 = true && that.isSetMaxlist2();
    if (this_present_maxlist2 || that_present_maxlist2) {
      if (!(this_present_maxlist2 && that_present_maxlist2))
        return false;
      if (this.maxlist2 != that.maxlist2)
        return false;
    }

    boolean this_present_minlist1 = true && this.isSetMinlist1();
    boolean that_present_minlist1 = true && that.isSetMinlist1();
    if (this_present_minlist1 || that_present_minlist1) {
      if (!(this_present_minlist1 && that_present_minlist1))
        return false;
      if (this.minlist1 != that.minlist1)
        return false;
    }

    boolean this_present_minlist2 = true && this.isSetMinlist2();
    boolean that_present_minlist2 = true && that.isSetMinlist2();
    if (this_present_minlist2 || that_present_minlist2) {
      if (!(this_present_minlist2 && that_present_minlist2))
        return false;
      if (this.minlist2 != that.minlist2)
        return false;
    }

    boolean this_present_sumlist = true && this.isSetSumlist();
    boolean that_present_sumlist = true && that.isSetSumlist();
    if (this_present_sumlist || that_present_sumlist) {
      if (!(this_present_sumlist && that_present_sumlist))
        return false;
      if (this.sumlist != that.sumlist)
        return false;
    }

    boolean this_present_shard = true && this.isSetShard();
    boolean that_present_shard = true && that.isSetShard();
    if (this_present_shard || that_present_shard) {
      if (!(this_present_shard && that_present_shard))
        return false;
      if (this.shard != that.shard)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + query;

    hashCode = hashCode * 8191 + ((isSetTime()) ? 131071 : 524287);
    if (isSetTime())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(time);

    hashCode = hashCode * 8191 + ((isSetMaxlist1()) ? 131071 : 524287);
    if (isSetMaxlist1())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(maxlist1);

    hashCode = hashCode * 8191 + ((isSetMaxlist2()) ? 131071 : 524287);
    if (isSetMaxlist2())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(maxlist2);

    hashCode = hashCode * 8191 + ((isSetMinlist1()) ? 131071 : 524287);
    if (isSetMinlist1())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(minlist1);

    hashCode = hashCode * 8191 + ((isSetMinlist2()) ? 131071 : 524287);
    if (isSetMinlist2())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(minlist2);

    hashCode = hashCode * 8191 + ((isSetSumlist()) ? 131071 : 524287);
    if (isSetSumlist())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(sumlist);

    hashCode = hashCode * 8191 + ((isSetShard()) ? 131071 : 524287);
    if (isSetShard())
      hashCode = hashCode * 8191 + shard;

    return hashCode;
  }

  @Override
  public int compareTo(QueryFeatures other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetQuery()).compareTo(other.isSetQuery());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQuery()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.query, other.query);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetTime()).compareTo(other.isSetTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.time, other.time);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetMaxlist1()).compareTo(other.isSetMaxlist1());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMaxlist1()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.maxlist1, other.maxlist1);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetMaxlist2()).compareTo(other.isSetMaxlist2());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMaxlist2()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.maxlist2, other.maxlist2);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetMinlist1()).compareTo(other.isSetMinlist1());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMinlist1()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.minlist1, other.minlist1);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetMinlist2()).compareTo(other.isSetMinlist2());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMinlist2()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.minlist2, other.minlist2);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetSumlist()).compareTo(other.isSetSumlist());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSumlist()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sumlist, other.sumlist);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetShard()).compareTo(other.isSetShard());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetShard()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.shard, other.shard);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("QueryFeatures(");
    boolean first = true;

    sb.append("query:");
    sb.append(this.query);
    first = false;
    if (isSetTime()) {
      if (!first) sb.append(", ");
      sb.append("time:");
      sb.append(this.time);
      first = false;
    }
    if (isSetMaxlist1()) {
      if (!first) sb.append(", ");
      sb.append("maxlist1:");
      sb.append(this.maxlist1);
      first = false;
    }
    if (isSetMaxlist2()) {
      if (!first) sb.append(", ");
      sb.append("maxlist2:");
      sb.append(this.maxlist2);
      first = false;
    }
    if (isSetMinlist1()) {
      if (!first) sb.append(", ");
      sb.append("minlist1:");
      sb.append(this.minlist1);
      first = false;
    }
    if (isSetMinlist2()) {
      if (!first) sb.append(", ");
      sb.append("minlist2:");
      sb.append(this.minlist2);
      first = false;
    }
    if (isSetSumlist()) {
      if (!first) sb.append(", ");
      sb.append("sumlist:");
      sb.append(this.sumlist);
      first = false;
    }
    if (isSetShard()) {
      if (!first) sb.append(", ");
      sb.append("shard:");
      sb.append(this.shard);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'query' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class QueryFeaturesStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public QueryFeaturesStandardScheme getScheme() {
      return new QueryFeaturesStandardScheme();
    }
  }

  private static class QueryFeaturesStandardScheme extends org.apache.thrift.scheme.StandardScheme<QueryFeatures> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, QueryFeatures struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // QUERY
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.query = iprot.readI32();
              struct.setQueryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.time = iprot.readI64();
              struct.setTimeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // MAXLIST1
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.maxlist1 = iprot.readI64();
              struct.setMaxlist1IsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // MAXLIST2
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.maxlist2 = iprot.readI64();
              struct.setMaxlist2IsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // MINLIST1
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.minlist1 = iprot.readI64();
              struct.setMinlist1IsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // MINLIST2
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.minlist2 = iprot.readI64();
              struct.setMinlist2IsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // SUMLIST
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.sumlist = iprot.readI64();
              struct.setSumlistIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 8: // SHARD
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.shard = iprot.readI32();
              struct.setShardIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetQuery()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'query' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, QueryFeatures struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(QUERY_FIELD_DESC);
      oprot.writeI32(struct.query);
      oprot.writeFieldEnd();
      if (struct.isSetTime()) {
        oprot.writeFieldBegin(TIME_FIELD_DESC);
        oprot.writeI64(struct.time);
        oprot.writeFieldEnd();
      }
      if (struct.isSetMaxlist1()) {
        oprot.writeFieldBegin(MAXLIST1_FIELD_DESC);
        oprot.writeI64(struct.maxlist1);
        oprot.writeFieldEnd();
      }
      if (struct.isSetMaxlist2()) {
        oprot.writeFieldBegin(MAXLIST2_FIELD_DESC);
        oprot.writeI64(struct.maxlist2);
        oprot.writeFieldEnd();
      }
      if (struct.isSetMinlist1()) {
        oprot.writeFieldBegin(MINLIST1_FIELD_DESC);
        oprot.writeI64(struct.minlist1);
        oprot.writeFieldEnd();
      }
      if (struct.isSetMinlist2()) {
        oprot.writeFieldBegin(MINLIST2_FIELD_DESC);
        oprot.writeI64(struct.minlist2);
        oprot.writeFieldEnd();
      }
      if (struct.isSetSumlist()) {
        oprot.writeFieldBegin(SUMLIST_FIELD_DESC);
        oprot.writeI64(struct.sumlist);
        oprot.writeFieldEnd();
      }
      if (struct.isSetShard()) {
        oprot.writeFieldBegin(SHARD_FIELD_DESC);
        oprot.writeI32(struct.shard);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class QueryFeaturesTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public QueryFeaturesTupleScheme getScheme() {
      return new QueryFeaturesTupleScheme();
    }
  }

  private static class QueryFeaturesTupleScheme extends org.apache.thrift.scheme.TupleScheme<QueryFeatures> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, QueryFeatures struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.query);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetTime()) {
        optionals.set(0);
      }
      if (struct.isSetMaxlist1()) {
        optionals.set(1);
      }
      if (struct.isSetMaxlist2()) {
        optionals.set(2);
      }
      if (struct.isSetMinlist1()) {
        optionals.set(3);
      }
      if (struct.isSetMinlist2()) {
        optionals.set(4);
      }
      if (struct.isSetSumlist()) {
        optionals.set(5);
      }
      if (struct.isSetShard()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.isSetTime()) {
        oprot.writeI64(struct.time);
      }
      if (struct.isSetMaxlist1()) {
        oprot.writeI64(struct.maxlist1);
      }
      if (struct.isSetMaxlist2()) {
        oprot.writeI64(struct.maxlist2);
      }
      if (struct.isSetMinlist1()) {
        oprot.writeI64(struct.minlist1);
      }
      if (struct.isSetMinlist2()) {
        oprot.writeI64(struct.minlist2);
      }
      if (struct.isSetSumlist()) {
        oprot.writeI64(struct.sumlist);
      }
      if (struct.isSetShard()) {
        oprot.writeI32(struct.shard);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, QueryFeatures struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.query = iprot.readI32();
      struct.setQueryIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.time = iprot.readI64();
        struct.setTimeIsSet(true);
      }
      if (incoming.get(1)) {
        struct.maxlist1 = iprot.readI64();
        struct.setMaxlist1IsSet(true);
      }
      if (incoming.get(2)) {
        struct.maxlist2 = iprot.readI64();
        struct.setMaxlist2IsSet(true);
      }
      if (incoming.get(3)) {
        struct.minlist1 = iprot.readI64();
        struct.setMinlist1IsSet(true);
      }
      if (incoming.get(4)) {
        struct.minlist2 = iprot.readI64();
        struct.setMinlist2IsSet(true);
      }
      if (incoming.get(5)) {
        struct.sumlist = iprot.readI64();
        struct.setSumlistIsSet(true);
      }
      if (incoming.get(6)) {
        struct.shard = iprot.readI32();
        struct.setShardIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
