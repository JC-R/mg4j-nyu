/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package edu.nyu.tandon.experiments.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.10.0)", date = "2017-04-11")
public class Result implements org.apache.thrift.TBase<Result, Result._Fields>, java.io.Serializable, Cloneable, Comparable<Result> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Result");

  private static final org.apache.thrift.protocol.TField QUERY_FIELD_DESC = new org.apache.thrift.protocol.TField("query", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField RANK_FIELD_DESC = new org.apache.thrift.protocol.TField("rank", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField LDOCID_FIELD_DESC = new org.apache.thrift.protocol.TField("ldocid", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField GDOCID_FIELD_DESC = new org.apache.thrift.protocol.TField("gdocid", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField SCORE_FIELD_DESC = new org.apache.thrift.protocol.TField("score", org.apache.thrift.protocol.TType.DOUBLE, (short)5);
  private static final org.apache.thrift.protocol.TField SHARD_FIELD_DESC = new org.apache.thrift.protocol.TField("shard", org.apache.thrift.protocol.TType.I32, (short)6);
  private static final org.apache.thrift.protocol.TField BUCKET_FIELD_DESC = new org.apache.thrift.protocol.TField("bucket", org.apache.thrift.protocol.TType.I32, (short)7);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ResultStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ResultTupleSchemeFactory();

  public int query; // required
  public int rank; // required
  public long ldocid; // optional
  public long gdocid; // optional
  public double score; // optional
  public int shard; // optional
  public int bucket; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QUERY((short)1, "query"),
    RANK((short)2, "rank"),
    LDOCID((short)3, "ldocid"),
    GDOCID((short)4, "gdocid"),
    SCORE((short)5, "score"),
    SHARD((short)6, "shard"),
    BUCKET((short)7, "bucket");

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
        case 2: // RANK
          return RANK;
        case 3: // LDOCID
          return LDOCID;
        case 4: // GDOCID
          return GDOCID;
        case 5: // SCORE
          return SCORE;
        case 6: // SHARD
          return SHARD;
        case 7: // BUCKET
          return BUCKET;
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
  private static final int __RANK_ISSET_ID = 1;
  private static final int __LDOCID_ISSET_ID = 2;
  private static final int __GDOCID_ISSET_ID = 3;
  private static final int __SCORE_ISSET_ID = 4;
  private static final int __SHARD_ISSET_ID = 5;
  private static final int __BUCKET_ISSET_ID = 6;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.LDOCID,_Fields.GDOCID,_Fields.SCORE,_Fields.SHARD,_Fields.BUCKET};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.QUERY, new org.apache.thrift.meta_data.FieldMetaData("query", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.RANK, new org.apache.thrift.meta_data.FieldMetaData("rank", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.LDOCID, new org.apache.thrift.meta_data.FieldMetaData("ldocid", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.GDOCID, new org.apache.thrift.meta_data.FieldMetaData("gdocid", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SCORE, new org.apache.thrift.meta_data.FieldMetaData("score", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.SHARD, new org.apache.thrift.meta_data.FieldMetaData("shard", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.BUCKET, new org.apache.thrift.meta_data.FieldMetaData("bucket", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Result.class, metaDataMap);
  }

  public Result() {
  }

  public Result(
    int query,
    int rank)
  {
    this();
    this.query = query;
    setQueryIsSet(true);
    this.rank = rank;
    setRankIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Result(Result other) {
    __isset_bitfield = other.__isset_bitfield;
    this.query = other.query;
    this.rank = other.rank;
    this.ldocid = other.ldocid;
    this.gdocid = other.gdocid;
    this.score = other.score;
    this.shard = other.shard;
    this.bucket = other.bucket;
  }

  public Result deepCopy() {
    return new Result(this);
  }

  @Override
  public void clear() {
    setQueryIsSet(false);
    this.query = 0;
    setRankIsSet(false);
    this.rank = 0;
    setLdocidIsSet(false);
    this.ldocid = 0;
    setGdocidIsSet(false);
    this.gdocid = 0;
    setScoreIsSet(false);
    this.score = 0.0;
    setShardIsSet(false);
    this.shard = 0;
    setBucketIsSet(false);
    this.bucket = 0;
  }

  public int getQuery() {
    return this.query;
  }

  public Result setQuery(int query) {
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

  public int getRank() {
    return this.rank;
  }

  public Result setRank(int rank) {
    this.rank = rank;
    setRankIsSet(true);
    return this;
  }

  public void unsetRank() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __RANK_ISSET_ID);
  }

  /** Returns true if field rank is set (has been assigned a value) and false otherwise */
  public boolean isSetRank() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __RANK_ISSET_ID);
  }

  public void setRankIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __RANK_ISSET_ID, value);
  }

  public long getLdocid() {
    return this.ldocid;
  }

  public Result setLdocid(long ldocid) {
    this.ldocid = ldocid;
    setLdocidIsSet(true);
    return this;
  }

  public void unsetLdocid() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __LDOCID_ISSET_ID);
  }

  /** Returns true if field ldocid is set (has been assigned a value) and false otherwise */
  public boolean isSetLdocid() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __LDOCID_ISSET_ID);
  }

  public void setLdocidIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __LDOCID_ISSET_ID, value);
  }

  public long getGdocid() {
    return this.gdocid;
  }

  public Result setGdocid(long gdocid) {
    this.gdocid = gdocid;
    setGdocidIsSet(true);
    return this;
  }

  public void unsetGdocid() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __GDOCID_ISSET_ID);
  }

  /** Returns true if field gdocid is set (has been assigned a value) and false otherwise */
  public boolean isSetGdocid() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __GDOCID_ISSET_ID);
  }

  public void setGdocidIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __GDOCID_ISSET_ID, value);
  }

  public double getScore() {
    return this.score;
  }

  public Result setScore(double score) {
    this.score = score;
    setScoreIsSet(true);
    return this;
  }

  public void unsetScore() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SCORE_ISSET_ID);
  }

  /** Returns true if field score is set (has been assigned a value) and false otherwise */
  public boolean isSetScore() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SCORE_ISSET_ID);
  }

  public void setScoreIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SCORE_ISSET_ID, value);
  }

  public int getShard() {
    return this.shard;
  }

  public Result setShard(int shard) {
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

  public int getBucket() {
    return this.bucket;
  }

  public Result setBucket(int bucket) {
    this.bucket = bucket;
    setBucketIsSet(true);
    return this;
  }

  public void unsetBucket() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __BUCKET_ISSET_ID);
  }

  /** Returns true if field bucket is set (has been assigned a value) and false otherwise */
  public boolean isSetBucket() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __BUCKET_ISSET_ID);
  }

  public void setBucketIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __BUCKET_ISSET_ID, value);
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

    case RANK:
      if (value == null) {
        unsetRank();
      } else {
        setRank((java.lang.Integer)value);
      }
      break;

    case LDOCID:
      if (value == null) {
        unsetLdocid();
      } else {
        setLdocid((java.lang.Long)value);
      }
      break;

    case GDOCID:
      if (value == null) {
        unsetGdocid();
      } else {
        setGdocid((java.lang.Long)value);
      }
      break;

    case SCORE:
      if (value == null) {
        unsetScore();
      } else {
        setScore((java.lang.Double)value);
      }
      break;

    case SHARD:
      if (value == null) {
        unsetShard();
      } else {
        setShard((java.lang.Integer)value);
      }
      break;

    case BUCKET:
      if (value == null) {
        unsetBucket();
      } else {
        setBucket((java.lang.Integer)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case QUERY:
      return getQuery();

    case RANK:
      return getRank();

    case LDOCID:
      return getLdocid();

    case GDOCID:
      return getGdocid();

    case SCORE:
      return getScore();

    case SHARD:
      return getShard();

    case BUCKET:
      return getBucket();

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
    case RANK:
      return isSetRank();
    case LDOCID:
      return isSetLdocid();
    case GDOCID:
      return isSetGdocid();
    case SCORE:
      return isSetScore();
    case SHARD:
      return isSetShard();
    case BUCKET:
      return isSetBucket();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof Result)
      return this.equals((Result)that);
    return false;
  }

  public boolean equals(Result that) {
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

    boolean this_present_rank = true;
    boolean that_present_rank = true;
    if (this_present_rank || that_present_rank) {
      if (!(this_present_rank && that_present_rank))
        return false;
      if (this.rank != that.rank)
        return false;
    }

    boolean this_present_ldocid = true && this.isSetLdocid();
    boolean that_present_ldocid = true && that.isSetLdocid();
    if (this_present_ldocid || that_present_ldocid) {
      if (!(this_present_ldocid && that_present_ldocid))
        return false;
      if (this.ldocid != that.ldocid)
        return false;
    }

    boolean this_present_gdocid = true && this.isSetGdocid();
    boolean that_present_gdocid = true && that.isSetGdocid();
    if (this_present_gdocid || that_present_gdocid) {
      if (!(this_present_gdocid && that_present_gdocid))
        return false;
      if (this.gdocid != that.gdocid)
        return false;
    }

    boolean this_present_score = true && this.isSetScore();
    boolean that_present_score = true && that.isSetScore();
    if (this_present_score || that_present_score) {
      if (!(this_present_score && that_present_score))
        return false;
      if (this.score != that.score)
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

    boolean this_present_bucket = true && this.isSetBucket();
    boolean that_present_bucket = true && that.isSetBucket();
    if (this_present_bucket || that_present_bucket) {
      if (!(this_present_bucket && that_present_bucket))
        return false;
      if (this.bucket != that.bucket)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + query;

    hashCode = hashCode * 8191 + rank;

    hashCode = hashCode * 8191 + ((isSetLdocid()) ? 131071 : 524287);
    if (isSetLdocid())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(ldocid);

    hashCode = hashCode * 8191 + ((isSetGdocid()) ? 131071 : 524287);
    if (isSetGdocid())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(gdocid);

    hashCode = hashCode * 8191 + ((isSetScore()) ? 131071 : 524287);
    if (isSetScore())
      hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(score);

    hashCode = hashCode * 8191 + ((isSetShard()) ? 131071 : 524287);
    if (isSetShard())
      hashCode = hashCode * 8191 + shard;

    hashCode = hashCode * 8191 + ((isSetBucket()) ? 131071 : 524287);
    if (isSetBucket())
      hashCode = hashCode * 8191 + bucket;

    return hashCode;
  }

  @Override
  public int compareTo(Result other) {
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
    lastComparison = java.lang.Boolean.valueOf(isSetRank()).compareTo(other.isSetRank());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRank()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.rank, other.rank);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetLdocid()).compareTo(other.isSetLdocid());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLdocid()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ldocid, other.ldocid);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetGdocid()).compareTo(other.isSetGdocid());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGdocid()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.gdocid, other.gdocid);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetScore()).compareTo(other.isSetScore());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetScore()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.score, other.score);
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
    lastComparison = java.lang.Boolean.valueOf(isSetBucket()).compareTo(other.isSetBucket());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBucket()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.bucket, other.bucket);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Result(");
    boolean first = true;

    sb.append("query:");
    sb.append(this.query);
    first = false;
    if (!first) sb.append(", ");
    sb.append("rank:");
    sb.append(this.rank);
    first = false;
    if (isSetLdocid()) {
      if (!first) sb.append(", ");
      sb.append("ldocid:");
      sb.append(this.ldocid);
      first = false;
    }
    if (isSetGdocid()) {
      if (!first) sb.append(", ");
      sb.append("gdocid:");
      sb.append(this.gdocid);
      first = false;
    }
    if (isSetScore()) {
      if (!first) sb.append(", ");
      sb.append("score:");
      sb.append(this.score);
      first = false;
    }
    if (isSetShard()) {
      if (!first) sb.append(", ");
      sb.append("shard:");
      sb.append(this.shard);
      first = false;
    }
    if (isSetBucket()) {
      if (!first) sb.append(", ");
      sb.append("bucket:");
      sb.append(this.bucket);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'query' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'rank' because it's a primitive and you chose the non-beans generator.
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

  private static class ResultStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ResultStandardScheme getScheme() {
      return new ResultStandardScheme();
    }
  }

  private static class ResultStandardScheme extends org.apache.thrift.scheme.StandardScheme<Result> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Result struct) throws org.apache.thrift.TException {
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
          case 2: // RANK
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.rank = iprot.readI32();
              struct.setRankIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LDOCID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.ldocid = iprot.readI64();
              struct.setLdocidIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // GDOCID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.gdocid = iprot.readI64();
              struct.setGdocidIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // SCORE
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.score = iprot.readDouble();
              struct.setScoreIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // SHARD
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.shard = iprot.readI32();
              struct.setShardIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // BUCKET
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.bucket = iprot.readI32();
              struct.setBucketIsSet(true);
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
      if (!struct.isSetRank()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'rank' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Result struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(QUERY_FIELD_DESC);
      oprot.writeI32(struct.query);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(RANK_FIELD_DESC);
      oprot.writeI32(struct.rank);
      oprot.writeFieldEnd();
      if (struct.isSetLdocid()) {
        oprot.writeFieldBegin(LDOCID_FIELD_DESC);
        oprot.writeI64(struct.ldocid);
        oprot.writeFieldEnd();
      }
      if (struct.isSetGdocid()) {
        oprot.writeFieldBegin(GDOCID_FIELD_DESC);
        oprot.writeI64(struct.gdocid);
        oprot.writeFieldEnd();
      }
      if (struct.isSetScore()) {
        oprot.writeFieldBegin(SCORE_FIELD_DESC);
        oprot.writeDouble(struct.score);
        oprot.writeFieldEnd();
      }
      if (struct.isSetShard()) {
        oprot.writeFieldBegin(SHARD_FIELD_DESC);
        oprot.writeI32(struct.shard);
        oprot.writeFieldEnd();
      }
      if (struct.isSetBucket()) {
        oprot.writeFieldBegin(BUCKET_FIELD_DESC);
        oprot.writeI32(struct.bucket);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ResultTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ResultTupleScheme getScheme() {
      return new ResultTupleScheme();
    }
  }

  private static class ResultTupleScheme extends org.apache.thrift.scheme.TupleScheme<Result> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Result struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.query);
      oprot.writeI32(struct.rank);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetLdocid()) {
        optionals.set(0);
      }
      if (struct.isSetGdocid()) {
        optionals.set(1);
      }
      if (struct.isSetScore()) {
        optionals.set(2);
      }
      if (struct.isSetShard()) {
        optionals.set(3);
      }
      if (struct.isSetBucket()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetLdocid()) {
        oprot.writeI64(struct.ldocid);
      }
      if (struct.isSetGdocid()) {
        oprot.writeI64(struct.gdocid);
      }
      if (struct.isSetScore()) {
        oprot.writeDouble(struct.score);
      }
      if (struct.isSetShard()) {
        oprot.writeI32(struct.shard);
      }
      if (struct.isSetBucket()) {
        oprot.writeI32(struct.bucket);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Result struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.query = iprot.readI32();
      struct.setQueryIsSet(true);
      struct.rank = iprot.readI32();
      struct.setRankIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.ldocid = iprot.readI64();
        struct.setLdocidIsSet(true);
      }
      if (incoming.get(1)) {
        struct.gdocid = iprot.readI64();
        struct.setGdocidIsSet(true);
      }
      if (incoming.get(2)) {
        struct.score = iprot.readDouble();
        struct.setScoreIsSet(true);
      }
      if (incoming.get(3)) {
        struct.shard = iprot.readI32();
        struct.setShardIsSet(true);
      }
      if (incoming.get(4)) {
        struct.bucket = iprot.readI32();
        struct.setBucketIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

