namespace java edu.nyu.tandon.experiments.thrift

struct QueryFeatures {
    1: required i32 query;
    2: optional i64 time;
    3: optional i64 maxlist1;
    4: optional i64 maxlist2;
    5: optional i64 minlist1;
    6: optional i64 minlist2;
    7: optional i64 sumlist;
    8: optional i32 shard;
}
