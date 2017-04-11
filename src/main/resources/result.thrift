namespace java edu.nyu.tandon.experiments.thrift

struct Result {
    1: required i32 query;
    2: required i32 rank;
    3: optional i64 ldocid;
    4: optional i64 gdocid;
    5: optional double score;
    6: optional i32 shard;
    7: optional i32 bucket;
}