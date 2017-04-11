namespace java edu.nyu.tandon.experiments.thrift

struct Result {
    1: required i32 query;
    2: optional list<i64> docids_local;
    3: optional list<i64> docids_global;
    4: optional list<double> scores;
    5: optional i32 shard;
    6: optional i32 bucket;
}