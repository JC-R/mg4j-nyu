namespace java edu.nyu.tandon.experiments.thrift

struct Posting {
    1: required i32 termid;
    2: optional i64 docid;
    3: optional double score;
}