package org.apache.carbondata.acid.transaction;

// operation which need to be recorded for the transaction
public enum TransactionOperation {

  LOAD,

  INSERT,

  UPDATE,

  DELETE,

  MERGE,

  COMPACTION,

  SCHEMA_CHANGE,

  DROP_SEGMENT
}
