package io.appform.dropwizard.sharding.dao.operations;

public enum OpType {

  // Read operations
  COUNT,
  SELECT,
  GET_BY_LOOKUP_KEY, // lookup dao specific get
  GET,
  READ_ONLY,


  // Write operations
  LOCK_AND_EXECUTE, // operations performed within locked context
  UPDATE_BY_QUERY,
  UPDATE,
  UPDATE_WITH_SCROLL,
  GET_AND_UPDATE,
  SELECT_AND_UPDATE,
  RUN,
  DELETE_BY_LOOKUP_KEY,
  SAVE_ALL,
  SAVE,
  CREATE_OR_UPDATE,
}
