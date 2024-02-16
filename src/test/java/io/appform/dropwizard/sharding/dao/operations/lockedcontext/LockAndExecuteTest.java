package io.appform.dropwizard.sharding.dao.operations.lockedcontext;

import static org.junit.jupiter.api.Assertions.*;
import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.testdata.entities.Order;

public class LockAndExecuteTest {

  public LockAndExecute<Order> lockAndExecute;

  public void testInsertMode() {

    lockAndExecute = LockAndExecute.<Order>buildForInsert().build();
//    lockAndExecute.apply()
  }

}