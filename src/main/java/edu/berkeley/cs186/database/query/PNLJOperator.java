package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;

class PNLJOperator extends BNLJOperator {
    PNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction);

        joinType = JoinType.PNLJ;
        numBuffers = 3;
    }
}
