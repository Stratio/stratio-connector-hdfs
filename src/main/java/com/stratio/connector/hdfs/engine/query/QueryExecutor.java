package com.stratio.connector.hdfs.engine.query;

import java.util.List;

import com.stratio.connector.hdfs.connection.HDFSConnectionHandler;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.statements.structures.Operator;

public class QueryExecutor {

    private final HDFSConnectionHandler connectionHandler;

    public QueryExecutor(HDFSConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }


    /**
     * Validates the logical workflow and stores the needed steps.
     *
     *
     */
    private void execLogicalWorkflow(LogicalWorkflow logicalWorkFlow) throws UnsupportedException {
        List<LogicalStep> logicalSteps = logicalWorkFlow.getInitialSteps();

        for (LogicalStep currentStep : logicalSteps) {

            do {
                if (currentStep instanceof Project) {

                } else if (currentStep instanceof Filter) {

                    Filter step = (Filter) currentStep;
                    if (Operator.MATCH == step.getRelation().getOperator()) {

                    } else {

                    }
                } else if (currentStep instanceof Select) {


                } else if (currentStep instanceof Limit) {

                } else {
                    throw new UnsupportedException("Logical step [" + currentStep.getOperation().toString() + " not yet supported");
                }

                currentStep = currentStep.getNextStep();

            } while (currentStep != null);

        }
    }

}
