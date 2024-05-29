import { aws_stepfunctions as sfn } from "aws-cdk-lib"
import { StepFunctionOutputConstants } from "./constants/stepFunctionOutputConstants";
import { KeywordConstants } from "./constants/keywordConstants";
import { parameterConstants } from "./constants/parameterConstants";
import { WorkflowState } from "./constants/WorkflowState";
import { WorkflowAction } from "./constants/WorkflowAction";

export class ConditionBuilder {
    executeFullExport: sfn.Condition;
    fullExportStillRunning: sfn.Condition;
    pitrGapWorkflowState: sfn.Condition;
    resetWithFullExportAgain: sfn.Condition;
    isWorkflowPaused: sfn.Condition;

    earliestRestoreDateTimeIsGreaterThanExportStartTime: sfn.Condition;

    lastIncrementalExportTimeParameterExist: sfn.Condition;
    lastIncrementalExportTimeParameterIsTrue: sfn.Condition;
    lastIncrementalExportTimeIsValid: sfn.Condition;

    pitrIsEnabled: sfn.Condition;

    workflowInitializedParameterIsFalse: sfn.Condition;
    incrementalExportStateIsFalse: sfn.Condition;
    incrementalExportCompleted: sfn.Condition;
    incrementalExportFailed: sfn.Condition;
    fullExportCompleted: sfn.Condition;
    fullExportFailed: sfn.Condition;

    workflowInitializedParameterOutputFalse: sfn.Condition;

    nextIncrementalExportEndTimeIsPastCurrentTime: sfn.Condition;

    constructor() {
        this.buildConditions();
    }
    
    private buildConditions() {

        const fullExportTimeParameterExist = sfn.Condition.numberEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.FULL_EXPORT_TIME_PARAMETER}.valueCount`, 1);
        const fullExportTimeParameterDoesNotExist = sfn.Condition.not(fullExportTimeParameterExist);
        
        const workflowInitiatedParameterDoesExist = sfn.Condition.numberEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_INITATED_PARAMETER}.valueCount`, 1);
        const workflowInitiatedParameterDoesNotExist = sfn.Condition.not(workflowInitiatedParameterDoesExist);
        const workflowInitiatedParameterIsFalse = 
            sfn.Condition.stringEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_INITATED_PARAMETER}.value[0].Value`, KeywordConstants.FALSE_STRING);
        const workflowInitiatedParameterIsEmpty = 
            sfn.Condition.stringEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_INITATED_PARAMETER}.value[0].Value`, KeywordConstants.NULL_STRING);

        // a full export might take longer to execute and therefore only the full export time should be checked to ensure workflow has been initiated
        this.executeFullExport = sfn.Condition
            .and(fullExportTimeParameterDoesNotExist, sfn.Condition.or(workflowInitiatedParameterDoesNotExist, workflowInitiatedParameterIsFalse));        
        
        const workflowActionParameterDoesExist = sfn.Condition.numberEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_ACTION_PARAMETER}.valueCount`, 1);
        const workflowActionParameterIsPause = sfn.Condition.stringEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_ACTION_PARAMETER}.value[0].Value`, WorkflowAction[WorkflowAction.PAUSE]);
        const workflowActionParameterIsResetWithFullExportAgain = sfn.Condition.stringEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_ACTION_PARAMETER}.value[0].Value`, WorkflowAction[WorkflowAction.RESET_WITH_FULL_EXPORT_AGAIN]);
        this.isWorkflowPaused = sfn.Condition.and(workflowActionParameterDoesExist, workflowActionParameterIsPause);
        this.resetWithFullExportAgain = sfn.Condition.and(workflowActionParameterDoesExist, workflowActionParameterIsResetWithFullExportAgain);

        this.fullExportStillRunning = sfn.Condition.and(fullExportTimeParameterExist, workflowInitiatedParameterDoesExist, workflowInitiatedParameterIsEmpty);

        const workflowStateParameterDoesExist = sfn.Condition.numberEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_STATE_PARAMETER}.valueCount`, 1);
        const workflowStateParameterIsPitrGap = sfn.Condition.stringEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.WORKFLOW_STATE_PARAMETER}.value[0].Value`, WorkflowState[WorkflowState.PITR_GAP]);
        this.pitrGapWorkflowState = sfn.Condition.and(workflowStateParameterDoesExist, workflowStateParameterIsPitrGap);
        
        this.earliestRestoreDateTimeIsGreaterThanExportStartTime = 
            sfn.Condition.timestampGreaterThanJsonPath(
                `$.${StepFunctionOutputConstants.DESCRIBE_CONTINUOUS_BACKUPS_OUTPUT}.ContinuousBackupsDescription.PointInTimeRecoveryDescription.EarliestRestorableDateTime`,
                `$.${StepFunctionOutputConstants.EXPORT_TIME_PARAMETER_TO_USE_OUTPUT}.exportTimeParameterToUse`);

        this.lastIncrementalExportTimeParameterExist = sfn.Condition.numberEquals(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.LAST_INCREMENTAL_EXPORT_TIME_PARAMETER}.valueCount`, 1);
        this.lastIncrementalExportTimeParameterIsTrue = sfn.Condition.isTimestamp(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.LAST_INCREMENTAL_EXPORT_TIME_PARAMETER}.value[0].Value`);
        this.lastIncrementalExportTimeIsValid = sfn.Condition.and(
            this.lastIncrementalExportTimeParameterExist, 
            this.lastIncrementalExportTimeParameterIsTrue);

        this.pitrIsEnabled = 
            sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.DESCRIBE_CONTINUOUS_BACKUPS_OUTPUT}.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus`, KeywordConstants.ENABLED);

        this.workflowInitializedParameterIsFalse = 
            sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.WORKFLOW_INITIALIZED_PARAMETER_OUTPUT}.success`, KeywordConstants.FALSE_STRING);
        this.incrementalExportStateIsFalse = 
        sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.INCREMENTAL_EXPORT_PARAMETER_OUTPUT}.success`, KeywordConstants.FALSE_STRING);

        this.incrementalExportCompleted = 
            sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.ExportStatus`, KeywordConstants.COMPLETED);
        this.incrementalExportFailed = 
            sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.ExportStatus`, KeywordConstants.FAILED);

        this.fullExportCompleted = 
            sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}.ExportDescription.ExportStatus`, KeywordConstants.COMPLETED);
        this.fullExportFailed = 
            sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}.ExportDescription.ExportStatus`, KeywordConstants.FAILED);

        this.workflowInitializedParameterOutputFalse = 
            sfn.Condition.stringEquals(`$.${StepFunctionOutputConstants.WORKFLOW_INITIALIZED_PARAMETER_OUTPUT}.success`, KeywordConstants.FALSE_STRING)

        this.nextIncrementalExportEndTimeIsPastCurrentTime = sfn.Condition.timestampGreaterThanJsonPath(`$.${StepFunctionOutputConstants.GET_NEXT_INCREMENTAL_EXPORT_TIME_OUTPUT}.Payload.body.durationAddedStartTime`, '$$.State.EnteredTime');
    }
}