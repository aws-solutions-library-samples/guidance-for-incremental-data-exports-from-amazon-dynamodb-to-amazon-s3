import { aws_stepfunctions as sfn } from "aws-cdk-lib"
import { StepFunctionOutputConstants } from "./constants/stepFunctionOutputConstants";
import { KeywordConstants } from "./constants/keywordConstants";
import { parameterConstants } from "./constants/parameterConstants";

export class ConditionBuilder {
    executeFullExport: sfn.Condition;
    fullExportStillRunning: sfn.Condition;

    fullExportTimeParameterIsGreaterThanCond: sfn.Condition;
    fullExportTimeParameterIsLessThanCond: sfn.Condition;
    fullExportTimeParameterFallsOutsidePitrWindow: sfn.Condition;

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

        this.fullExportStillRunning = sfn.Condition.and(fullExportTimeParameterExist, workflowInitiatedParameterDoesExist, workflowInitiatedParameterIsEmpty);
        

        this.fullExportTimeParameterIsGreaterThanCond = 
            sfn.Condition.timestampGreaterThanJsonPath(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.FULL_EXPORT_TIME_PARAMETER}.value[0].Value`, 
                `$.${StepFunctionOutputConstants.DESCRIBE_CONTINUOUS_BACKUPS_OUTPUT}.ContinuousBackupsDescription.PointInTimeRecoveryDescription.LatestRestorableDateTime`);
        this.fullExportTimeParameterIsLessThanCond = 
            sfn.Condition.timestampLessThanJsonPath(`$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.FULL_EXPORT_TIME_PARAMETER}.value[0].Value`, 
                `$.${StepFunctionOutputConstants.DESCRIBE_CONTINUOUS_BACKUPS_OUTPUT}.ContinuousBackupsDescription.PointInTimeRecoveryDescription.EarliestRestorableDateTime`);
        this.fullExportTimeParameterFallsOutsidePitrWindow =
            sfn.Condition.or(this.fullExportTimeParameterIsGreaterThanCond, this.fullExportTimeParameterIsLessThanCond);

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