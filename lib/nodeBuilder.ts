import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { aws_dynamodb as ddb, 
    aws_s3 as s3, 
    aws_stepfunctions as sfn, 
    aws_stepfunctions_tasks as tasks, 
    aws_lambda as lambda, 
    aws_sns as sns, 
    aws_iam as iam } from "aws-cdk-lib";
import { DataExportBucket } from './constructs/dataExportBucket';
import { StepFunctionOutputConstants } from './constants/stepFunctionOutputConstants';
import { parameterConstants } from './constants/parameterConstants';
import { SsmParameterConstants } from './constants/ssmParameterConstants';
import { KeywordConstants } from "./constants/keywordConstants";
import { ExportType } from "./constants/exportType";
import { ExportViewType } from "./constants/exportViewType";
import { Configuration } from './configuration';
import { IncrementalExportDefaults } from './constants/incrementalExportDefaults';
import { WorkflowState } from './constants/workflowState';
import { WorkflowAction } from './constants/workflowAction';

export class NodeBuilder {

    private scope: Construct;
    private sourceDynamoDbTable: ddb.ITable;
    private snsTopic: sns.Topic;
    private incrementalExportTimeManipulatorFunction: lambda.Function;
    private sourceDataExportBucket: DataExportBucket;

    private parameterPathPrefix: string;
    private workflowInitiatedParameterName: string;
    private fullExportTimeParameterName: string;
    private lastIncrementalExportTimeParameterName: string;
    private workflowStateParameterName: string;
    private workflowActionParameterName: string;

    private incrementalExportWindowSizeInMinutes: number;
    private waitTimeToCheckExportStatusInSeconds: number;
    private exportViewType: ExportViewType;
    private awsApiInvocationTaskTimeout: sfn.Timeout;

    // Pass nodes
    cleanParametersPassState: sfn.Pass;
    useFullExportTimeParameterValue: sfn.Pass;
    useLastIncrementalExportTimeParameterValue: sfn.Pass;
    incrementalExportParameterTrue: sfn.Pass;
    incrementalExportParameterFalse: sfn.Pass;
    workflowInitializedParameterTrue: sfn.Pass;
    workflowInitializedParameterFalse: sfn.Pass;

    // Choice nodes
    isLastIncrementalExportParameterValid: sfn.Choice;
    incrementalExportCompletedState: sfn.Choice;
    didIncrementalExportCompleteSuccessfully: sfn.Choice;
    fullExportCompletedState: sfn.Choice;
    pitrEnabledChoice: sfn.Choice;
    isEarliestRestoreDateTimeValidChoice: sfn.Choice;
    initializeWorkflowState: sfn.Choice;
    didWorkflowInitiateSuccessfully: sfn.Choice;
    checkIncrementalExportNeeded: sfn.Choice
    checkWorkflowAction: sfn.Choice

    // AWSService nodes
    ensureTableExistsTask: tasks.CallAwsService;
    getParametersTask: tasks.CallAwsService;
    getNextIncrementalExportTimeLambdaInvoke: tasks.LambdaInvoke;
    setLastIncrementalExportTimeParameter: tasks.CallAwsService;
    describeIncrementalExport: tasks.CallAwsService;
    executeFullExport: tasks.CallAwsService;
    setFullExportTimeParameter: tasks.CallAwsService;
    describeContinuousBackupsAwsServiceTask: tasks.CallAwsService;
    describeFullExport: tasks.CallAwsService;
    executeIncrementalExport: tasks.CallAwsService;
    setWorkflowInitiatedParameter: tasks.CallAwsService;
    setEmptyWorkflowInitiatedParameter: tasks.CallAwsService;
    deleteLastIncrementalExportTimeParameter: tasks.CallAwsService;
    setWorkflowStateParameterToPitrGap: tasks.CallAwsService;
    setWorkflowActionParameterToRun: tasks.CallAwsService;
    setWorkflowStateParameterToNormal: tasks.CallAwsService;
    
    // Sns public nodes
    notifyOnIncrementalExportStartTimeOutsidePitrWindow: tasks.SnsPublish;
    notifyOnIncrementalExport: tasks.SnsPublish;
    notifyOnFullExport: tasks.SnsPublish;
    notifyOnFullExportRunning: tasks.SnsPublish;
    notifyOnPitrDisabled: tasks.SnsPublish;
    notifyOnTaskFailed: tasks.SnsPublish;
    notifyOnPitrGap: tasks.SnsPublish;
    
    // Success/Fail nodes
    taskFailedNode: sfn.Fail;
    incrementalExportStartTimeOutsidePitrWindowFail: sfn.Fail;
    pitrDisabledFail: sfn.Fail;
    incrementalExportSucceeded: sfn.Succeed;
    incrementalExportFailed: sfn.Fail;
    fullExportFailed: sfn.Fail;
    fullExportSucceeded: sfn.Succeed;
    fullExportStillRunning: sfn.Succeed;
    incrementalExportNotNeeded: sfn.Succeed;
    pitrGapFound: sfn.Fail;
    workflowPaused: sfn.Succeed;
    
    // Wait nodes
    waitForIncrementalExport: sfn.Wait;
    waitForFullExport: sfn.Wait;

    constructor(scope: Construct, sourceDynamoDbTable: ddb.ITable, sourceDataExportBucket: DataExportBucket, snsTopic: sns.Topic, incrementalExportTimeManipulatorFunction: lambda.Function, configuration: Configuration) {

        this.scope = scope;
        this.sourceDynamoDbTable = sourceDynamoDbTable;
        this.sourceDataExportBucket = sourceDataExportBucket;
        this.snsTopic = snsTopic;
        this.incrementalExportTimeManipulatorFunction = incrementalExportTimeManipulatorFunction;

        this.incrementalExportWindowSizeInMinutes = configuration.incrementalExportWindowSizeInMinutes;
        this.waitTimeToCheckExportStatusInSeconds = configuration.waitTimeToCheckExportStatusInSeconds;
        this.exportViewType = configuration.exportViewType;

        this.parameterPathPrefix = `${SsmParameterConstants.PARAMETER_PATH}/${configuration.deploymentAlias}`;    
        this.workflowInitiatedParameterName = `${this.parameterPathPrefix}/${SsmParameterConstants.WORKFLOW_INITIATED_PARAMETER_NAME}`;
        this.fullExportTimeParameterName = `${this.parameterPathPrefix}/${SsmParameterConstants.FULL_EXPORT_TIME_PARAMETER_NAME}`;
        this.lastIncrementalExportTimeParameterName = `${this.parameterPathPrefix}/${SsmParameterConstants.LAST_INCREMENTAL_EXPORT_TIME_PARAMETER_NAME}`;
        this.workflowStateParameterName = `${this.parameterPathPrefix}/${SsmParameterConstants.WORKFLOW_STATE_PARAMETER_NAME}`;
        this.workflowActionParameterName = `${this.parameterPathPrefix}/${SsmParameterConstants.WORKFLOW_ACTION_PARAMETER_NAME}`;

        this.awsApiInvocationTaskTimeout = sfn.Timeout.duration(cdk.Duration.seconds(configuration.awsApiInvocationTimeoutInSeconds));

        this.buildNodes();
    }

    private buildNodes() {

        this.taskFailedNode = new sfn.Fail(this.scope, 'table-does-not-exist', 
        {
            comment: 'Task failed',
            stateName: 'TaskFailed'
        });
        
        this.ensureTableExistsTask = this.getDynamoDbTask(
            'ddb-describe-table', 
            'describeTable', 
            'EnsureTableExists', 
            'Ensures that the DynamoDB table exists on each execution', 
            `$.${StepFunctionOutputConstants.TABLE_INFO}`, 
            { 'TableName': this.sourceDynamoDbTable.tableName }
          );

        this.getParametersTask = new tasks.CallAwsService(this.scope, 'get-parameters-by-path', {
            service: 'ssm',
            action: 'getParametersByPath',
            stateName: 'GetParametersByPath',
            comment: 'Get parameters from the store',
            iamResources: [this.getParameterStoreParameterArn(this.parameterPathPrefix, cdk.ArnFormat.SLASH_RESOURCE_NAME)],
            parameters: {
              Path: `/${this.parameterPathPrefix}`,
              Recursive: true
            },
            resultPath: `$.${StepFunctionOutputConstants.PARAMETER_VALUES}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
          });

        this.cleanParametersPassState = new sfn.Pass(this.scope, 'clean-parameters', {
            comment: 'Clean output',
            stateName: 'CleanOutput',
            parameters: {
                parameterInfo: {
                    workflowInitiatedParameter: {
                        parameterName: `/${this.workflowInitiatedParameterName}`,
                        'valueCount.$': `States.ArrayLength($.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.workflowInitiatedParameterName})])`,
                        'value.$': `$.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.workflowInitiatedParameterName})]`
                    },
                    fullExportTimeParameter: {
                        parameterName: this.fullExportTimeParameterName,
                        'valueCount.$': `States.ArrayLength($.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.fullExportTimeParameterName})])`,
                        'value.$': `$.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.fullExportTimeParameterName})]`
                    },
                    lastIncrementalExportTimeParameter: {
                        parameterName: this.lastIncrementalExportTimeParameterName,
                        'valueCount.$': `States.ArrayLength($.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.lastIncrementalExportTimeParameterName})])`,
                        'value.$': `$.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.lastIncrementalExportTimeParameterName})]`
                    },
                    workflowStateParameter: {
                        parameterName: this.workflowStateParameterName,
                        'valueCount.$': `States.ArrayLength($.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.workflowStateParameterName})])`,
                        'value.$': `$.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.workflowStateParameterName})]`
                    },
                    workflowActionParameter: {
                        parameterName: this.workflowActionParameterName,
                        'valueCount.$': `States.ArrayLength($.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.workflowActionParameterName})])`,
                        'value.$': `$.${StepFunctionOutputConstants.PARAMETER_VALUES}.Parameters[?(@.Name == /${this.workflowActionParameterName})]`
                    }
                }
            }
        });

        this.notifyOnIncrementalExportStartTimeOutsidePitrWindow = this.getNotifyOnIncrementalExportStartTimeOutsidePitrWindowSnsPublishTask(
            'notify-on-incremental-export-start-time-outside-pitr-window',
            'NotifyOnIncrementalExportStartTimeOutsidePITRWindow',
            `$.${StepFunctionOutputConstants.NOTIFY_ON_INCREMENTAL_EXPORT_START_TIME_OUTSIDE_PITR_WINDOW_OUTPUT}`);

        this.incrementalExportStartTimeOutsidePitrWindowFail = new sfn.Fail(this.scope, 'incremental-export-start-time-outside-pitr-window-fail', {
            stateName: 'IncrementalExportStartTimeOutsidePITRWindow',
            error: 'IncrementalExportStartTimeOutsidePITRWindow',
            cause: 'PITR might be disabled and enabled invalidating the last full table export'
        });

        this.pitrDisabledFail = new sfn.Fail(this.scope, 'pitr-disabled-fail', { 
            stateName: 'PITRDisabled',
            comment: 'PITR is disabled, cannot proceed', 
            error: 'PITRDisabled', 
            cause: 'PITR needs to be enabled on the table for full/incremental exports to work' 
        });

        const pitrDisabledSnsMessage = {
            message: `PITR for table ${this.sourceDynamoDbTable.tableName} is disabled`,
            'executionId.$': '$$.Execution.Id',
            exportType: ExportType[ExportType.FULL_EXPORT],
            status: KeywordConstants.SNS_FAILED,
            remedy: `Ensure PITR is enabled by following https://aws.amazon.com/dynamodb/pitr/`
        };
    
        this.notifyOnPitrDisabled = this.getSnsPublishTask(
            'notify-on-pitr-disabled', 
            'NotifyOnPITRDisabled', 
            'PITR Disabled', 
            sfn.TaskInput.fromObject(pitrDisabledSnsMessage), 
            'PITR disabled', 
            `$.${StepFunctionOutputConstants.NOTIFY_ON_PITR_DISABLED_OUTPUT}`);
        
        this.isLastIncrementalExportParameterValid = new sfn.Choice(this.scope, 'is-last-incremental-export-time-parameter-valid', {
            stateName: 'IsLastIncrementalExportTimeParameterValid?',
            comment: 'Check if the last incremental export time parameter is in a valid state'
        });

        this.useFullExportTimeParameterValue = new sfn.Pass(this.scope, 'use-full-export-time-parameter', {
            stateName: 'UseFullExportTimeParameter',
            parameters: {
                'exportTimeParameterToUse.$': `$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.FULL_EXPORT_TIME_PARAMETER}.value[0].Value`
            },
            resultPath: `$.${StepFunctionOutputConstants.EXPORT_TIME_PARAMETER_TO_USE_OUTPUT}`
        });

        this.useLastIncrementalExportTimeParameterValue = new sfn.Pass(this.scope, 'use-incremental-export-time-parameter', {
            stateName: 'UseIncrementalExportTimeParameter',
            parameters: {
                'exportTimeParameterToUse.$': `$.${parameterConstants.PARAMETER_INFO}.${parameterConstants.LAST_INCREMENTAL_EXPORT_TIME_PARAMETER}.value[0].Value`
            },
            resultPath: `$.${StepFunctionOutputConstants.EXPORT_TIME_PARAMETER_TO_USE_OUTPUT}`
        });

        this.getNextIncrementalExportTimeLambdaInvoke = new tasks.LambdaInvoke(this.scope, 'get-next-incremental-export-time-lambda-invoke', {
            stateName: 'GetNextIncrementalExportTime',
            comment: 'Gets the next window for the incremental export based on the previous exported data plus the duration',
            lambdaFunction: this.incrementalExportTimeManipulatorFunction,
            payload: sfn.TaskInput.fromObject(
              {
                'executionId.$': '$$.Execution.Id',
                'lastExportTime.$': `$.${StepFunctionOutputConstants.EXPORT_TIME_PARAMETER_TO_USE_OUTPUT}.exportTimeParameterToUse`,
                'incrementalExportWindowSizeInMinutes': this.incrementalExportWindowSizeInMinutes,
                'tableArn': this.sourceDynamoDbTable.tableArn,
                'bucketOwner': this.sourceDataExportBucket.bucketOwnerAccountId,
                'bucket': this.sourceDataExportBucket.bucket,
                'bucketPrefix': this.sourceDataExportBucket.prefix,
                'exportFormat': IncrementalExportDefaults.DATA_EXPORT_FORMAT,
                'exportViewType': ExportViewType[this.exportViewType]
              }),
            resultPath: `$.${StepFunctionOutputConstants.GET_NEXT_INCREMENTAL_EXPORT_TIME_OUTPUT}`
          });
          
        const lastIncrementalExportTimeParameterIamArn = this.getParameterStoreParameterArn(this.lastIncrementalExportTimeParameterName, cdk.ArnFormat.SLASH_RESOURCE_NAME);
        this.setLastIncrementalExportTimeParameter = new tasks.CallAwsService(this.scope, 'set-last-incremental-time-parameter', {
            service: 'ssm',
            action: 'putParameter',
            stateName: 'SetLastIncrementalExportTimeParameter',
            comment: 'Incremental export end time',
            iamResources: [lastIncrementalExportTimeParameterIamArn],
            parameters: {
                Name: `/${this.lastIncrementalExportTimeParameterName}`,
                Description: `Incremental Export table ${this.sourceDynamoDbTable.tableName}`,
                'Value.$': `$.${StepFunctionOutputConstants.INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.IncrementalExportSpecification.ExportToTime`,
                'Overwrite': true,
                'Type': 'String'
            },
            resultPath: `$.${StepFunctionOutputConstants.PUT_INCREMENTAL_EXPORT_TIME_PARAMETER_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });

        this.describeIncrementalExport = this.getDescribeExportNode(
            'describe-incremental-export', 
            'DescribeIncrementalExport', 
            `$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}`, 
            { 
                'ExportArn.$': `$.${StepFunctionOutputConstants.INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.ExportArn`
            }
        );

        this.incrementalExportCompletedState = this.getExportCompletedNode('incremental-export-completed', 'IncrementalExportCompleted?');

        const incrementExportPassNodes = this.getExportPassNodes(
            'incremental-export', ExportType.INCREMENTAL_EXPORT, 
            'IncrementalExportState', 
            `$.${StepFunctionOutputConstants.INCREMENTAL_EXPORT_PARAMETER_OUTPUT}`, 
            StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT,
            `$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.IncrementalExportSpecification.ExportFromTime`,
            `$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.IncrementalExportSpecification.ExportToTime`,
            `$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.StartTime`,
            `$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.EndTime`,
            { 'incrementalBlocksBehind.$': `$.${StepFunctionOutputConstants.GET_NEXT_INCREMENTAL_EXPORT_TIME_OUTPUT}.Payload.body.incrementalBlocksBehind` },
            {'remedy.$': `$.${StepFunctionOutputConstants.GET_NEXT_INCREMENTAL_EXPORT_TIME_OUTPUT}.Payload.body.remedy` },
            `$.${StepFunctionOutputConstants.DESCRIBE_INCREMENTAL_EXPORT_OUTPUT}.ExportDescription.ExportArn`,
        );
        this.incrementalExportParameterTrue = incrementExportPassNodes.success;
        this.incrementalExportParameterFalse = incrementExportPassNodes.failed;

        this.notifyOnIncrementalExport = this.getSnsPublishTask(
            'notify-on-incremental-export', 
            'NotifyOnIncrementalExport', 
            'Incremental export outcome', 
            sfn.TaskInput.fromJsonPathAt(`$.${StepFunctionOutputConstants.INCREMENTAL_EXPORT_PARAMETER_OUTPUT}.snsMessage`), 
            'Notify on incremental export outcome', 
            `$.${StepFunctionOutputConstants.NOTIFY_ON_INCREMENTAL_EXPORT_OUTPUT}`
        );

        this.didIncrementalExportCompleteSuccessfully = new sfn.Choice(this.scope, 'did-incremental-export-complete-successfully', {
            comment: 'Did incremental export complete successfully?',
            stateName: 'DidIncrementalExportCompleteSuccessfully?'
        });

        // Refer to: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport_Requesting.html
        const exportToS3Path = `${this.sourceDataExportBucket.bucket}/${this.sourceDataExportBucket.prefix}/*`.replace(/\/\//g, '/');
        const exportToS3IamPolicyStatement = new iam.PolicyStatement({
            actions: ['s3:AbortMultipartUpload', 's3:PutObject', 's3:PutObjectAcl'],
            resources: [ `arn:aws:s3:::${exportToS3Path}`],
            effect: iam.Effect.ALLOW,
            sid: 'AllowWriteToDestinationBucket'
        });
        const exportParametersBase = this.sourceDataExportBucket.getExecuteExportParameters(this.sourceDynamoDbTable.tableArn);

        const executeFullExportParameters = {
            ...exportParametersBase,
            S3BucketOwner: this.sourceDataExportBucket.bucketOwnerAccountId,
            ExportType: ExportType[ExportType.FULL_EXPORT]
        }
        this.executeFullExport = this.getDynamoDbTask(
            'exec-full-export', 
            'exportTableToPointInTime', 
            'ExecuteFullExport', 
            'Initial full export of the table', 
            `$.${StepFunctionOutputConstants.FULL_EXPORT_OUTPUT}`, 
            executeFullExportParameters, 
            [exportToS3IamPolicyStatement]
        );
  
        const fullExportTimeParameterIamArn = this.getParameterStoreParameterArn(this.fullExportTimeParameterName, cdk.ArnFormat.SLASH_RESOURCE_NAME);
        this.setFullExportTimeParameter = new tasks.CallAwsService(this.scope, 'set-full-export-time-parameter', {
            service: 'ssm',
            action: 'putParameter',
            stateName: 'SetFullExportTimeParameter',
            comment: 'Full export end time',
            iamResources: [fullExportTimeParameterIamArn],
            parameters: {
                Name: `/${this.fullExportTimeParameterName}`,
                Description: `Incremental Export table ${this.sourceDynamoDbTable.tableName}`,
                'Value.$': `$.${StepFunctionOutputConstants.FULL_EXPORT_OUTPUT}.ExportDescription.ExportTime`,
                'Overwrite': true,
                'Type': 'String'
            },
            resultPath: `$.${StepFunctionOutputConstants.PUT_FULL_EXPORT_TIME_PARAMETER_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });

        this.deleteLastIncrementalExportTimeParameter = new tasks.CallAwsService(this.scope, 'delete-last-incremental-export-time-parameter', {
            service: 'ssm',
            action: 'deleteParameter',
            stateName: 'DeleteLastIncrementalExportTimeParameter',
            comment: 'Delete last incremental export time if set earlier',
            iamResources: [lastIncrementalExportTimeParameterIamArn],
            parameters: {
                Name: `/${this.lastIncrementalExportTimeParameterName}`
            },
            resultPath: `$.${StepFunctionOutputConstants.DELETE_LAST_INCREMENTAL_EXPORT_TIME_PARAMETER_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });

        this.fullExportCompletedState = this.getExportCompletedNode('full-export-completed', 'FullExportCompleted?');
        this.describeFullExport = this.getDescribeExportNode(
            'describe-full-export', 
            'DescribeFullExport', 
            `$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}`, 
            { 
                'ExportArn.$': `$.${StepFunctionOutputConstants.FULL_EXPORT_OUTPUT}.ExportDescription.ExportArn`
            }
        );

        this.waitForIncrementalExport = this.getWaitForExportNode('wait-for-incremental-export', 'WaitForIncrementalExport');
        this.waitForFullExport = this.getWaitForExportNode('wait-for-full-export', 'WaitForFullExport');
        
        const exportPassNodes = this.getExportPassNodes(
            'workflow-initialized-parameter', 
            ExportType.FULL_EXPORT, 
            'WorkflowInitializedParameter', 
            `$.${StepFunctionOutputConstants.WORKFLOW_INITIALIZED_PARAMETER_OUTPUT}`,
            StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT,
            `$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}.ExportDescription.ExportTime`,
            `$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}.ExportDescription.ExportTime`,
            `$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}.ExportDescription.StartTime`,
            `$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}.ExportDescription.EndTime`, 
            undefined,
            {'remedy:': 'Workflow will be triggered again'},
            `$.${StepFunctionOutputConstants.DESCRIBE_FULL_EXPORT_OUTPUT}.ExportDescription.ExportArn`,
        );
        this.workflowInitializedParameterTrue = exportPassNodes.success;
        this.workflowInitializedParameterFalse = exportPassNodes.failed;

        this.pitrEnabledChoice = new sfn.Choice(this.scope, 'pitr-enabled-choice', { 
            stateName: 'PITREnabled?', 
            comment: 'Check if PITR is enabled' 
        });

        this.describeContinuousBackupsAwsServiceTask = this.getDynamoDbTask(
            'ensure-pitr-enabled', 
            'describeContinuousBackups', 
            'DescribeContinuousBackups', 
            'Check if PITR is enabled on the table', 
            `$.${StepFunctionOutputConstants.DESCRIBE_CONTINUOUS_BACKUPS_OUTPUT}`, 
            { TableName: this.sourceDynamoDbTable.tableName }
        );

        this.isEarliestRestoreDateTimeValidChoice = new sfn.Choice(this.scope, 'is-earliest-restore-datetime-valid-choice', {
            stateName: 'IsEarliestRestoreDateTimeValid?',
            comment: 'Ensure the PITR earliest restore date time is less than the start time of the incremental'
        });

        this.incrementalExportSucceeded = new sfn.Succeed(this.scope, 'incremental-export-succeeded', { 
            comment: 'Incremental export succeeded', 
            stateName: 'IncrementalExportSucceeded' 
        });
        this.incrementalExportFailed = new sfn.Fail(this.scope, 'incremental-export-failed', { 
            comment: 'Incremental export failed', 
            stateName: 'IncrementalExportFailed' 
        });

        const executeIncrementalExportParameters = {
            ...exportParametersBase,
            S3BucketOwner: this.sourceDataExportBucket.bucketOwnerAccountId,
            ExportFormat: IncrementalExportDefaults.DATA_EXPORT_FORMAT,
            ExportType: ExportType[ExportType.INCREMENTAL_EXPORT],
            'IncrementalExportSpecification': {
                'ExportFromTime.$': `$.${StepFunctionOutputConstants.GET_NEXT_INCREMENTAL_EXPORT_TIME_OUTPUT}.Payload.body.lastExportTime`,
                'ExportToTime.$': `$.${StepFunctionOutputConstants.GET_NEXT_INCREMENTAL_EXPORT_TIME_OUTPUT}.Payload.body.durationAddedStartTime`,
                'ExportViewType': `${ExportViewType[this.exportViewType]}`
            }
        };
        this.executeIncrementalExport = this.getDynamoDbTask(
            'exec-incr-export', 
            'exportTableToPointInTime', 
            'ExecuteIncrementalExport', 
            'Initial full export of the table', 
            `$.${StepFunctionOutputConstants.INCREMENTAL_EXPORT_OUTPUT}`, 
            executeIncrementalExportParameters
        );

        this.initializeWorkflowState = new sfn.Choice(
            this.scope, 
            'initialize-workflow', 
            { comment: 'Initialize workflow', stateName: 'InitializeWorkflow?' }
        );

        const workflowInitiatedParameterArn = this.getParameterStoreParameterArn(this.workflowInitiatedParameterName, cdk.ArnFormat.SLASH_RESOURCE_NAME);
        this.setWorkflowInitiatedParameter = new tasks.CallAwsService(this.scope, 'set-workflow-initiated-parameter', {
            service: 'ssm',
            action: 'putParameter',
            stateName: 'SetWorkflowInitiatedParameter',
            comment: 'Update the workflow initiated parameter',
            iamResources: [workflowInitiatedParameterArn],
            parameters: {
                Name: `/${this.workflowInitiatedParameterName}`,
                Description: `Incremental Export table ${this.sourceDynamoDbTable.tableName}`,
                'Value.$': `$.${StepFunctionOutputConstants.WORKFLOW_INITIALIZED_PARAMETER_OUTPUT}.success`,
                'Overwrite': true,
                'Type': 'String'
            },
            resultPath: `$.${StepFunctionOutputConstants.PUT_WORKFLOW_INITIATED_PARAMETER_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });
        
        const workflowStateParameterArn = this.getParameterStoreParameterArn(this.workflowStateParameterName, cdk.ArnFormat.SLASH_RESOURCE_NAME);
        this.setWorkflowStateParameterToPitrGap = new tasks.CallAwsService(this.scope, 'set-workflow-state-parameter-to-pitr', {
            service: 'ssm',
            action: 'putParameter',
            stateName: 'SetWorkflowStateParameterToPitrGap',
            comment: 'Update the workflow state parameter to PITR gap',
            iamResources: [workflowStateParameterArn],
            parameters: {
                Name: `/${this.workflowStateParameterName}`,
                Description: `Incremental Export table ${this.sourceDynamoDbTable.tableName}`,
                'Value': WorkflowState[WorkflowState.PITR_GAP],
                'Overwrite': true,
                'Type': 'String'
            },
            resultPath: `$.${StepFunctionOutputConstants.PUT_WORKFLOW_STATE_PARAMETER_TO_PITR_GAP_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });

        this.setWorkflowStateParameterToNormal = new tasks.CallAwsService(this.scope, 'set-workflow-state-parameter-to-normal', {
            service: 'ssm',
            action: 'putParameter',
            stateName: 'SetWorkflowStateParameterToNormal',
            comment: 'Update the workflow state parameter to NORMAL',
            iamResources: [workflowStateParameterArn],
            parameters: {
                Name: `/${this.workflowStateParameterName}`,
                Description: `Incremental Export table ${this.sourceDynamoDbTable.tableName}`,
                'Value': WorkflowState[WorkflowState.NORMAL],
                'Overwrite': true,
                'Type': 'String'
            },
            resultPath: `$.${StepFunctionOutputConstants.PUT_WORKFLOW_STATE_PARAMETER_TO_NORMAL_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });

        const workflowActionParameterArn = this.getParameterStoreParameterArn(this.workflowActionParameterName, cdk.ArnFormat.SLASH_RESOURCE_NAME);
        this.setWorkflowActionParameterToRun = new tasks.CallAwsService(this.scope, 'set-workflow-action-parameter-to-run', {
            service: 'ssm',
            action: 'putParameter',
            stateName: 'SetWorkflowActionParameterToRun',
            comment: 'Update the workflow action parameter to RUN',
            iamResources: [workflowActionParameterArn],
            parameters: {
                Name: `/${this.workflowActionParameterName}`,
                Description: `Incremental Export table ${this.sourceDynamoDbTable.tableName}`,
                'Value': WorkflowAction[WorkflowAction.RUN],
                'Overwrite': true,
                'Type': 'String'
            },
            resultPath: `$.${StepFunctionOutputConstants.PUT_WORKFLOW_ACTION_PARAMETER_TO_RUN_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });
        

        this.setEmptyWorkflowInitiatedParameter = new tasks.CallAwsService(this.scope, 'set-empty-workflow-initiated-parameter', {
            service: 'ssm',
            action: 'putParameter',
            stateName: 'SetEmptyWorkflowInitiatedParameter',
            comment: 'Set the workflow initiated parameter to an empty value',
            iamResources: [workflowInitiatedParameterArn],
            parameters: {
                Name: `/${this.workflowInitiatedParameterName}`,
                Description: `Incremental Export table ${this.sourceDynamoDbTable.tableName}`,
                'Value': KeywordConstants.NULL_STRING, // we set an empty value to indicate that the worklow has begun but not completed (with success or failure)
                'Overwrite': true,
                'Type': 'String'
            },
            resultPath: `$.${StepFunctionOutputConstants.PUT_WORKFLOW_INITIATED_PARAMETER_OUTPUT}`,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });

        this.pitrGapFound = new sfn.Fail(this.scope, 'pitr-gap-found-failed',  {
            stateName: 'PITRGapFound'
        });

        this.fullExportFailed = new sfn.Fail(this.scope, 'full-export-failed',  {
            stateName: 'FullExportFailed'
        });

        this.fullExportSucceeded = new sfn.Succeed(this.scope, 'full-export-succeeded',  {
            stateName: 'FullExportSucceeded',
        });

        this.fullExportStillRunning = new sfn.Succeed(this.scope, 'full-export-still-running',  {
            stateName: 'FullExportStillRunning',
        });

        this.didWorkflowInitiateSuccessfully = new sfn.Choice(this.scope, 'did-worklow-initiate-successfully', {
            comment: 'Did the workflow initiate successfully?',
            stateName: 'DidWorkflowInitiateSuccessfully?'
        });

        this.checkIncrementalExportNeeded = new sfn.Choice(this.scope, 'check-incremental-export-needed', {
            comment: 'Check if incremental export is needed',
            stateName: 'CheckIncrementalExportNeeded?'
        });

        this.checkWorkflowAction = new sfn.Choice(this.scope, 'check-workflow-action-state', {
            comment: 'Check if workflow needs to run',
            stateName: 'CheckWorkflowAction?'
        });

        this.incrementalExportNotNeeded = new sfn.Succeed(this.scope, 'incremental-export-not-needed', {
            comment: 'Incremental export not needed',
            stateName: 'IncrementalExportNotNeeded'
        });

        this.workflowPaused = new sfn.Succeed(this.scope, 'workflow-paused', {
            comment: 'Workflow has been paused',
            stateName: 'WorkflowPaused'
        });

        this.notifyOnFullExport = this.getSnsPublishTask(
            'notify-on-full-export', 
            'NotifyOnFullExport', 
            'Full export outcome', 
            sfn.TaskInput.fromJsonPathAt(`$.${StepFunctionOutputConstants.WORKFLOW_INITIALIZED_PARAMETER_OUTPUT}.snsMessage`), 
            'Notify on full export outcome', 
            `$.${StepFunctionOutputConstants.NOTIFY_ON_FULL_EXPORT_OUTPUT}`);

        this.notifyOnFullExportRunning = this.getSnsPublishTask(
            'notify-on-full-export-running', 
            'NotifyOnFullExportRunning', 
            'Full export outcome', 
            sfn.TaskInput.fromObject({
                message: `${ExportType[ExportType.FULL_EXPORT]} export for table '${this.sourceDynamoDbTable.tableName}' is still running`,
                'executionId.$': '$$.Execution.Id',
                exportType: ExportType[ExportType.FULL_EXPORT],
                status: KeywordConstants.SNS_SUCCESS
            }),
            'Notify on full export running', 
            `$.${StepFunctionOutputConstants.NOTIFY_ON_FULL_EXPORT_RUNNING_OUTPUT}`);
            
        this.notifyOnPitrGap = this.getNotifyOnIncrementalExportStartTimeOutsidePitrWindowSnsPublishTask('notify-on-pitr-gap', 
            'NotifyOnPitrGap', `$.${StepFunctionOutputConstants.NOTIFY_ON_PITR_GAP_OUTPUT}`);

        this.notifyOnTaskFailed = this.getSnsPublishTask(
            'notify-on-task-failed', 
            'NotifyOnTaskFailed',
            'Task failed', 
            sfn.TaskInput.fromObject({
                'executionId.$': '$$.Execution.Id',
                'executionStartTime.$': '$$.Execution.StartTime',
                'error.$': '$.Error',
                'cause.$': '$.Cause',
                status: KeywordConstants.SNS_FAILED
            }),
            'Notify on task failed', 
            `$.${StepFunctionOutputConstants.NOTIFY_ON_DESCRIBE_TABLE_FAILED_OUTPUT}`);
          
    }

    private getParameterStoreParameterArn(parameterName: string, arnFormat: cdk.ArnFormat = cdk.ArnFormat.COLON_RESOURCE_NAME) : string {
        return this.getArn('ssm', 'parameter', parameterName, arnFormat);
    }

    private getArn(service: string, resource: string, resourceName: string, arnFormat: cdk.ArnFormat = cdk.ArnFormat.COLON_RESOURCE_NAME) {
        return cdk.Stack.of(this.scope).formatArn({
            service: service,
            resource: resource,
            resourceName: resourceName,
            arnFormat: arnFormat,
        });
    }

    private getDescribeExportNode(id: string, stateName: string, resultPath: string, parameters: any) {
        return this.getDynamoDbTask(id, 'describeExport', stateName, 'Let\'s check if export has finished', resultPath, parameters,
            undefined,
            this.getArn('dynamodb', 'table', `${this.sourceDynamoDbTable.tableName}/export/*`, cdk.ArnFormat.SLASH_RESOURCE_NAME));
    }

    private getDynamoDbTask(taskId: string, taskAction: string, taskStateName: string, taskComment: string, taskResultPath: string, parameters: any, additionalIamStatements?: iam.PolicyStatement[], ...iamResources: string[]): tasks.CallAwsService {
        const dynamoDbTask = new tasks.CallAwsService(this.scope, taskId, {
            service: 'dynamoDb',
            action: taskAction,
            stateName: taskStateName,
            comment: taskComment,
            resultPath: taskResultPath,
            iamResources: [this.sourceDynamoDbTable.tableArn, ...iamResources],
            parameters: parameters,
            additionalIamStatements: additionalIamStatements,
            taskTimeout: this.awsApiInvocationTaskTimeout
        });


        return dynamoDbTask;
    }

    private getExportCompletedNode(id: string, stateName: string) {
        return new sfn.Choice(this.scope, id, {
            comment: 'Checks whether the export has completed',
            stateName: stateName
        });
    }

    private getExportPassNodes(id: string, exportType: ExportType, stateName: string, resultPath: string, outputName: string, 
        exportStartTime: string, exportEndTime: string, startTime: string, endTime: string, incrementalBlocksBehind: any, remedy: any,
        exportArn: string) : { success: sfn.Pass, failed: sfn.Pass } {
        
        const timeParamSuccess = {
            'exportStartTime.$': exportStartTime,
            'exportEndTime.$': exportEndTime,
            'startTime.$': startTime,
            'endTime.$': endTime
        };

        const timeParamFailed = {
            'exportStartTime.$': exportStartTime,
            'startTime.$': startTime,
        };
        
        const success = new sfn.Pass(this.scope, `${id}-true`, {
            stateName: `${stateName}Success`,
            parameters: {
                success: 'true',
                snsMessage: {
                    message: `${ExportType[exportType]} export for table \'${this.sourceDynamoDbTable.tableName}\' succeeded`,
                    'executionId.$': '$$.Execution.Id',
                    sourceDynamoDbTable: this.sourceDynamoDbTable.tableName,
                    targetBucket: this.sourceDataExportBucket.bucket,
                    targetBucketPrefix: this.sourceDataExportBucket.prefix,
                    'exportArn.$': exportArn,
                    exportType: ExportType[exportType],
                    status: KeywordConstants.SNS_SUCCESS,
                    ...timeParamSuccess,
                    ...incrementalBlocksBehind
                }
            },
            resultPath: resultPath
        });

        const failed = new sfn.Pass(this.scope, `${id}-false`, {
            stateName: `${stateName}Failed`,
            parameters: {
                success: 'false',
                snsMessage: {
                    'message.$': `States.Format('${ExportType[exportType]} export for table ${this.sourceDynamoDbTable.tableName} failed because\n{}\n{}', $.${outputName}.ExportDescription.FailureCode, $.${outputName}.ExportDescription.FailureMessage)`,
                    'executionId.$': '$$.Execution.Id',
                    sourceDynamoDbTable: this.sourceDynamoDbTable.tableName,
                    targetBucket: this.sourceDataExportBucket.bucket,
                    targetBucketPrefix: this.sourceDataExportBucket.prefix,
                    exportArn: exportArn,
                    exportType: ExportType[exportType],
                    status: KeywordConstants.SNS_FAILED,
                    ...timeParamFailed,
                    ...incrementalBlocksBehind,
                    ...remedy
                }
            },
            resultPath: resultPath
        });

        return { success, failed };
    }

    private getWaitForExportNode(id: string, stateName: string) {
        return new sfn.Wait(this.scope, id, {
            stateName: stateName,
            comment: 'Wait for export to complete',
            time: sfn.WaitTime.duration(cdk.Duration.seconds(this.waitTimeToCheckExportStatusInSeconds))
        });
    }

    private getSnsPublishTask(id: string, stateName: string, subject: string, message: sfn.TaskInput, comment: string, resultPath: string) : tasks.SnsPublish {
        return new tasks.SnsPublish(this.scope, id, {
            stateName: stateName,
            message: message,
            topic: this.snsTopic,
            comment: comment,
            resultPath: resultPath,
            subject: subject
        });
    }

    private getNotifyOnIncrementalExportStartTimeOutsidePitrWindowSnsPublishTask(id: string, stateName: string, outputName: string) : tasks.SnsPublish {

        const incrementalExportStartTimeOutsidePitrWindowMessage = 
            `Incremental export start time for table '${this.sourceDynamoDbTable.tableName}' is outside PITR window, most likely due to PITR being disabled and re-enabled`;
        const incrementalExportStartTimeOutsidePitrWindowRemedy = 
            `Set the '${this.workflowActionParameterName}' to '${WorkflowAction[WorkflowAction.RESET_WITH_FULL_EXPORT_AGAIN]}' to reinitialize the workflow [refer to the ReadMe]`;

        const incrementalExportStartTimeOutsidePitrWindowSnsMessage = {
            message: incrementalExportStartTimeOutsidePitrWindowMessage,
            'executionId.$': '$$.Execution.Id',
            exportType: ExportType[ExportType.INCREMENTAL_EXPORT],
            status: KeywordConstants.SNS_FAILED,
            remedy: incrementalExportStartTimeOutsidePitrWindowRemedy
          };

        return this.getSnsPublishTask(id, stateName, 'Incremental export start time outside PITR window', 
            sfn.TaskInput.fromObject(incrementalExportStartTimeOutsidePitrWindowSnsMessage), 'Notify on PITR gap', outputName);
    }
}
