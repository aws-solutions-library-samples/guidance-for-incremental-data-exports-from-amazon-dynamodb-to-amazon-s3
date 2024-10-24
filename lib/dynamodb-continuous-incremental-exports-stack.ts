import * as ddbSdk from '@aws-sdk/client-dynamodb';
import * as cdk from 'aws-cdk-lib';
import {
  aws_dynamodb as ddb,
  aws_iam as iam,
  aws_kms as kms,
  aws_lambda as lambda,
  aws_logs as logs,
  aws_scheduler as scheduler,
  aws_stepfunctions as sfn,
  aws_sns as sns
} from "aws-cdk-lib";
import { Construct } from 'constructs';
import { ConditionBuilder } from './conditionBuilder';
import { Configuration } from './configuration';
import { AwsServicePrincipals } from './constants/awsServicePrincipals';
import { ScheduleConstants } from './constants/scheduleConstants';
import { StepFunctionOutputConstants } from './constants/stepFunctionOutputConstants';
import { DataExportBucket } from './constructs/dataExportBucket';
import { NodeBuilder } from './nodeBuilder';
import { KeywordConstants } from './constants/keywordConstants';
import { IncrementalExportDefaults } from './constants/incrementalExportDefaults';

export class DynamoDbContinuousIncrementalExportsStack extends cdk.Stack {

  private configuration: Configuration;
  private sourceDataExportBucket: DataExportBucket;
  private sourceDynamoDbTable: ddb.ITable;

  private ddbExportNotificationTopic: sns.Topic;
  private conditionBuilder: ConditionBuilder;
  private nodeBuilder: NodeBuilder;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    this.templateOptions.description = 'Guidance for Incremental Data Exports from Amazon DynamoDB (SO9520)';
    this.init();
  }

  private async init() {
    
    this.configuration = new Configuration(this);
    await this.sanityChecks();

    // Let's create/get the source data export bucket as needed
    this.sourceDataExportBucket = new DataExportBucket(this, 'source-data-export-bucket', {
      account: this.account,
      region: this.region,
      bucketOwnerAccountId: this.configuration.dataExportBucketOwnerAccountId,
      name: this.configuration.dataExportBucketName,
      sourceDdbTablename: this.configuration.sourceDynamoDbTableName,
      deploymentAlias: this.configuration.deploymentAlias,
      prefix: this.configuration.dataExportBucketPrefix,
      exportFormat: this.configuration.exportFormat,
    });

    this.sourceDynamoDbTable = ddb.Table.fromTableName(this, 'source-ddb-source-table', this.configuration.sourceDynamoDbTableName);
    
    const kmsKeyUsedForSnsTopic = this.deployNotificationModule();
    const incrementalExportTimeManipulatorFunction = this.deployIncrementalExportTimeManipulatorFunction();

    this.conditionBuilder = new ConditionBuilder();
    this.nodeBuilder = new NodeBuilder(this, this.sourceDynamoDbTable, this.sourceDataExportBucket, this.ddbExportNotificationTopic, incrementalExportTimeManipulatorFunction, this.configuration);

    const incrementalExportStateMachine = this.deployStepFunction(kmsKeyUsedForSnsTopic);

    const schedulerRole = new iam.Role(this, 'step-function-trigger-role', {
      roleName: `${this.configuration.deploymentAlias}-incremental-export-schedule-role`,
      description: 'Roles used to triggers the step function scheduler',
      assumedBy: new iam.ServicePrincipal(AwsServicePrincipals.SCHEDULER, {
        conditions: {
          StringEquals: {
            'aws:SourceAccount': this.account
          }
        }
      }),
      inlinePolicies: {
        'execute_statemachine': new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement(
            {
              effect: iam.Effect.ALLOW,
              actions: ['states:StartExecution'],
              resources: [incrementalExportStateMachine.stateMachineArn]
            })
          ]
        })
      }
    });

    /* 
     * If the scheduler is stopped, we need capability to automatically catch-up, 
     * therefore the scheduler needs to run more often. So we divide the time period by 3
     */
    const schedulerTime = Math.floor(this.configuration.incrementalExportWindowSizeInMinutes/3);
    const cfnSchedule = new scheduler.CfnSchedule(this, 'step-function-trigger-schedule', {
      flexibleTimeWindow: {
        mode: ScheduleConstants.SCHEDULE_MODE_OFF
      },
      scheduleExpression: `rate(${schedulerTime} minutes)`,
      target: {
        arn: incrementalExportStateMachine.stateMachineArn,
        roleArn: schedulerRole.roleArn,
    
        retryPolicy: {
          maximumEventAgeInSeconds: cdk.Duration.days(1).toSeconds(),
          maximumRetryAttempts: 5,
        },
      },
      description: `${this.configuration.deploymentAlias} export schedule, triggers the step function every ${schedulerTime} minutes`,
      state: KeywordConstants.ENABLED
    });
  }

  private deployNotificationModule() : kms.Key {
    const snsKey = new kms.Key(this, 'ddb-export-notification-topic-key', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pendingWindow: cdk.Duration.days(7),
      description: `Key for SSE for the notification topic used by incremental export for table ${this.sourceDynamoDbTable.tableName}`,
      enableKeyRotation: true
    });
    cdk.Tags.of(snsKey).add('Name', `${this.configuration.deploymentAlias}-ddb-export-notification-topic-key`);

    const topicName = `${this.configuration.deploymentAlias}-notification-topic`;
    this.ddbExportNotificationTopic = new sns.Topic(this, 'ddb-export-notification-topic', {
      displayName: topicName,
      topicName: topicName,
      enforceSSL: true,
      masterKey: snsKey
    });

    if(this.configuration.successNotificationEmail && this.configuration.successNotificationEmail !== "") {
      const successNotificationSub = new sns.Subscription(this, 'ddb-export-notification-success-subsc', {
        topic: this.ddbExportNotificationTopic,
        endpoint: this.configuration.successNotificationEmail,
        protocol: sns.SubscriptionProtocol.EMAIL,
        filterPolicyWithMessageBody: { status: sns.FilterOrPolicy.filter(sns.SubscriptionFilter.stringFilter({ allowlist: ['SUCCESS'] })) }
      });
    }

    if(this.configuration.failureNotificationEmail && this.configuration.failureNotificationEmail !== "") {
      const failureNotificationSub = new sns.Subscription(this, 'ddb-export-notification-failure-subsc', {
        topic: this.ddbExportNotificationTopic,
        endpoint: this.configuration.failureNotificationEmail,
        protocol: sns.SubscriptionProtocol.EMAIL,
        filterPolicyWithMessageBody: { status: sns.FilterOrPolicy.filter(sns.SubscriptionFilter.stringFilter({ allowlist: ['FAILED'] })) }
      });
    }

    return snsKey;
  }

  private dynamoDbSdkRetryHandler() : any {
    return {
      errors: ['DynamoDb.SdkClientException'], 
      interval: cdk.Duration.seconds(IncrementalExportDefaults.SDK_EXCEPTION_INTERVAL_DURATION_SECONDS), 
      maxAttempts: IncrementalExportDefaults.SDK_EXCEPTION_RETRY_ATTEMPTS, 
      backoffRate: IncrementalExportDefaults.SDK_EXCEPTION_BACKOFF_RATE
    };
  }

  private ssmSdkRetryHandler() : any {
    return {
      errors: ['Ssm.SdkClientException'], 
      interval: cdk.Duration.seconds(IncrementalExportDefaults.SDK_EXCEPTION_INTERVAL_DURATION_SECONDS), 
      maxAttempts: IncrementalExportDefaults.SDK_EXCEPTION_RETRY_ATTEMPTS, 
      backoffRate: IncrementalExportDefaults.SDK_EXCEPTION_BACKOFF_RATE
    };
  }

  private deployStepFunction(kmsKeyUsedForSnsTopic: kms.Key) {

    // alias for better readability
    const nb = this.nodeBuilder;
    const cb = this.conditionBuilder;

    nb.notifyOnIncrementalExport.addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
      .next(nb.didIncrementalExportCompleteSuccessfully
        .when(cb.incrementalExportStateIsFalse, nb.incrementalExportFailed, {comment: 'Incremental export failed'})
        .afterwards({includeOtherwise: true})
        .next(nb.incrementalExportSucceeded)
      );

    nb.setWorkflowStateParameterToPitrGap
        .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
        .addRetry(this.ssmSdkRetryHandler())
      .next(nb.notifyOnIncrementalExportStartTimeOutsidePitrWindow
        .next(nb.incrementalExportStartTimeOutsidePitrWindowFail)
      );

    nb.getNextIncrementalExportTimeLambdaInvoke.addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
      .next(nb.checkIncrementalExportNeeded
        .when(cb.nextIncrementalExportEndTimeIsPastCurrentTime, nb.incrementalExportNotNeeded, {comment: 'Incremental export is not needed'})
        .afterwards({includeOtherwise: true})
        .next(nb.isEarliestRestoreDateTimeValidChoice
          .when(cb.earliestRestoreDateTimeIsGreaterThanExportStartTime, nb.setWorkflowStateParameterToPitrGap, { comment: 'Incremental export start time is after PITR earliest restore time'})
          .afterwards({includeOtherwise: true})
          .next(nb.executeIncrementalExport
              .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
              .addCatch(nb.setWorkflowStateParameterToPitrGap, { errors: ['DynamoDb.InvalidExportTimeException']})

              // At times the incremental export happens too quickly and therefore resulting in the exportToTime being past the current time
              .addRetry({errors: ['DynamoDb.InvalidExportTimeException'], interval: cdk.Duration.minutes(1), maxAttempts: 2, backoffRate: 1})
              .addRetry(this.dynamoDbSdkRetryHandler())
            .next(nb.setLastIncrementalExportTimeParameter
                .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                .addRetry(this.ssmSdkRetryHandler())
              .next(nb.describeIncrementalExport
                  .addRetry(this.dynamoDbSdkRetryHandler())
                  .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                .next(nb.incrementalExportCompletedState
                  .when(cb.incrementalExportCompleted, nb.incrementalExportParameterTrue.next(nb.notifyOnIncrementalExport), {comment: 'IncrementalExport successful'})
                  .when(cb.incrementalExportFailed, nb.incrementalExportParameterFalse.next(nb.notifyOnIncrementalExport), {comment: 'IncrementalExport failed'})
                  .afterwards({includeOtherwise: true})
                  .next(nb.waitForIncrementalExport
                    .next(nb.describeIncrementalExport)
                  )
                )  
              )
            )
          )
        )
      );
    

    nb.setWorkflowInitiatedParameter
        .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
        .addRetry(this.ssmSdkRetryHandler())
      .next(nb.notifyOnFullExport.addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
        .next(nb.didWorkflowInitiateSuccessfully
          .when(cb.workflowInitializedParameterOutputFalse, nb.fullExportFailed, {comment: 'Workflow initialized but unsuccessfully'})
          .afterwards({includeOtherwise: true})
          .next(nb.fullExportSucceeded)
        )
      );
  
    nb.notifyOnTaskFailed.next(nb.taskFailedNode);

    nb.executeFullExport
        .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
        .addRetry(this.dynamoDbSdkRetryHandler())
      .next(nb.setFullExportTimeParameter
          .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
          .addRetry(this.ssmSdkRetryHandler())
        .next(nb.setWorkflowActionParameterToRun
            .addCatch(nb.setEmptyWorkflowInitiatedParameter, {
              errors: ['Ssm.ParameterNotFoundException'],
              resultPath: `$.${StepFunctionOutputConstants.PUT_WORKFLOW_ACTION_PARAMETER_TO_RUN_OUTPUT}`,
            })
            .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
            .addRetry(this.ssmSdkRetryHandler())
          .next(nb.setWorkflowStateParameterToNormal
              .addCatch(nb.setEmptyWorkflowInitiatedParameter, {
                errors: ['Ssm.ParameterNotFoundException'],
                resultPath: `$.${StepFunctionOutputConstants.PUT_WORKFLOW_STATE_PARAMETER_TO_NORMAL_OUTPUT}`,
              })
              .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
              .addRetry(this.ssmSdkRetryHandler())
            .next(nb.setEmptyWorkflowInitiatedParameter
                .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                .addRetry(this.ssmSdkRetryHandler())
              .next(nb.deleteLastIncrementalExportTimeParameter
                  .addCatch(nb.describeFullExport, {
                    errors: ['Ssm.ParameterNotFoundException'],
                    resultPath: `$.${StepFunctionOutputConstants.DELETE_LAST_INCREMENTAL_EXPORT_TIME_PARAMETER_OUTPUT}`,
                  })
                  .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                  .addRetry(this.ssmSdkRetryHandler())
                .next(nb.describeFullExport
                    .addRetry(this.dynamoDbSdkRetryHandler())
                    .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                  .next(nb.fullExportCompletedState
                    .when(cb.fullExportCompleted, nb.workflowInitializedParameterTrue.next(nb.setWorkflowInitiatedParameter), {comment: 'FullExport successful'})
                    .when(cb.fullExportFailed, nb.workflowInitializedParameterFalse.next(nb.setWorkflowInitiatedParameter), {comment: 'FullExport failed'})
                    .afterwards({includeOtherwise: true})
                    .next(nb.waitForFullExport
                      .next(nb.describeFullExport)
                    )
                  )
                )
              )
            )
          )
        )
      );

    const definition = nb.getParametersTask
            .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
            .addRetry(this.ssmSdkRetryHandler())
          .next(nb.cleanParametersPassState
            .next(nb.checkWorkflowAction
              .when(cb.isWorkflowPaused, nb.workflowPaused, { comment: 'Workflow is paused'} )
              .afterwards({includeOtherwise: true})
              .next(nb.ensureTableExistsTask
                  .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                  .addRetry(this.dynamoDbSdkRetryHandler())
                .next(nb.describeContinuousBackupsAwsServiceTask
                    .addRetry(this.dynamoDbSdkRetryHandler())
                    .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                  .next(nb.pitrEnabledChoice
                    .when(cb.pitrIsEnabled, nb.initializeWorkflowState
                      .when(cb.fullExportStillRunning, nb.notifyOnFullExportRunning
                        .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                        .next(nb.fullExportStillRunning), {comment: 'Full export still running'})
                      .when(cb.resetWithFullExportAgain, nb.executeFullExport, { comment: 'Workflow needs to start again due to PITR gap'}) // ensure this condition is before the workflow_state:PITR_GAP condition
                      .when(cb.pitrGapWorkflowState, nb.notifyOnPitrGap
                        .addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                        .next(nb.pitrGapFound), { comment: 'PITR gap found' })
                      .when(cb.executeFullExport, nb.executeFullExport, { comment: 'Workflow has not been initialized'})
                      .afterwards({includeOtherwise: true})
                      .next(nb.isLastIncrementalExportParameterValid
                        .when(this.conditionBuilder.lastIncrementalExportTimeIsValid, 
                          nb.useLastIncrementalExportTimeParameterValue
                            .next(nb.getNextIncrementalExportTimeLambdaInvoke), 
                          { comment: 'Last incremental export time is valid'}
                        )
                        .afterwards({includeOtherwise: true})
                        .next(nb.useFullExportTimeParameterValue
                          .next(nb.getNextIncrementalExportTimeLambdaInvoke)
                        )
                      ), 
                    { comment: 'PITR is enabled'})
                    .afterwards({includeOtherwise: true})
                    .next(nb.notifyOnPitrDisabled.addCatch(nb.notifyOnTaskFailed, { errors: ['States.ALL'] })
                      .next(nb.pitrDisabledFail)
                    )
                  )
                )
              )
            )
          );

    const stateMachineLogGroup = new logs.LogGroup(this, 'incremental-export-log-group', {
      logGroupName: `${this.configuration.deploymentAlias}-incremental-export-log-group`,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      retention: logs.RetentionDays.FIVE_DAYS,
      logGroupClass: logs.LogGroupClass.INFREQUENT_ACCESS
    });

    const stateMachineName = `${this.configuration.deploymentAlias}-ddb-export-state-machine`;
    const stateMachine = new sfn.StateMachine(this, stateMachineName, {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      stateMachineName: stateMachineName,
      comment: 'DynamoDB export state machine',
      tracingEnabled: true,
      logs: {
        destination: stateMachineLogGroup,
        level: sfn.LogLevel.ALL,
        includeExecutionData: false // Turn this on if you want to see state information being passed
      }
    });

    stateMachine.addToRolePolicy(new iam.PolicyStatement(
    {
      effect: iam.Effect.ALLOW,
      actions: ['logs:CreateLogDelivery',
        'logs:DeleteLogDelivery',
        'logs:DescribeLogGroups',
        'logs:DescribeResourcePolicies',
        'logs:GetLogDelivery',
        'logs:ListLogDeliveries',
        'logs:PutResourcePolicy',
        'logs:UpdateLogDelivery'],
      resources: [stateMachineLogGroup.logGroupArn]
    }));
    stateMachine.addToRolePolicy(new iam.PolicyStatement(
    {
      effect: iam.Effect.ALLOW,
      actions: ['kms:GenerateDataKey', 'kms:Decrypt'],
      resources: [kmsKeyUsedForSnsTopic.keyArn]
    }));

    return stateMachine;
  }

  private deployIncrementalExportTimeManipulatorFunction() : lambda.Function {

    const incrementalExportTimeManipulatorFunctionName = `${this.configuration.deploymentAlias}-incremental-export-time-manipulator`;

    const incrementalExportTimeManipulatorLogGroup = new logs.LogGroup(this, 'incremental-export-time-manipulator-log-group', {
      logGroupName: `${this.configuration.deploymentAlias}-incremental-export-time-manipulator-log-group`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.FIVE_DAYS,
      logGroupClass: logs.LogGroupClass.INFREQUENT_ACCESS
    });

    const incrementalExportTimeManipulatorLambdaExecutionRole = new iam.Role(this, 'incremental-export-time-manipulator-role', {
      roleName: `${this.configuration.deploymentAlias}-incremental-export-time-manipulator-role`,
      assumedBy: new iam.ServicePrincipal(AwsServicePrincipals.LAMBDA),
      inlinePolicies: {
        'lambdaExecPolicy': new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement(
            {
              effect: iam.Effect.ALLOW,
              actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
              resources: [incrementalExportTimeManipulatorLogGroup.logGroupArn]
            })
          ]
        })
      }
    });

    return new lambda.Function(this, 'incremental-export-time-manipulator', {
      code: lambda.Code.fromAsset('./lib/runtime/continuousIncrementalExportsTimeManipulator'),
      handler: 'index.handler',
      runtime: lambda.Runtime.NODEJS_20_X,
      functionName: incrementalExportTimeManipulatorFunctionName,
      architecture: lambda.Architecture.ARM_64,
      role: incrementalExportTimeManipulatorLambdaExecutionRole,
      logGroup: incrementalExportTimeManipulatorLogGroup
    });
  }

  private async sanityChecks() {
    await this.dynamoDbSanityChecks();

    if (!this.isValidDeploymentAlias())
      throw new Error(`deploymentAlias has to be an alphanumeric string between 1 and 15 characters`);

    if (this.configuration.incrementalExportWindowSizeInMinutes < 15 || this.configuration.incrementalExportWindowSizeInMinutes > 24*60) {
      throw new Error(`incrementalExportWindowSizeInMinutes has to be between 15 minutes and 1,440 minutes (24h)`);
    }
  }

  private isValidDeploymentAlias(): boolean {
    const regex = /^[a-z0-9]{1,15}$/;
    return regex.test(this.configuration.deploymentAlias);
  }

  private async dynamoDbSanityChecks() {
    const ddbTableName = this.configuration.sourceDynamoDbTableName;
    if (!ddbTableName || ddbTableName === "") {
      throw new Error('Source DynamoDB table name must be supplied');
    }

    await this.doesTableExist(ddbTableName)
      .then(function (result: boolean) {
        if (!result) {
          throw new Error(`Source DynamoDB table \'${ddbTableName}\' cannot be found`);
        }
      });
  }

  private async doesTableExist(tableName: string): Promise<boolean> {
    const dynamoDB = new ddbSdk.DynamoDB();
  
    try {
      const response = await dynamoDB.describeTable({ TableName: tableName }, {
        requestTimeout: 60000
      });
      return true; 
    } catch (error) {
      return false;
    }
  }
}
