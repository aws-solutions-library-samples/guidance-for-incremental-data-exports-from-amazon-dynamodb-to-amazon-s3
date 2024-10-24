import { Construct } from 'constructs';
import { IncrementalExportDefaults } from './constants/incrementalExportDefaults';
import { ExportViewType } from './constants/exportViewType';
import { ExportFormat } from './constants/exportFormat';

export class Configuration {
  sourceDynamoDbTableName: string;
  deploymentAlias: string;
  failureNotificationEmail: string;
  successNotificationEmail: string;
  dataExportBucketOwnerAccountId?: string;
  dataExportBucketName: string;
  dataExportBucketPrefix: string;
  incrementalExportWindowSizeInMinutes: number;
  waitTimeToCheckExportStatusInSeconds: number;
  exportViewType: ExportViewType;
  exportFormat: ExportFormat;
  awsApiInvocationTimeoutInSeconds: number;

  constructor(scope: Construct) {
    this.sourceDynamoDbTableName = scope.node.tryGetContext('sourceDynamoDbTableName') as string;
    this.deploymentAlias = scope.node.tryGetContext('deploymentAlias') as string;
    this.failureNotificationEmail = scope.node.tryGetContext('failureNotificationEmail') as string;
    this.successNotificationEmail = scope.node.tryGetContext('successNotificationEmail') as string;
    this.dataExportBucketOwnerAccountId = scope.node.tryGetContext('dataExportBucketOwnerAccountId') as string;
    this.dataExportBucketName = scope.node.tryGetContext('dataExportBucketName') as string;
    this.dataExportBucketPrefix = scope.node.tryGetContext('dataExportBucketPrefix') as string;
    this.incrementalExportWindowSizeInMinutes = parseInt(scope.node.tryGetContext('incrementalExportWindowSizeInMinutes')) || 
      IncrementalExportDefaults.DEFAULT_INCREMENTAL_EXPORT_WINDOW_SIZE_IN_MINUTES;
    this.waitTimeToCheckExportStatusInSeconds = parseInt(scope.node.tryGetContext('waitTimeToCheckExportStatusInSeconds')) || 
      IncrementalExportDefaults.WAIT_TIME_TO_CHECK_EXPORT_STATUS_IN_SECONDS;
    this.exportFormat = scope.node.tryGetContext('exportFormat') as ExportFormat || 
      ExportFormat.DYNAMODB_JSON;
    this.exportViewType = scope.node.tryGetContext('exportViewType') as ExportViewType || 
      ExportViewType.NEW_AND_OLD_IMAGES;

    this.awsApiInvocationTimeoutInSeconds = parseInt(scope.node.tryGetContext('awsApiInvocationTimeoutInSeconds')) || 
      IncrementalExportDefaults.AWS_API_INVOCATION_TIMEOUT_IN_SECONDS;
  }
}
