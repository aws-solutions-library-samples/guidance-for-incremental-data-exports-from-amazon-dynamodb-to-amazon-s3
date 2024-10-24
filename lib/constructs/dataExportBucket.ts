import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import {aws_s3 as s3, aws_iam as iam} from "aws-cdk-lib"
import { ExportFormat } from '../constants/exportFormat';

export interface BucketProps {
  name: string;
  bucketOwnerAccountId?: string;
  region: string;
  account: string;
  sourceDdbTablename: string;
  deploymentAlias: string;
  prefix?: string;
  exportFormat: ExportFormat
}

export class DataExportBucket extends Construct {

  bucketOwnerAccountId: string;
  bucket: string;
  prefix?: string;
  exportFormat: ExportFormat;

  constructor(scope: Construct, id: string, props: BucketProps) {
    super(scope, id);

    this.bucketOwnerAccountId = props.bucketOwnerAccountId ?? props.account;
    let bucketName = props.name;

    if (!bucketName || bucketName === "") {

      bucketName = `${props.deploymentAlias}-data-export`;

      const serverAccessLogBucket = new s3.Bucket(this, `${id}-resource-server-access-log-bucket`, {
        bucketName: `${bucketName}-server-access-logs`,
        autoDeleteObjects: false,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });
      this.onlyHttpsRequestsAllowed(serverAccessLogBucket);
      
      const bucket = new s3.Bucket(this, `${id}-resource`, {
        bucketName: bucketName,
        encryption: s3.BucketEncryption.KMS_MANAGED,
        autoDeleteObjects: false,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
        serverAccessLogsBucket: serverAccessLogBucket,
        
        // more props to be set here as needed
      });
      this.onlyHttpsRequestsAllowed(bucket);
      this.bucket = bucket.bucketName;
    }
    else {
      this.bucket = bucketName;
    }

    this.prefix = props.prefix;
    this.exportFormat = props.exportFormat;

    new cdk.CfnOutput(this, `${props.deploymentAlias}-data-export-bucket`, {
      value: this.bucket,
      exportName: `${props.deploymentAlias}-data-export-bucket`,
      description: 'Data export bucket'
    });

    if (this.prefix !== undefined && this.prefix !== "") {
      new cdk.CfnOutput(this, `${props.deploymentAlias}-data-export-bucket-prefix`, {
        value: this.prefix,
        exportName: `${props.deploymentAlias}-data-export-bucket-prefix`,
        description: 'Data export bucket prefix'
      });
    }
  }

  private onlyHttpsRequestsAllowed(bucket: s3.IBucket) {
    bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        actions: ['s3:*'],
        effect: iam.Effect.DENY,
        principals: [new iam.AnyPrincipal()],
        resources: [bucket.bucketArn, bucket.bucketArn + "/*"],
        conditions: {
          Bool: {
            'aws:SecureTransport': 'false'
          },
        },
      })
    );
  }

  hasPrefix() : boolean {
    return this.prefix !== undefined && this.prefix !== "";
  }

  public getExecuteExportParameters(sourceDynamoDbTableArn: string): any {
    if (!this.hasPrefix()) {
      return {
          S3Bucket: this.bucket,
          TableArn: sourceDynamoDbTableArn,
          ExportFormat: ExportFormat[this.exportFormat]
      };
    } else {
      return {
          S3Bucket: this.bucket,
          S3Prefix: this.prefix,
          TableArn: sourceDynamoDbTableArn,
          ExportFormat: ExportFormat[this.exportFormat]
      };
    }
  }
}
