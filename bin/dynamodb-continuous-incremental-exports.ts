#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag'
import { Aspects } from 'aws-cdk-lib';
import { DynamoDbContinuousIncrementalExportsStack } from '../lib/dynamodb-continuous-incremental-exports-stack';
import { NagSuppressions } from 'cdk-nag';

const app = new cdk.App();

Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));

const stackName = app.node.tryGetContext('stackName') as string;
const incrementalExportStack = new DynamoDbContinuousIncrementalExportsStack(app, `${stackName}`, {});
NagSuppressions.addStackSuppressions(incrementalExportStack, [
    {
        id: 'AwsSolutions-IAM5',
        reason: 'Tasks added to the step function add the appropriate policies to the IAM role used by the Step Function',
    }
]);