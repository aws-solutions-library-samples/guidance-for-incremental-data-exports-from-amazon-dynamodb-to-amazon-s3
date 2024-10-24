export const handler = async (event, context) => {
    
    console.log(event);
    
    const executionId = event.executionId;
    const incrementalExportWindowSizeInMinutes = event.incrementalExportWindowSizeInMinutes;
    const lastExportTime = new Date(event.lastExportTime);
    const resultDate = new Date(lastExportTime.getTime() + (incrementalExportWindowSizeInMinutes * 60000));

    const currentDateTime = new Date();
    const timeDiffFromLastExportToNow = currentDateTime - lastExportTime;
    const timeDiffFromLastExportToNowInMinutes = timeDiffFromLastExportToNow / (1000 * 60);
    const timeDiffFromLastExportToNowInMinutesInBlocks = timeDiffFromLastExportToNowInMinutes/incrementalExportWindowSizeInMinutes;
    const incrementalBlocksBehind = Math.floor(timeDiffFromLastExportToNowInMinutesInBlocks) - 1; // the current window is always discarded

    console.log(`Id:${executionId} - Current time:${currentDateTime}`);
    console.log(`Id:${executionId} - Time Diff:${timeDiffFromLastExportToNow}`);
    console.log(`Id:${executionId} - Time Diff in Mins:${timeDiffFromLastExportToNowInMinutes}`);
    console.log(`Id:${executionId} - Time Diff in Mins in blocks:${timeDiffFromLastExportToNowInMinutesInBlocks}`);
    console.log(`Id:${executionId} - Incremental blocks behind:${incrementalBlocksBehind}`);

    const tableArn = event.tableArn;
    const bucket = event.bucket;
    const bucketOwner = event.bucketOwner;
    const bucketPrefix = event.bucketPrefix;
    const exportFormat = event.exportFormat;
    const exportViewType = event.exportViewType;

    let prefixCmd = '';
    if (bucketPrefix !== '')
    {
        prefixCmd = ` --s3-prefix ${bucketPrefix}`;
    }

    const remedy = `aws dynamodb export-table-to-point-in-time --table-arn ${tableArn} --s3-bucket-owner ${bucketOwner} --s3-bucket ${bucket}${prefixCmd} --export-format ${exportFormat} --export-type INCREMENTAL_EXPORT --incremental-export-specification ExportFromTime=${Math.floor(lastExportTime/1000)},ExportToTime=${Math.floor(resultDate/1000)},ExportViewType=${exportViewType}`;

    const response = {
        statusCode: 200,
        body: {
            "lastExportTime": lastExportTime,
            "incrementalExportWindowSizeInMinutes": incrementalExportWindowSizeInMinutes,
            "durationAddedStartTime": resultDate,
            "incrementalBlocksBehind": incrementalBlocksBehind,
            "remedy": remedy
        }
    };

    return response;
};
