const {S3} = require('aws-sdk');
const {stat, open} = require('fs').promises;
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config()
console.log(process.env)

const accessKey = process.env.CF_ACCESS_KEY
const secretAccessKey = process.env.CF_SECRET_ACCESS_KEY

const key = process.env.CF_OBJECT_KEY;
const bucket = process.env.CF_BUCKET_NAME;
console.log(key)
const partSize = 5242880;
const objectSize = 24262281;
const failureRatePercentageOnUploadEvent = 30;

const options = {
    accessKeyId: accessKey,
    secretAccessKey: secretAccessKey,
    endpoint: `https://yzqkzfkyflrt.compat.objectstorage.ca-toronto-1.oraclecloud.com`,
    signatureVersion: 'v4',
    s3ForcePathStyle: true,
    region: 'auto',
};


async function doIt() {

    try {
        const s3 = new S3(options);


        const createMultipartUploadResponse = await s3.createMultipartUpload({
            Bucket: bucket,
            Key: key
        }).promise();


        const listParts = [];
        let index = 0;
        do {

            try{
                let start = index * partSize;
                console.log('start upload part ' + (index + 1));

                const uploadPartResponse = await s3.getSignedUrlPromise('uploadPart', {
                    Bucket: bucket,
                    Key: key,
                    UploadId: createMultipartUploadResponse.UploadId,
                    PartNumber: index + 1
                });

                let end = Math.min(objectSize, (index + 1) * partSize);

                const controller = axios.CancelToken.source();

                const response = await axios.put(uploadPartResponse, crypto.randomBytes(end - start), {
                    cancelToken: controller.token,
                    onUploadProgress: function (e) {
                        if(Math.random() * 100 > (100 - failureRatePercentageOnUploadEvent)){
                            controller.cancel();
                        }
                    },
                    headers: {
                        'Content-length': end - start,
                    }
                });


                let header = response.headers['etag'];

                console.log('part '+(index+1) + 'part success, etag: '+header)

                listParts.push({
                    ETag: header,
                    PartNumber: index + 1,
                })

                index += 1;
            }catch (e){
                console.log('catch error, without incrementing index so part is going to be restarted');
            }

        } while (index * partSize < objectSize)


        let params = {
            Bucket: bucket,
            Key: key,
            UploadId: createMultipartUploadResponse.UploadId,
            MultipartUpload: {
                Parts: listParts
            }
        };

        console.log('completing with the following params : ', JSON.stringify(params))

        await s3.completeMultipartUpload(params).promise();

    } catch (e) {
        if (e.response) {
            console.log({
                errorData: e.response?.data,
                errorStatus: e.response?.status,
                errorHeaders: e.response?.headers,
            })
        } else {
            console.error(e)
        }
    }

}

doIt();


// "AWS_ACCESS_KEY_ID=$SCW_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$SCW_SECRET_KEY AWS_REGION=${var.region} BUCKET=${scaleway_object_bucket.bucket[count.index].name} node lifecycle/put-pucket-lifcycle.js"
