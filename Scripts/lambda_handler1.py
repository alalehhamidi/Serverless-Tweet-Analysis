import json
import boto3


def lambda_handler(event, context):
    shard = event['shard']

    REGION = 'us-east-1'
    ACCESS_KEY_ID = 'AKIA5JXMNHVWKFDDDXHR'
    SECRET_ACCESS_KEY = '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'
    BUCKET_NAME = 'aws-logs-914250087788-us-east-1'
    key = 'RawFiles/input.json'
    resultPath = '/tmp/result.json'
    s3c = boto3.client(
        's3',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )
    s3c.download_file(BUCKET_NAME, key, '/tmp/input.json')

    comprehend = boto3.client('comprehend')

    result = {}

    with open('/tmp/input.json') as json_file:
        data = json.load(json_file)
        i = 1
        for item in data:
            response = comprehend.detect_sentiment(Text=data[str(i)]['text'], LanguageCode='en')
            a = response['Sentiment']
            b = a.lower()
            b = b.title()
            result[i] = {'id': data[str(i)]['id'], 'date': data[str(i)]['date'], 'text': data[str(i)]['text'],
                         'sentiment': a, 'score': response['SentimentScore'][b]}
            i = i + 1

    with open(resultPath, 'w') as jsonFile:
        jsonFile.write(json.dumps(result, indent=4))

    s3c.upload_file(resultPath, BUCKET_NAME, 'ProcessedFiles/Result.json')
    clientl = boto3.client('lambda',
                           region_name='us-east-1',
                           aws_access_key_id='AKIA5JXMNHVWKFDDDXHR',
                           aws_secret_access_key='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'
                           )

    InputForInvoker = {'shard': shard}
    response = clientl.invoke(FunctionName='arn:aws:lambda:us-east-1:914250087788:function:rsd_handler',
                              InvocationType='RequestResponse',
                              Payload=json.dumps(InputForInvoker)
                              )
    resjson = json.load(response['Payload'])

    return {
        'shard': shard
    }
