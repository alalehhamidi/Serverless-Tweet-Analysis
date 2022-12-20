from flask import Flask, request, redirect, url_for, render_template, send_from_directory
from werkzeug.utils import secure_filename
import os
import boto3
import json
import psycopg2
import time

currentDirectory = os.getcwd()

# To download file from S3
def Dwnld_S3(Obj_name, ecp_name):
    REGION = 'us-east-1'
    ACCESS_KEY_ID = 'Put_Your_ACCESS_KEY_ID'
    SECRET_ACCESS_KEY = 'Put_SECRET_ACCESS_KEY'
    BUCKET_NAME = 'aws-logs-914250087788-us-east-1'
    s3c = boto3.client(
        's3',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )
    s3c.download_file(BUCKET_NAME, Obj_name, ecp_name)


# To upload to S3
def Upld_S3(fPath, objPath):
    REGION = 'us-east-1'
    ACCESS_KEY_ID = 'Put_Your_ACCESS_KEY_ID'
    SECRET_ACCESS_KEY = 'Put_SECRET_ACCESS_KEY'
    BUCKET_NAME = 'aws-logs-914250087788-us-east-1'
    s3c = boto3.client(
        's3',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )

    s3c.upload_file(fPath, BUCKET_NAME, objPath)



Dwnld_S3('FlaskInterface.html', 'FlaskInterface.html')
app = Flask(__name__, template_folder=currentDirectory)

ALLOWED_EXTENSIONS = {'json'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(os.getcwd(), filename, as_attachment=True)


@app.route('/sharding', methods=['GET', 'POST'])
def shFunc():
    s = '1'
    start_time = time.time()
    if request.method == 'POST':
        if 'file' not in request.files:
            return ('No file attached in request')

        file = request.files['file']
        if file.filename == '':
            return ('No file selected')

        if not allowed_file(file.filename):
            return ('File type is not json')

        if file and allowed_file(file.filename):
            # Save file in working directory and upload it to S3
            filename = secure_filename(file.filename)
            filepath = currentDirectory + '/' + filename
            file.save(filepath)
            Upld_S3(filename, 'RawFiles/input.json')

            # Invoke first lambda function
            clientl = boto3.client('lambda',
                                   region_name='us-east-1',
                                   aws_access_key_id='Put_Your_ACCESS_KEY_ID',
                                   aws_secret_access_key='Put_SECRET_ACCESS_KEY'
                                   )
            InputForInvoker = {'shard': s}
            response = clientl.invoke(
                FunctionName='arn:aws:lambda:us-east-1:914250087788:function:lambda_sentiment_handler',
                InvocationType='RequestResponse',
                Payload=json.dumps(InputForInvoker)
            )
            resjson = json.load(response['Payload'])


            # Sending query to RDS
            conn = psycopg2.connect(database='postgres',
                                    user='alale4361',
                                    password='***',
                                    port='5432',
                                    host='tp3.cvxd7xbegzqp.us-east-1.rds.amazonaws.com'
                                    )

            cur = conn.cursor()
            cur.execute("""
                        SELECT id, sentiment, score FROM first.older 
                        UNION ALL 
                        SELECT id, sentiment, score FROM second.newer
                        """)

            final = cur.fetchall()
            print()
            print('affected rows:', str(cur.rowcount))
            print()
            # conn.close()

            finalName = '/Result-' + filename
            finalPath = currentDirectory + finalName
            with open(finalPath, 'w') as jsonFile:
                jsonFile.write(json.dumps(final, indent=4))
            print("Sharding Execution Time: {0:2f} seconds".format(time.time() - start_time))
            print()
            print()
            return redirect(url_for('uploaded_file', filename=finalName))



    return render_template('FlaskInterface.html')



@app.route('/proxy', methods=['GET', 'POST'])
def proFunc():
    s = '0'
    start_time = time.time()
    if request.method == 'POST':
        if 'file' not in request.files:
            return ('No file attached in request')

        file = request.files['file']
        if file.filename == '':
            return ('No file selected')

        if not allowed_file(file.filename):
            return ('File type is not json')

        if file and allowed_file(file.filename):
            # Save file in working directory and upload it to S3
            filename = secure_filename(file.filename)
            filepath = currentDirectory + '/' + filename
            file.save(filepath)
            Upld_S3(filename, 'RawFiles/input.json')

            # Invoke first lambda function
            clientl = boto3.client('lambda',
                                   region_name='us-east-1',
                                   aws_access_key_id='Put_Your_ACCESS_KEY_ID',
                                   aws_secret_access_key='Put_SECRET_ACCESS_KEY'
                                   )
            InputForInvoker = {'shard': s}
            response = clientl.invoke(
                FunctionName='arn:aws:lambda:us-east-1:914250087788:function:lambda_sentiment_handler',
                InvocationType='RequestResponse',
                Payload=json.dumps(InputForInvoker)
            )
            resjson = json.load(response['Payload'])


            # Sending query to RDS
            conn = psycopg2.connect(database='postgres',
                                    user='alale4361',
                                    password='*****',
                                    port='5432',
                                    host='tp3.cvxd7xbegzqp.us-east-1.rds.amazonaws.com'
                                    )

            cur = conn.cursor()
            cur.execute("""
                        SELECT id, sentiment, score FROM first.even 
                        UNION ALL 
                        SELECT id, sentiment, score FROM second.odd
                        """)

            final = cur.fetchall()
            print()
            print('affected rows:', str(cur.rowcount))
            print()

            # conn.close()

            finalName = '/Result-' + filename
            finalPath = currentDirectory + finalName
            with open(finalPath, 'w') as jsonFile:
                jsonFile.write(json.dumps(final, indent=4))
            print("Proxy Execution Time: {0:2f} seconds".format(time.time() - start_time))
            print()
            print()
            return redirect(url_for('uploaded_file', filename=finalName))



    return render_template('FlaskInterface.html')





app.config['MAX_CONTENT_LENGTH'] = 400 * 1024 * 1024




app.run(host='0.0.0.0', port=5000)