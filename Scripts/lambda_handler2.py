import psycopg2
import json
import boto3

def lambda_handler(event, context):
    
	shard = int(event['shard'])
	print(type(shard))


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
	s3c.download_file(BUCKET_NAME, 'ProcessedFiles/Result.json', '/tmp/Result.json')





	conn = psycopg2.connect(database = 'postgres',
								user = 'alale4361',
								password = '*****',
								port = '5432',
								host = 'tp3.cvxd7xbegzqp.us-east-1.rds.amazonaws.com'
								)
	cur = conn.cursor()

	#Delete tables'content
	tableName = ['first.older', 'first.even', 'second.newer', 'second.odd']
	for name in tableName:
		cur.execute("DELETE FROM "+ name)
		conn.commit()

	with open('/tmp/Result.json') as json_file:
		res = json.load(json_file)

	if shard ==1:
		i=1
		for item in res:
			dt = res[str(i)]['date'].split("/")
			month = dt[0]
			day = dt[1]
			year = dt[2][0:4]
			time = year+'-'+month+'-'+day
			#year = res[str(i)]['date'][4:8]


			if int(year)<2020:
				cur.execute("INSERT INTO first.older(id, date, text, sentiment, score) VALUES(%s, %s, %s, %s, %s)", (res[str(i)]['id'], time, res[str(i)]['text'], res[str(i)]['sentiment'], res[str(i)]['score']))
				conn.commit()
			else:
				cur.execute("INSERT INTO second.newer(id, date, text, sentiment, score) VALUES(%s, %s, %s, %s, %s)",
							(res[str(i)]['id'], time, res[str(i)]['text'], res[str(i)]['sentiment'], res[str(i)]['score']))
				conn.commit()

			i = i + 1

	else:
		i=1
		for item in res:
			dt = res[str(i)]['date'].split("/")
			month = dt[0]
			day = dt[1]
			year = dt[2][0:4]
			time = year + '-' + month + '-' + day
			k = res[str(i)]['id'][0:6]
			if  (int(k) % 2) == 0:
				cur.execute("INSERT INTO first.even(id, date, text, sentiment, score) VALUES(%s, %s, %s, %s, %s)",
							(res[str(i)]['id'], time, res[str(i)]['text'], res[str(i)]['sentiment'], res[str(i)]['score']))
				conn.commit()

			else:
				cur.execute("INSERT INTO second.odd(id, date, text, sentiment, score) VALUES(%s, %s, %s, %s, %s)",
							(res[str(i)]['id'], time, res[str(i)]['text'], res[str(i)]['sentiment'], res[str(i)]['score']))
				conn.commit()
			i= i + 1




	return {'shard': shard}
