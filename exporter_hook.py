from flask import Flask
import cloudwatch_metrics
from datetime import datetime
import sys

app = Flask(__name__)

@app.route("/rds/metrics", methods=['GET'])
def get_metrics():
   try:
      start_time = datetime.now()
      data = cloudwatch_metrics.rds_metric()
      print(datetime.now() - start_time)
      if data is None:
         data = ''
   except Exception as e:
      print('get_metrics: ', e)

   return data


if __name__ == '__main__':
   # app.debug = True
   app.run(host='0.0.0.0', port='9120', threaded=True)