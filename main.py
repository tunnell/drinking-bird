# [START gae_python37_app]
from flask import Flask
import pymongo
import json
import datetime


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

@app.route('/')
def main():
    return "Cannot be directly called"

@app.route('/tasks/hourly')
def hourly():
    responses = []
    responses.append(clear_timeout())
    responses.append(clear_no_heartbeat())

    return "\n\n".join(responses)

def get_mongo_url():
    doc = json.load(open('keys.json'))
    url = f"mongodb://{doc['mongoUsername']}:{doc['mongoPassword']}@{doc['mongoUrl']}"
    return url

def clear_no_heartbeat():
    client = pymongo.MongoClient(get_mongo_url())
    collection = client['xenon1t']['processing_queue']

    now = datetime.datetime.utcnow()
    hour = datetime.timedelta(minutes=10)
    result = collection.update_many({'heartBeat' : {'$lt' : now - hour},
                                     'endTime' : None},
                                    {'$set' : {'error' : True,
                                               'error_msg' : 'No heartbeat.  Closed by GCP cron',
                                               'endTime' : now}})
    return f'Queue no heartbeat: Matched {result.matched_count} updated {result.modified_count}'

def clear_timeout():
    client = pymongo.MongoClient(get_mongo_url())
    collection = client['xenon1t']['processing_queue']

    now = datetime.datetime.utcnow()
    hours = datetime.timedelta(hours=24) # 24 from snax!
    result = collection.update_many({'startTime' : {'$lt' : now - hours},
                                     'endTime' : None},
                                    {'$set' : {'error' : True,
                                               'error_msg' : 'Timeout.  Closed by GCP cron',
                                               'endTime' : now}})
    return f'Queue timeout: Matched {result.matched_count} updated {result.modified_count}'



if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]
