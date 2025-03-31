import os
from gridappsd import GridAPPSD, topics
import time


os.environ["GRIDAPPSD_USER"]="artemis"
os.environ["GRIDAPPSD_PASSWORD"]="artemis"
gapps = GridAPPSD(address=("54.245.16.81",61616))

request = {
        "requestType": "QUERY_MODEL_INFO",
        "resultFormat": "JSON"
}
response = gapps.get_response('goss.gridappsd.process.request.data.powergridmodel',request)
print(response)
def on_message(header, message):
    print(message)

#gapps.subscribe(topics.field_output_topic(), on_message)
#gapps.subscribe('/topic/goss.gridappsd.simulation.meas_aggr.output', on_message)



topic= 'goss.gridappsd.request.data.cimtopology'




while True:
    try:
        time.sleep(0.1)
    except KeyboardInterrupt:
        print("Exiting sample")
