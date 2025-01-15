import logging
import time

from gridappsd import GridAPPSD
from gridappsd.topics import field_input_topic

logging.basicConfig(level=logging.DEBUG)
_log = logging.getLogger(__name__)


gapps = GridAPPSD()

assert gapps.connected

# # Note we are sending the function not executing the function in the second parameter
topic = field_input_topic()
# gapps.subscribe(topic, on_message_callback)
# # gapps.subscribe('/topic/goss.gridappsd.field.input', on_message_callback)

# gapps.send(topic, "A message about subscription lalalalalal")

# gapps.send(topic, {"key": "value =======lal"})  # or use key-value pair as message

# example_cim_message
cim_message = {
    "command": "update",
    "input": {
        "simulation_id": "123456",
        "message": {
            "timestamp": 1357048800,
            "difference_mrid": "123a456b-789c-012d-345e-678f901a235c",
            "reverse_differences": [
                {
                    "object": "61A547FB-9F68-5635-BB4C-F7F537FD824E",
                    "attribute": "ShuntCompensator.sections",
                    "value": 1,
                },
                {
                    "object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA",
                    "attribute": "ShuntCompensator.sections",
                    "value": 0,
                },
            ],
            "forward_differences": [
                {
                    "object": "61A547FB-9F68-5635-BB4C-F7F537FD824E",
                    "attribute": "ShuntCompensator.sections",
                    "value": 0,
                },
                {
                    "object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA",
                    "attribute": "ShuntCompensator.sections",
                    "value": 1,
                },
            ],
        },
    },
}

# print(cim_message["input"]["message"]["timestamp"])  # 1357048800
# print(
#     cim_message["input"]["message"]["forward_differences"]
# )  # [{'object': '61A547FB-9F68-5635-BB4C-F7F537FD824E', 'attribute': 'ShuntCompensator.sections', 'value': 0}, {'object': 'E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA', 'attribute': 'ShuntCompensator.sections', 'value': 1}]
gapps.send(topic, cim_message)
