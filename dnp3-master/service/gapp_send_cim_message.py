import logging
import time
import argparse
import json

from gridappsd import GridAPPSD
from gridappsd.topics import field_input_topic

logging.basicConfig(level=logging.DEBUG)
_log = logging.getLogger(__name__)

def main():
    # gapps = GridAPPSD()
    gapps = GridAPPSD(stomp_address="54.245.16.81", stomp_port=61616)
    # gapps.connect()  # Note: need to disable this

    assert gapps.connected

    # # Note we are sending the function not executing the function in the second parameter
    topic = field_input_topic()
    # gapps.subscribe(topic, on_message_callback)
    # # gapps.subscribe('/topic/goss.gridappsd.field.input', on_message_callback)

    # gapps.send(topic, "A message about subscription lalalalalal")

    # gapps.send(topic, {"key": "value =======lal"})  # or use key-value pair as message

    # example_cim_message
    # print(f"{args.val = }")
    relay_state = True if args.val == 1 else False
    cim_message = {
      "command": "update",
      "input": {
        "message": {
          "timestamp": time.time(),  # e.g.,1736530734.662945
          "difference_mrid": "ccbc9b44-c6b6-4b89-b876-f9472867a2ca",
          "reverse_differences": [
            {
              "object": "5e6dfa84-705b-495a-8497-1c230541b636",
              "attribute": "FunctionBlock.enabled",
              "value": True
            },
            {
              "object": "c3a4f467-fe35-4dce-b0b7-ab1f2f0a4c5d",
              "attribute": "FunctionBlock.enabled",
              "value": False
            }
          ],
          "forward_differences": [
            {
              "object": "5e6dfa84-705b-495a-8497-1c230541b636",
              "attribute": "FunctionBlock.enabled",
              "value": relay_state
            },
            {
              "object": "c3a4f467-fe35-4dce-b0b7-ab1f2f0a4c5d",
              "attribute": "FunctionBlock.enabled",
              "value": True
            }
          ]
        },
        "simulation_id": "field_data"
      }
    }

    print(json.dumps(cim_message, indent=4))
    # print(cim_message["input"]["message"]["timestamp"])  # 1357048800
    # print(
    #     cim_message["input"]["message"]["forward_differences"]
    # )  # [{'object': '61A547FB-9F68-5635-BB4C-F7F537FD824E', 'attribute': 'ShuntCompensator.sections', 'value': 0}, {'object': 'E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA', 'attribute': 'ShuntCompensator.sections', 'value': 1}]
    gapps.send(topic, cim_message)


# Setup argparse
parser = argparse.ArgumentParser()
parser.add_argument(
    "--val", help="set relay status to True/False <1/0>", type=int
)


# Parse arguments globally
args = parser.parse_args()


if __name__ == "__main__":
    main()