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

# Note message format: dict[str, dict[str, bool]], {RTU-id: {register-id: True/False}},
#     e.g., {"RTU1": {"ufls_59.1": True}}
gapps.send(topic, {"RTU1": {"ufls_59.1": True}})
