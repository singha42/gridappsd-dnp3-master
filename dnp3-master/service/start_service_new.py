# Copyright (c) 2019 Alliance for Sustainable Energy, LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#########################Edited/Prepared this code by Dr.Venkateswara Reddy Motakatla, NREL (VenkateswaraReddy.Motakatla@nrel.gov)
######################### PNNL Technical support: Poorva, and Alka
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass


# from dnp3_python.dnp3station.master import MyMaster
from dnp3_python.dnp3station.master_new import MyMasterNew
from gridappsd import GridAPPSD
from gridappsd.topics import field_input_topic
from pydnp3 import opendnp3


# Setup argparse
parser = argparse.ArgumentParser()
parser.add_argument(
    "outstation_names", nargs="+", help="name of dnp3 outstation", type=str
)
parser.add_argument(
    "--config_path",
    help="Path of the folder containing input configuration files",
    default="config",
    type=str,
)

# Parse arguments globally
args = parser.parse_args()

# Now args is available globally within the script
outstation_names = args.outstation_names
config_path = args.config_path


@dataclass
class RTUConfig:
    """
    A configuration class for RTU (Remote Terminal Unit) devices.

    This class holds all the necessary configuration details for an RTU,
    allowing easy access and management of its properties. It also includes
    a static method to load configurations from a JSON file, simplifying
    initialization and reducing potential setup errors.

    Attributes:
        name (str): The name of the RTU.
        conversion_type (str): Type of conversion used by the RTU.
        CIM_object (str): CIM object identifier.
        port (int): Network port number the RTU uses for communication.
        ip (str): IP address of the RTU.
        desc (str): A brief description of the RTU.
        link_local_addr (int): Local network address used for linking.
        link_remote_addr (int): Remote network address used for linking.
    """

    name: str
    conversion_type: str
    CIM_object: str
    port: int
    outstation_ip: str
    desc: str
    link_local_addr: int
    link_remote_addr: int

    @staticmethod
    def load_json_config(file_path: str, rtu_key: str) -> RTUConfig:
        """
        Load RTU configuration from a JSON file.

        :param file_path: Path to the JSON file containing the RTU configurations.
        :param rtu_key: Key of the RTU entry to be loaded from the JSON file.
        :return: An instance of RTUConfig initialized with data from the JSON file.
        """
        try:
            with open(file_path, "r") as file:
                data = json.load(file)
                rtu_data = data[rtu_key]
                return RTUConfig(
                    name=rtu_data["name"],
                    conversion_type=rtu_data["conversion_type"],
                    CIM_object=rtu_data["CIM object"],
                    port=int(rtu_data["port"]),
                    outstation_ip=rtu_data["outstation_ip"],
                    desc=rtu_data["desc"],
                    link_local_addr=int(rtu_data["link_local_addr"]),
                    link_remote_addr=int(rtu_data["link_remote_addr"]),
                )

        except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
            raise Exception(f"Error loading RTU configuration: {e}")


myCIMProcessor = None

logging.basicConfig(level=logging.DEBUG)
_log = logging.getLogger(__name__)


def on_message_control_outstation_binaryOutput(headers, message):
    """
    A callback function when the receiving message from a subscribed topic.
    message: a cim-difference message
    """
    _log.info(f"==== on_message triggered. {headers = }, {message = }")
    forward_differences = message["input"]["message"]["forward_differences"]
    with open(
        os.path.join(config_path, "mrid_object_outstation_index_dict_receive.json")
    ) as f:
        register_to_db_index_receive = json.load(f)
    # Note: filtered index 100, 101
    register_to_db_index_receive_updated = {
        k: v for (k, v) in register_to_db_index_receive.items() if v in [100, 101]
    }
    register_to_db_index_receive = register_to_db_index_receive_updated
    rtu: str = outstation_names[0]  # Note: assume only one RTU device.
    for command in forward_differences:
        master_app: MyMasterNew = master_apps[rtu]
        mrid = command["object"]
        if (
            mrid not in register_to_db_index_receive.keys()
        ):  # Note: if not in mrid, skip
            continue
        val_to_set = True if command["value"] == True else False
        index = register_to_db_index_receive[mrid]

        if val_to_set:
            control_state = opendnp3.ControlCode.LATCH_ON
        else:
            control_state = opendnp3.ControlCode.LATCH_OFF
        master_app.send_direct_operate_command(
            opendnp3.ControlRelayOutputBlock(control_state),
            index,
        )
        _log.info(
            f"==== Executed send_direct_operate_command, {control_state = }, {index = }"
        )


def _construct_cim_full_msg(
    db_data,
    cim_full_msg=None,
) -> dict:
    """
    Constructs a CIM message with measurement data.
    Args:
        cim_full_msg (dict, optional): Initial structure of the CIM message. Defaults to None.
    Returns:
        dict: A dictionary containing the CIM message structured with measurements.
    """

    if cim_full_msg is None:
        cim_full_msg = {"message": {}}

    # Load measurement definitions from a JSON file.
    with open(
        os.path.join(config_path, "mrid_object_outstation_index_dict_send.json")
    ) as f:
        register_to_db_index_send = json.load(f)
    register_to_db_index_send_updated = {
        k: v for (k, v) in register_to_db_index_send.items() if v in [100, 101]
    }
    register_to_db_index_send = register_to_db_index_send_updated

    # Populate the CIM message with the latest measurements.
    for k, v in register_to_db_index_send.items():
        mr_id = k
        index = v
        # Note: For this project, always read state from Binary (rather than BinaryOutputStatus), only take index in [100, 101]
        if index not in db_data["Binary"].keys():
            continue
        try:
            polled_status: int = db_data["Binary"][index]
        except Exception as e:
            _log.warning(
                f"Poll status failed. {e = }, {index = }, {db_data['Binary'] = }"
            )
            polled_status = None
        value = True if polled_status == True else False  # Note: db_data can be None
        current_time = str(int(time.time()))
        cim_full_msg["message"]["timestamp"] = current_time
        if not cim_full_msg["message"].get("measurements"):
            cim_full_msg["message"]["measurements"] = {}
        measurements_content = {mr_id: {"measurement_mrid": mr_id, "value": value}}
        cim_full_msg["message"]["measurements"].update(measurements_content)

    return cim_full_msg


def run_master():
    # init master station(s)
    config_path = args.config_path
    # Make it global accessible to use in gridapps-d on_message callback workflow
    global master_apps
    master_apps = {}
    for outstaion_name in outstation_names:
        config: RTUConfig = RTUConfig.load_json_config(
            os.path.join(config_path, "device_ip_port_config.json"), outstaion_name
        )
        master_app = MyMasterNew(
            outstation_ip=config.outstation_ip,
            port=config.port,
            master_id=config.link_local_addr,
            outstation_id=config.link_remote_addr,
        )
        master_app.start()
        master_apps[outstaion_name] = master_app

    # subscribe to gridapps-d topic to receive control command
    topic = field_input_topic()
    gapps.subscribe(topic, on_message_control_outstation_binaryOutput)
    # loop to poll data from outstation, construct a cim-message and send it
    while True:
        for outstation_name, master_app in master_apps.items():
            # Send scan request and process data
            master_app.send_scan_all_request()
            db_data = master_app.soe_handler.db
            _log.debug(f"==== Polled from outstation. {db_data =}")

            cim_full_msg = _construct_cim_full_msg(db_data)

            # Sending the cim-message
            topic = "/topic/goss.gridappsd.field.dnp3"
            if cim_full_msg:
                gapps.send(topic, json.dumps(cim_full_msg))
                _log.info(f"==== Sent to {topic = }, {cim_full_msg = }")
            else:
                _log.info(f"==== Invalid {cim_full_msg = }")
        time.sleep(30)  # Interval to poll data from outstation.


if __name__ == "__main__":
    gapps = GridAPPSD(stomp_address="54.245.16.81", stomp_port=61616)
    gapps.connect()

    time.sleep(1)

    run_master()
