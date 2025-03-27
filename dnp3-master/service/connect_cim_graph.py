from cimgraph.databases import ConnectionParameters, GridappsdConnection, BlazegraphConnection
from cimgraph.models import BusBranchModel, DistributedArea
import cimgraph.utils as utils
import cimgraph.data_profile.cimhub_ufls as cim
import json
from uuid import UUID
from gridappsd import GridAPPSD


gapps = GridAPPSD(stomp_address="54.245.16.81", stomp_port=61616)
# gapps.connect()  # Note: need to disable this

assert gapps.connected

CIM_PROFILE = 'cimhub_ufls' # using custom ufls profile
IEC61970_301 = 8 # using urn:uuid instead of rdf:id

GAPPS_HOST = 'localhost'
GAPPS_PORT = '61613'
BG_PORT = '8889'

BG_PARAMS = ConnectionParameters(
    url = f"http://{GAPPS_HOST}:{BG_PORT}/bigdata/namespace/kb/sparql",
    cim_profile=CIM_PROFILE,
    iec61970_301=IEC61970_301
    )

GAD_PARAMS = ConnectionParameters(
    host = GAPPS_HOST,
    port = GAPPS_PORT,
    cim_profile=CIM_PROFILE,
    iec61970_301=IEC61970_301
    )

# mRID of DistributionArea for VEC
DIST_AREA_ID = "067248c5-53c4-43e8-8720-f0c605c05b84"


database = GridappsdConnection(GAD_PARAMS)
distribution_area = cim.DistributionArea(mRID=DIST_AREA_ID)
network = BusBranchModel(connection=database, container=distribution_area, distributed=False)
# Build CIM-Graph graph network from VEC service territory to UFLS relay
network.get_all_edges(cim.DistributionArea)
network.get_all_edges(cim.Substation)
network.get_all_edges(cim.Feeder)
network.get_all_edges(cim.Breaker)
network.get_all_edges(cim.UnderFrequencyProtectionFunctionBlock)
network.get_all_edges(cim.ProtectionEquipment)
network.get_all_edges(cim.Discrete)

print(f"queried for {len(network.graph[cim.Substation])} substations")
print(f"queried for {len(network.graph[cim.Feeder])} feeders")
print(f"queried for {len(network.graph[cim.UnderFrequencyProtectionFunctionBlock])} relay elements")