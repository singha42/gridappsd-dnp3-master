import os

from cimgraph.databases import ConnectionParameters, GridappsdConnection
from cimgraph.models import BusBranchModel, DistributedArea
import cimgraph.data_profile.cimhub_ufls as cim

os.environ['CIMG_CIM_PROFILE'] = 'cimhub_ufls'
os.environ['CIMG_URL'] = 'http://localhost:8889/bigdata/namespace/kb/sparql'
#os.environ['CIMG_URL'] = 'http://54.245.16.81:8889/bigdata/namespace/kb/sparql'
os.environ['CIMG_DATABASE'] = 'powergridmodel'
os.environ['CIMG_HOST'] = '54.245.16.81'
os.environ['CIMG_PORT'] = '61616'
os.environ['CIMG_USERNAME'] = 'artemis'
os.environ['CIMG_PASSWORD'] = 'artemis'
os.environ['CIMG_NAMESPACE'] = 'http://iec.ch/TC57/CIM100#'
os.environ['CIMG_IEC61970_301'] = '8'
os.environ['CIMG_USE_UNITS'] = 'False'

DIST_AREA_ID = "067248c5-53c4-43e8-8720-f0c605c05b84"

#class MyGridappsdConnection(GridappsdConnection):
#    
#    def __init__(self):
#        
#        self.connection_params = ConnectionParameters(cim_profile='cimhub_ufls', iec61970_301=8)
#        super().__init__()
        

database = GridappsdConnection()

distribution_area = cim.DistributionArea(mRID=DIST_AREA_ID)
network = BusBranchModel(connection=database, container=distribution_area, distributed=False)
print("network", network)
network.get_all_edges(cim.DistributionArea)
network.get_all_edges(cim.Substation)
network.get_all_edges(cim.Feeder)
network.get_all_edges(cim.Breaker)
network.get_all_edges(cim.UnderFrequencyProtectionFunctionBlock)
network.get_all_edges(cim.ProtectionEquipment)
network.get_all_edges(cim.Discrete)

print(network)
print(f"queried for {len(network.graph[cim.Substation])} substations")
#print(f"queried for {len(network.graph[cim.Feeder])} feeders")
#print(f"queried for {len(network.graph[cim.UnderFrequencyProtectionFunctionBlock])} relay elements")
