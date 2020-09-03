from .database import Database
from .simulations import Simulation

class ExperimentManager(object):
	'''
	This class is an extension over the database class.
	All its methods could in a way be packaged into the Database class,
	but gathering them here makes things clearer by separating storage-related part and simulation management part
	'''

	def __init__(self,db=None,**kwargs):
		if db is not None:
			self.db = db
		else:
			self.db = Database(**kwargs)

	def list_snapshots(self):
		'''
		Listing snapshots, ordered by timestamp
		'''
		self.db.cursor.execute('SELECT id,created_at,name,full_network ORDER BY created_at,full_network;')
		return self.db.cursor.fetchall()

	def get_simulation(self,snapshot_id,failing_project,random_seed=None,force_create=False,executed=None,**sim_cfg):
		'''
		Retrieves or creates a simulation, and returns the corresponding simulation object
		'''
		if not force_create:
			pass
			#detect if exists using list_simulations, returns sim_id


	def list_simulations(self,snapshot_id,failing_project,max_size=None,**sim_cfg):
		'''
		Lists existing simulations with the corresponding parameters.
		Returns empty list if none exist.
		Limits the output to max_size elements if the parameter is not None.
		'''
		pass


	def run_simulations(self,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,**sim_cfg):
		'''
		checking existing simulations, creating new ones if necessary, executing the ones that are not executed yet
		'''
		pass

	def get_results(self,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,**sim_cfg):
		'''
		Batch getting the results of the simulations.
		Returns a vector of IDs + a sparse binary matrix
		'''
		self.run_simulations(snapshot_id=snapshot_id,snapshot_time=snapshot_time,nb_sim=nb_sim,full_network=full_network,**sim_cfg)

