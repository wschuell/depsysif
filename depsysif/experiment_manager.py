from .database import Database
from .simulations import Simulation

import json
import logging

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)

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
		if not existing and force_create, creating it, otherwise throwing error
		'''
		if not force_create:
			pass
			#detect if exists using list_simulations, returns sim_id


	def list_simulations(self,failing_project,snapshot_id=None,snapshot_time=None,full_network=False,max_size=None,**sim_cfg):
		'''
		Lists existing simulations with the corresponding parameters.
		Returns empty list if none exist, (id,executed_bool), ordered by execution status and then id
		Limits the output to max_size elements if the parameter is not None.
		'''
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=False)
		# throws an error if snapshot does not exist, could catch it and return []
		if self.db.db_type == 'postgres':
			self.db.cursor.execute(''' SELECT id, executed FROM simulations
				WHERE snapshot_id = %s
					AND failing_project = %s
					AND sim_cfg = %s
				ORDER BY executed,id
				LIMIT %s 
				;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=False),max_size))
		else:
			if max_size is None:
				self.db.cursor.execute(''' SELECT id, executed FROM simulations
					WHERE snapshot_id = ?
						AND failing_project = ?
					AND sim_cfg = ?
					ORDER BY executed,id
					;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=False)))
			else:
				self.db.cursor.execute(''' SELECT id, executed FROM simulations
					WHERE snapshot_id = ?
						AND failing_project = ?
					AND sim_cfg = ?
					ORDER BY executed,id
					LIMIT ?
					;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=False),max_size))

		return list(self.db.cursor.fetchall())


	def run_simulations(self,failing_project=None,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,**sim_cfg):
		'''
		checking existing simulations, creating new ones if necessary, executing the ones that are not executed yet
		'''
		network = None
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=True)
		if failing_project is None:
			logger.info('Running simulations for all possible projects as source of failure')
			for p_id in self.db.get_nodes(snapshot_id=snapid):
				self.run_simulations(failing_project=p_id,snapshot_id=snapid,nb_sim=nb_sim,**sim_cfg)
		else:
			sim_list = self.list_simulations(failing_project=failing_project,snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
			for sim_id,exec_status in sim_list:
				if not exec_status:
					if network is None:
						network = self.db.get_network(snapshot_id=snapid)
					self.run_single_simulation(simulation_id=sim_id,network=network,snapshot_id=snapid)
			if len(sim_list) < nb_sim:
				for _ in range(nb_sim-len(sim_list)):
					if network is None:
						network = self.db.get_network(snapshot_id=snapid)
					sim = Simulation(network=network,snapshot_id=snapid,failing_project=failing_project,**sim_cfg)
					sim.run()
					self.db.register_simulation(sim)


	def run_single_simulation(self,simulation_id,network=None):
		'''
		Runs a single simulation, used in run_simulations
		'''

		if self.db.db_type == 'postgres':
			self.db.cursor.execute('SELECT sim_cfg,snapshot_id,failing_project,random_seed FROM simulations WHERE id=%s;',(simulation_id,))
		else:
			self.db.cursor.execute('SELECT sim_cfg,snapshot_id,failing_project,random_seed FROM simulations WHERE id=?;',(simulation_id,))
		
		if network is None:
			network = self.db.get_network(snapshot_id=snapshot_id)
		sim_cfg,snapshot_id,failing,random_seed = self.db.cursor.fetchone()
		sim = Simulation(network=network,snapshot_id=snapshot_id,failing_project=failing,random_seed=random_seed,**sim_cfg)
		sim.run()
		self.db.register_simulation(sim)


	def get_results(self,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,**sim_cfg):
		'''
		Batch getting the results of the simulations.
		Returns a vector of IDs + a sparse binary matrix
		Alternatively output could be a pandas dataframe?
		'''
		self.run_simulations(snapshot_id=snapshot_id,snapshot_time=snapshot_time,nb_sim=nb_sim,full_network=full_network,**sim_cfg)
		#list sim_ids
		# efficient query
		#
