from .database import Database
from .simulations import Simulation

import json
import logging
import numpy as np
# from scipy import sparse
import  scipy.sparse

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

	def get_simulation(self,snapshot_id,failing_project,**sim_cfg):
		'''
		Creates a simulation, and returns the corresponding simulation object
		'''
		sim_list = self.list_simulations(failing_project=failing_project,snapshot_id=snapshot_id)
		network = self.db.get_network(snapshot_id=snapshot_id)
		return Simulation(network=network,snapshot_id=snapshot_id,failing_project=failing_project,**sim_cfg)



	def list_simulations(self,failing_project,snapshot_id=None,snapshot_time=None,full_network=False,max_size=None,**sim_cfg):
		'''
		Lists existing simulations with the corresponding parameters.
		Returns empty list if none exist, (id,executed_bool), ordered by execution status and then id
		Limits the output to max_size elements if the parameter is not None.
		'''
		sim_cfg = Simulation.complete_sim_cfg(**sim_cfg)
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=False)
		# throws an error if snapshot does not exist, could catch it and return []
		logger.info('Listing simulations for snapshot_id {}, failing_project id {}'.format(snapid,failing_project))
		if self.db.db_type == 'postgres':
			self.db.cursor.execute(''' SELECT id, executed FROM simulations
				WHERE snapshot_id = %s
					AND failing_project = %s
					AND sim_cfg = %s
				ORDER BY executed,id
				LIMIT %s
				;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=True),max_size))
		else:
			if max_size is None:
				self.db.cursor.execute(''' SELECT id, executed FROM simulations
					WHERE snapshot_id = ?
						AND failing_project = ?
					AND sim_cfg = ?
					ORDER BY executed,id
					;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=True)))
			else:
				self.db.cursor.execute(''' SELECT id, executed FROM simulations
					WHERE snapshot_id = ?
						AND failing_project = ?
					AND sim_cfg = ?
					ORDER BY executed,id
					LIMIT ?
					;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=True),max_size))

		return list(self.db.cursor.fetchall())


	def run_simulations(self,failing_project=None,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,network=None,bootstrap_sim=None,commit=True,limit_ids=None,**sim_cfg):
		'''
		checking existing simulations, creating new ones if necessary, executing the ones that are not executed yet
		'''
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=True)
		if failing_project is None:
			logger.info('Running simulations for all possible projects as source of failure')
			if limit_ids:
				id_list = self.db.get_nodes(snapshot_id=snapid)[:limit_ids]
			else:
				id_list = self.db.get_nodes(snapshot_id=snapid)
			for p_id in id_list:

				if network is None:
					network = self.db.get_network(snapshot_id=snapid)
				if bootstrap_sim is None:
					bootstrap_sim = Simulation(network=network,failing_project=None,snapshot_id=snapid,**sim_cfg)

				self.run_simulations(failing_project=p_id,snapshot_id=snapid,nb_sim=nb_sim,bootstrap_sim=bootstrap_sim,network=network,commit=False,**sim_cfg)
				if commit:
					self.db.connection.commit()
		else:
			sim_list = self.list_simulations(failing_project=failing_project,snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
			for sim_id,exec_status in sim_list:
				if not exec_status:
					if network is None:
						network = self.db.get_network(snapshot_id=snapid)
					if bootstrap_sim is None:
						bootstrap_sim = Simulation(network=network,failing_project=None,snapshot_id=snapid,**sim_cfg)
					self.run_single_simulation(simulation_id=sim_id,network=network,snapshot_id=snapid,bootstrap_sim=bootstrap_sim,commit=commit)
			if len(sim_list) < nb_sim:
				for _ in range(nb_sim-len(sim_list)):
					if network is None:
						network = self.db.get_network(snapshot_id=snapid)

					if bootstrap_sim is None:
						bootstrap_sim = Simulation(network=network,failing_project=None,snapshot_id=snapid,**sim_cfg)
					sim = Simulation(network=network,snapshot_id=snapid,failing_project=failing_project,bootstrap_sim=bootstrap_sim,**sim_cfg)
					sim.run()
					self.db.register_simulation(sim,commit=commit)


	def run_single_simulation(self,simulation_id,network=None,commit=True):
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
		self.db.register_simulation(sim,commit=commit)

	def get_id_vector(self,snapshot_id):
		'''
		returns a vector of the ordered IDs
		[project_id_1 project_id_2 ,... ]
		'''

		return np.asarray(sorted(self.db.get_nodes(snapshot_id=snapshot_id)))

	def get_results(self,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,failing_project=None,result_type='counts',**sim_cfg):
		'''
		Batch getting the results of the simulations.
		Returns a vector of IDs +a vector of source IDs + a sparse binary matrix
		Alternatively output could be a pandas dataframe?
		'''
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=True)
		id_vec = self.get_id_vector(snapshot_id=snapid)
		self.run_simulations(snapshot_id=snapid,nb_sim=nb_sim,failing_project=failing_project,**sim_cfg)

		index_reverse = {n:i for i,n in enumerate(id_vec)}

		if failing_project is None:
			return self.get_results_full(snapshot_id=snapid,nb_sim=100,result_type=result_type,**sim_cfg)
		else:
			sim_list = self.list_simulations(failing_project=failing_project,snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
			if len(sim_list) == 0:
				raise ValueError('No simulations found')
			sim_id_list = sorted([s_id for s_id,exec_status in sim_list])
			reverse_sim_index = {s:i for i,s in enumerate(sim_id_list)}
			#### RAW  returns sparse_mat[project,sim]=np.bool
			if result_type == 'raw':
				if self.db.db_type =='postgres':
					self.db.cursor.execute('''
						SELECT simulation_id,failing FROM simulation_results
							WHERE simulation_id IN %s
							ORDER BY simulation_id,failing
						;''',(tuple(sim_id_list),))
				else:
					self.db.cursor.execute('''
						SELECT simulation_id,failing FROM simulation_results
							WHERE simulation_id IN ({})
							ORDER BY simulation_id,failing
						;'''.format(','.join(['?' for _ in sim_id_list])),sim_id_list)

				results_data = [(index_reverse[fp],reverse_sim_index[s_id],True) for s_id,fp in self.db.cursor.fetchall()]
				results_v = np.asarray([r[2] for r in results_data])
				results_i = np.asarray([r[0] for r in results_data])
				results_j = np.asarray([r[1] for r in results_data])
				results_ijv = (results_v,(results_i,results_j))
				results = scipy.sparse.coo_matrix(results_ijv,shape=(len(id_vec),nb_sim),dtype=np.bool).tocsr()
				return results

			#### COUNTS   returns nparray[project]
			elif result_type == 'counts':
				if self.db.db_type =='postgres':
					self.db.cursor.execute('''
						SELECT COUNT(*),failing FROM simulation_results
							WHERE simulation_id IN %s
							GROUP BY failing
						;''',(tuple(sim_id_list),))
				else:
					self.db.cursor.execute('''
						SELECT COUNT(*),failing FROM simulation_results
							WHERE simulation_id IN ({})
							GROUP BY failing
						;'''.format(','.join(['?' for _ in sim_id_list])),sim_id_list)

				results = np.zeros((len(id_vec),))
				for val,fp in self.db.cursor.fetchall():
					results[index_reverse[fp]] = val
				return results
			#### NB FAILING   returns nparray[sim]
			elif result_type == 'nb_failing':
				if self.db.db_type =='postgres':
					self.db.cursor.execute('''
						SELECT COUNT(*),simulation_id FROM simulation_results
							WHERE simulation_id IN %s
							GROUP BY simulation_id
						;''',(tuple(sim_id_list),))
				else:
					self.db.cursor.execute('''
						SELECT COUNT(*),simulation_id FROM simulation_results
							WHERE simulation_id IN ({})
							GROUP BY simulation_id
						;'''.format(','.join(['?' for _ in sim_id_list])),sim_id_list)

				results = np.zeros((len(sim_id_list),))
				for val,s_id in self.db.cursor.fetchall():
					results[reverse_sim_index[s_id]] = val
				return results
			else:
				raise ValueError('Unknown result_type: {}'.format(result_type))

	def get_results_full(self,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,result_type='counts',aggregated=False,**sim_cfg):
		'''
		Same as preceding method but aggregated over all possible source failed projects
		'''
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=True)
		id_vec = self.get_id_vector(snapshot_id=snapid)

		index_reverse = {n:i for i,n in enumerate(id_vec)}

		self.run_simulations(snapshot_id=snapid,nb_sim=nb_sim,**sim_cfg)

		sim_list = []
		for n in id_vec:
			sim_list += self.list_simulations(failing_project=int(n),snapshot_id=snapid,max_size=nb_sim,**sim_cfg)

		sim_id_list = sorted([s_id for s_id,exec_status in sim_list])
		reverse_sim_index = {s:i for i,s in enumerate(sim_id_list)}
		#### RAW   returns sparse_mat[project,sim] aggregated by orig_failing_project
		if result_type == 'raw':
			if aggregated:
				raise ValueError('Aggregated mode is not available for result_type raw')
			else:
				if self.db.db_type =='postgres':
					self.db.cursor.execute('''
						SELECT sr.simulation_id,sr.failing,s.failing_project FROM simulation_results sr
							INNER JOIN simulations s
							ON sr.simulation_id IN %s AND s.id=sr.simulation_id
							ORDER BY sr.simulation_id,sr.failing
						;''',(tuple(sim_id_list),))
				else:
					self.db.cursor.execute('''
						SELECT sr.simulation_id,sr.failing,s.failing_project FROM simulation_results sr
							INNER JOIN simulations s
							ON sr.simulation_id IN ({}) AND s.id=sr.simulation_id
							ORDER BY sr.simulation_id,s.failing_project,sr.failing
						;'''.format(','.join(['?' for _ in sim_id_list])),sim_id_list)


				results_data = [(index_reverse[fp],reverse_sim_index[s_id],True) for s_id,fp,orig_fp in self.db.cursor.fetchall()]
				results_v = np.asarray([r[2] for r in results_data])
				results_i = np.asarray([r[0] for r in results_data])
				results_j = np.asarray([r[1] for r in results_data])
				results_ijv = (results_v,(results_i,results_j))

				results = scipy.sparse.coo_matrix(results_ijv,shape=(len(id_vec),nb_sim*len(id_vec)),dtype=np.bool).tocsr()
				return results
		#### COUNTS   returns sparse_mat[project,orig_failing_project] or nparray[project] aggregated by orig_failing_project
		elif result_type == 'counts':
			if self.db.db_type =='postgres':
				self.db.cursor.execute('''
					SELECT sr.failing,s.failing_project,COUNT(*)  FROM simulation_results sr
						INNER JOIN simulations s
						ON sr.simulation_id IN %s AND s.id=sr.simulation_id
						GROUP BY sr.failing,s.failing_project
						ORDER BY sr.failing,s.failing_project
					;''',(tuple(sim_id_list),))
			else:
				self.db.cursor.execute('''
					SELECT sr.failing,s.failing_project,COUNT(*)  FROM simulation_results sr
						INNER JOIN simulations s
						ON sr.simulation_id IN ({}) AND s.id=sr.simulation_id
						GROUP BY sr.failing,s.failing_project
						ORDER BY sr.failing,s.failing_project
						;'''.format(','.join(['?' for _ in sim_id_list])),sim_id_list)
			# results = sparse.coo_matrix((nb_nodes,nb_sim))
			if aggregated:
				results = np.zeros((len(id_vec),))
				for fp,orig_fp,val in self.db.cursor.fetchall():
					try:
						results[index_reverse[fp]] += val
					except:
						logger.warning(fp)
						logger.warning(index_reverse)
						raise
			else:
				# as sparse

				results_data = [(index_reverse[fp],index_reverse[orig_fp],val) for fp,orig_fp,val in self.db.cursor.fetchall()]
				results_v = np.asarray([r[2] for r in results_data])
				results_i = np.asarray([r[0] for r in results_data])
				results_j = np.asarray([r[1] for r in results_data])
				results_ijv = (results_v,(results_i,results_j))

				results = scipy.sparse.coo_matrix(results_ijv,shape=(len(id_vec),len(id_vec),),dtype=np.int64).tocsr()

			return results
		#### NB FAILING   returns sparse_mat[sim,orig_failing_project] or nparray[sim] aggregated by orig_failing_project
		elif result_type == 'nb_failing':
			if self.db.db_type =='postgres':
				self.db.cursor.execute('''
					SELECT sr.simulation_id,s.failing_project,COUNT(*)  FROM simulation_results sr
						INNER JOIN simulations s
						ON sr.simulation_id IN %s AND s.id=sr.simulation_id
						GROUP BY sr.simulation_id,s.failing_project
						ORDER BY sr.simulation_id,s.failing_project
					;''',(tuple(sim_id_list),))
			else:
				self.db.cursor.execute('''
					SELECT sr.simulation_id,s.failing_project,COUNT(*)  FROM simulation_results sr
						INNER JOIN simulations s
						ON sr.simulation_id IN ({}) AND s.id=sr.simulation_id
						GROUP BY sr.simulation_id,s.failing_project
						ORDER BY sr.simulation_id,s.failing_project
						;'''.format(','.join(['?' for _ in sim_id_list])),sim_id_list)

			if aggregated:
				results = np.zeros((len(sim_id_list),))
				for s_id,orig_fp,val in self.db.cursor.fetchall():
					results[reverse_sim_index[s_id]] += val
			else:
				# as sparse; having to shift sim_id by nb_sim*(orig_fp) to stack simulations by orig_fp
				results_data = [(reverse_sim_index[s_id]-nb_sim*index_reverse[orig_fp],index_reverse[orig_fp],val) for s_id,orig_fp,val in self.db.cursor.fetchall()]
				results_v = np.asarray([r[2] for r in results_data])
				results_i = np.asarray([r[0] for r in results_data])
				results_j = np.asarray([r[1] for r in results_data])
				results_ijv = (results_v,(results_i,results_j))
				results = scipy.sparse.coo_matrix(results_ijv,shape=(nb_sim,len(id_vec),)).tocsr()
			return results
		else:
			raise ValueError('Unknown result_type: {}'.format(result_type))
