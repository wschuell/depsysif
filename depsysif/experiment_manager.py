from .database import Database
from .simulations import Simulation
from . import measures

import json
import logging
import numpy as np
# from scipy import sparse
import  scipy.sparse
from matplotlib import pyplot as plt

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
		if max_size is None:
			str_nb_sim = ''
		else:
			str_nb_sim = str(max_size)+' '
		logger.info('Listing {}simulations for snapshot_id {}, failing_project id {}'.format(str_nb_sim,snapid,failing_project))
		if failing_project is not None:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute(''' SELECT id, executed, failing_project FROM simulations
					WHERE snapshot_id = %s
						AND failing_project = %s
						AND sim_cfg = %s
					ORDER BY executed,id
					LIMIT %s
					;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=True),max_size))
			else:
				if max_size is None:
					self.db.cursor.execute(''' SELECT id, executed, failing_project FROM simulations
						WHERE snapshot_id = ?
							AND failing_project = ?
						AND sim_cfg = ?
						ORDER BY executed,id
						;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=True)))
				else:
					self.db.cursor.execute(''' SELECT id, executed, failing_project FROM simulations
						WHERE snapshot_id = ?
							AND failing_project = ?
						AND sim_cfg = ?
						ORDER BY executed,id
						LIMIT ?
						;''',(snapshot_id,failing_project,json.dumps(sim_cfg, indent=None, sort_keys=True),max_size))
		else:
			if self.db.db_type == 'postgres':
				self.db.cursor.execute('''
					SELECT ss.id,ss.executed,p.id FROM projects p
				 JOIN LATERAL (SELECT s.id, s.executed FROM simulations s
					WHERE s.snapshot_id = %s
						AND s.failing_project = p.id
						AND s.sim_cfg = %s
					ORDER BY s.executed,s.id
					LIMIT %s) AS ss ON TRUE
					ORDER BY ss.executed,ss.id
					;''',(snapshot_id,json.dumps(sim_cfg, indent=None, sort_keys=True),max_size))
			else:
				if max_size is None:
					self.db.cursor.execute(''' SELECT id, executed, failing_project FROM simulations
						WHERE snapshot_id = ?
						AND sim_cfg = ?
						ORDER BY executed,id
						;''',(snapshot_id,json.dumps(sim_cfg, indent=None, sort_keys=True)))
				else:
					self.db.cursor.execute('''
						SELECT s1.id,s1.executed,s1.failing_project FROM projects p
							JOIN simulations s1
								ON s1.id IN
									(SELECT s2.id FROM simulations s2
										WHERE s2.snapshot_id = ?
										AND s2.sim_cfg = ?
										AND s2.failing_project = p.id
										ORDER BY s2.executed,s2.id
										LIMIT ?)
								ORDER BY s1.executed,s1.id
						;''',(snapshot_id,json.dumps(sim_cfg, indent=None, sort_keys=True),max_size))

		return list(self.db.cursor.fetchall())


	def run_simulations(self,failing_project=None,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,network=None,bootstrap_sim=None,commit=True,limit_ids=None,**sim_cfg):
		'''
		checking existing simulations, creating new ones if necessary, executing the ones that are not executed yet
		'''
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=True)
		if failing_project is None:
			logger.info('Running simulations for all possible projects as source of failure')
			sim_list = self.list_simulations(failing_project=failing_project,snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
			if limit_ids:
				id_list = self.db.get_nodes(snapshot_id=snapid)[:limit_ids]
			else:
				id_list = self.db.get_nodes(snapshot_id=snapid)
			all_present = (len(sim_list) == nb_sim*len(id_list)) # assuming that everything is executed anyway, executed could be False only if code halted between simu creation in db and the computation of the simu, but conn commit should not happen in this interval anyway

			if not all_present:
				for p_id in id_list:

					if network is None:
						network = self.db.get_network(snapshot_id=snapid)
					if bootstrap_sim is None:
						bootstrap_sim = Simulation(network=network,failing_project=None,snapshot_id=snapid,**sim_cfg)

					self.run_simulations(failing_project=p_id,snapshot_id=snapid,nb_sim=nb_sim,bootstrap_sim=bootstrap_sim,network=network,commit=False,**sim_cfg)
					if commit:
						self.db.connection.commit()
			else:
				logger.info('All simulations already run for snapshot {}'.format(snapid))
		else:
			sim_list = self.list_simulations(failing_project=failing_project,snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
			for sim_id,exec_status,fp in sim_list:
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

	def get_results(self,snapshot_id=None,snapshot_time=None,full_network=False,nb_sim=100,failing_project=None,result_type='counts',aggregated=False,**sim_cfg):
		'''
		Batch getting the results of the simulations.
		Returns a vector of IDs +a vector of source IDs + a sparse binary matrix
		Alternatively output could be a pandas dataframe?
		'''
		snapid = self.db.get_snapshot_id(snapshot_id=snapshot_id,snapshot_time=snapshot_time,full_network=full_network,create=True)
		id_vec = self.get_id_vector(snapshot_id=snapid)

		index_reverse = {n:i for i,n in enumerate(id_vec)}

		if failing_project is None:
			return self.get_results_full(snapshot_id=snapid,nb_sim=nb_sim,result_type=result_type,aggregated=aggregated,**sim_cfg)
		else:
			# self.run_simulations(snapshot_id=snapid,nb_sim=nb_sim,failing_project=failing_project,**sim_cfg)  # redundancy with later list_simulations, but necessary to avoid measure computation when no simu available
			sim_list = self.list_simulations(failing_project=failing_project,snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
			if len(sim_list)<nb_sim*1:
				raise Exception('Not enough simulations, {}x{}={} expected, {} found.'.format(nb_sim,1,nb_sim*1,len(sim_list)))

			# if len(sim_list) == 0:
			# 	raise ValueError('No simulations found')
			sim_id_list = sorted([s_id for s_id,exec_status,fp in sim_list])
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

		# self.run_simulations(snapshot_id=snapid,nb_sim=nb_sim,**sim_cfg) # redundancy with later list_simulations, but necessary to avoid measure computation when no simu available

		# sim_list = []
		# for n in id_vec:
		# 	sim_list += self.list_simulations(failing_project=int(n),snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
		sim_list = self.list_simulations(failing_project=None,snapshot_id=snapid,max_size=nb_sim,**sim_cfg)
		if len(sim_list)<nb_sim*len(id_vec):
			raise Exception('Not enough simulations, {}x{}={} expected, {} found.'.format(nb_sim,len(id_vec),nb_sim*len(id_vec),len(sim_list)))

		sim_id_list = sorted([s_id for s_id,exec_status,fp in sim_list])
		reverse_sim_index = {s:i for i,s in enumerate(sim_id_list)}
		#### RAW   returns sparse_mat[project,sim] . along the sim dimension, all of them are here (no failing_project dim), hence a length of nb_sim*nb_projects
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
		#### COUNTS   returns sparse_mat[project,orig_failing_project] normalized by nb_sim or nparray[project] aggregated by orig_failing_project and normalized by nb_sim
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
				results = results/nb_sim # normalization outside of loop (not +=val/nb_sim) to avoid accumulation of rounding errors
			else:
				# as sparse

				results_data = [(index_reverse[fp],index_reverse[orig_fp],val) for fp,orig_fp,val in self.db.cursor.fetchall()]
				results_v = np.asarray([r[2] for r in results_data])
				results_i = np.asarray([r[0] for r in results_data])
				results_j = np.asarray([r[1] for r in results_data])
				results_ijv = (results_v,(results_i,results_j))

				results = scipy.sparse.coo_matrix(results_ijv,shape=(len(id_vec),len(id_vec),),dtype=np.int64).tocsr()/nb_sim

			return results
		#### NB FAILING   returns sparse_mat[sim,orig_failing_project] or nparray[orig_failing_project] aggregated by sim norm by nb_sim (no norm in non agg case)
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
				results = np.zeros((len(id_vec),))
				for s_id,orig_fp,val in self.db.cursor.fetchall():
					results[index_reverse[orig_fp]] += val
				results = results/nb_sim # normalization outside of loop (not +=val/nb_sim) to avoid accumulation of rounding errors
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

	def compute_measure(self,measure,snapshot_id=None,bootstrap_dict=None,**measure_cfg):
		'''
		Computes measure for all projects, for a given snapshot or iterating through all snapshots
		'''
		try:
			measure_func = getattr(measures,measure)
		except:
			raise ValueError('Unknown measure {}'.format(measure))

		if snapshot_id is None:
			logger.info('Computing measure {} for all snapshots'.format(measure))
			self.db.cursor.execute('SELECT id FROM snapshots;')
			snapshot_id_list = [ r[0] for r in self.db.cursor.fetchall()]
			for snapid in snapshot_id_list:
				self.compute_measure(measure=measure,snapshot_id=snapid,bootstrap_dict=bootstrap_dict,**measure_cfg)
		else:
			measure_cfg = getattr(measures,'complete_cfg_{}'.format(measure))(**measure_cfg)
			if not self.db.check_measure(measure=measure,snapshot_id=snapshot_id,**measure_cfg):
				logger.info('Computing measure {} for snapshot {}'.format(measure,snapshot_id))
				value_vec,projid_vec = measure_func(snapshot_id=snapshot_id,xp_man=self,bootstrap_dict=bootstrap_dict,**measure_cfg)
				self.db.fill_measures(measure=measure,snapshot_id=snapshot_id,value_vec=value_vec,projid_vec=projid_vec,**measure_cfg)
			else:
				logger.info('Measure {} for snapshot {} already computed'.format(measure,snapshot_id))


	def plot_measure(self,measure,project_name=None,project_id=None,show=True,**measure_cfg):
		'''
		Plots measure across time for a given project
		'''
		if project_name is None and project_id is None:
			raise ValueError('Provide project_name or project_id')
		elif project_name is None:
			project_name = self.db.get_project_name(project_id=project_id)
		else:
			project_id = self.db.get_project_id(project_name=project_name)

		try:
			measure_func = getattr(measures,measure)
		except:
			raise ValueError('Unknown measure {}'.format(measure))
		measure_cfg = getattr(measures,'complete_cfg_{}'.format(measure))(**measure_cfg)

		if self.db.db_type == 'postgres':
			self.db.cursor.execute('''
				SELECT m.value,s.snapshot_time FROM measures m
					INNER JOIN measure_types mt
						ON mt.name=%s
						AND mt.cfg=%s
						AND mt.id=m.measure_id
						AND m.project_id=%s
					INNER JOIN snapshots s
						ON m.snapshot_id=s.id
					ORDER BY s.snapshot_time
				;''',(measure,json.dumps(measure_cfg, indent=None, sort_keys=True),project_id))
		else:
			self.db.cursor.execute('''
				SELECT m.value,s.snapshot_time FROM measures m
					INNER JOIN measure_types mt
						ON mt.name=?
						AND mt.cfg=?
						AND mt.id=m.measure_id
						AND m.project_id=?
					INNER JOIN snapshots s
						ON m.snapshot_id=s.id
					ORDER BY s.snapshot_time
				;''',(measure,json.dumps(measure_cfg, indent=None, sort_keys=True),project_id))
		values = []
		dates = []
		for v,d in self.db.cursor.fetchall():
			values.append(v)
			dates.append(d)

		plt.plot(dates,values,label=project_name)
		plt.title(measure)

		if show:
			plt.show()


	def compute_exact_proba(self,snapshot_id=None,bootstrap_dict=None,proba_implementation='network',**sim_cfg):
		'''
		Computes proba distributions for all projects, for a given snapshot or iterating through all snapshots and source failing_project
		'''
		if snapshot_id is None:
			logger.info('Computing proba_distrib {} for all snapshots'.format(measure))
			self.db.cursor.execute('SELECT id FROM snapshots;')
			snapshot_id_list = [ r[0] for r in self.db.cursor.fetchall()]
			for snapid in snapshot_id_list:
				self.compute_exact_proba(snapshot_id=snapid,bootstrap_dict=bootstrap_dict,proba_implementation=proba_implementation,**sim_cfg)
		else:
			logger.info('Computing proba_distrib for snapshot {}'.format(snapshot_id))
			sim_cfg = Simulation.complete_sim_cfg(**sim_cfg)
			if not self.db.check_excomp(snapshot_id=snapshot_id,proba_implementation=proba_implementation,**sim_cfg):
				network = self.db.get_network(snapshot_id=snapshot_id)
				sim = Simulation(failing_project=None,snapshot_id=snapshot_id,network=network,**sim_cfg)
				projid_vec = self.get_id_vector(snapshot_id=snapshot_id)
				for p_id in projid_vec:
					logger.info('Computing proba distrib for snapshot {} with source failing project {}'.format(snapshot_id,p_id))
					sim.failing_project = p_id
					value_vec = sim.compute_exact(implementation=proba_implementation)
					self.db.fill_exact_comp(snapshot_id=snapshot_id,source_id=p_id,value_vec=value_vec,projid_vec=projid_vec,commit=False,proba_implementation=proba_implementation,**sim_cfg)
			else:
				logger.info('Proba distrib for snapshot {} already computed'.format(measure,snapshot_id))
