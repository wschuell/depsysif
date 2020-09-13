
import logging
import networkx as nx
import copy
import numpy as np
import scipy
import scipy.sparse

import warnings
from scipy.sparse import SparseEfficiencyWarning

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class Simulation(object):
	'''
	Simulation objects simulate cascades of failures in projects through the dependency hierarchy
	'''
	default_propag_proba = 0.9
	default_norm_exponent=0.
	# default_implementation = 'classic'
	default_implementation = 'matrix'

	def __init__(self,failing_project,network=None,propag_proba=default_propag_proba,norm_exponent=default_norm_exponent,implementation=default_implementation,random_seed=None,verbose=False,snapshot_id=None,set_network=True,bootstrap_sim=None):


		if random_seed is None:
			self.random_seed = np.random.randint(2**32-1)
		else:
			self.random_seed = random_seed

		self.sim_cfg = {'propag_proba':propag_proba,
						'norm_exponent':norm_exponent,
						'implementation':implementation}

		self.propag_proba = propag_proba # probability of failure propagation along an edge if no effect of number of dependencies
		self.norm_exponent = norm_exponent # exponent of the number of dependencies to be applied in the probability
		self.implementation = implementation # specific implementation to be used

		self.results = None
		self.verbose = verbose
		self.snapshot_id = snapshot_id

		self.failing_project = failing_project

		if set_network:
			self.set_network(network=network,bootstrap_sim=bootstrap_sim) # can potentially be None


	@classmethod
	def complete_sim_cfg(cls,in_place=False,**sim_cfg):
		'''
		Completes a partial sim_cfg dict with the default values
		'''
		sim_cfg = copy.deepcopy(sim_cfg)

		arg_list = ['propag_proba','norm_exponent','implementation']

		for arg in arg_list:
			if arg not in sim_cfg.keys():
				sim_cfg[arg] = getattr(cls,'default_{}'.format(arg))
		# if 'implementation' not in sim_cfg.keys():
		# 	sim_cfg['implementation'] = cls.default_implementation
		# if 'norm_exponent' not in sim_cfg.keys():
		# 	sim_cfg['norm_exponent'] = cls.default_norm_exponent
		# if 'propag_proba' not in sim_cfg.keys():
		# 	sim_cfg['propag_proba'] = cls.default_propag_proba


		return {k:v for k,v in sim_cfg.items() if k in arg_list}


	def set_network(self,network,bootstrap_sim):
		'''
		Setting network and other related attributes
		'''
		if bootstrap_sim is not None:
			self.network = bootstrap_sim.network
			self.sparse_mat = bootstrap_sim.sparse_mat
			self.index_nodes = bootstrap_sim.index_nodes
			self.index_reverse = bootstrap_sim.index_reverse
			self.propag_mat = bootstrap_sim.propag_mat
			self.network_diameter = bootstrap_sim.network_diameter

		elif network is not None:
			self.network = network
			self.index_nodes = np.sort(self.network.nodes())
			self.index_reverse = {n:i for i,n in enumerate(sorted(self.network.nodes()))} # sorted ensures that the process is deterministic (given the random seed)

			if len(network.nodes())>0:
				self.sparse_mat = nx.to_scipy_sparse_matrix(network).astype(np.bool)
				with np.errstate(divide='ignore',invalid='ignore'):
					norm_propag = 1./np.power(self.sparse_mat.sum(axis=0),self.norm_exponent)
			# self.propag_mat = nx.to_scipy_sparse_matrix(network).multiply(self.propag_proba/norm_propag).tocsr()
				self.propag_mat = self.sparse_mat.multiply(self.propag_proba/norm_propag).tocsr()
			# self.propag_mat = nx.to_scipy_sparse_matrix(network).transpose().multiply(self.propag_proba/norm_propag).tocsr()
			# self.propag_mat = nx.to_scipy_sparse_matrix(network).multiply(self.propag_proba/norm_propag).tocsr() # CHECK MULTIPLICATIONS ARE ALONG RIGHT DIMENSIONS
			# self.network_diameter = nx.diameter(self.network.to_undirected())
			try:
				self.network_diameter = nx.algorithms.dag.dag_longest_path_length(self.network)
			except nx.exception.NetworkXUnfeasible:
				logger.warning('Network has cycles, exact computation not available')
				self.network_diameter = None

	def set_from_edge_list(self,edge_list,node_list=None):
		'''
		Similar as set_network, but from an edge list
		Can be useful for different implementations (eg sparse matrices)
		'''
		self.network = nx.DiGraph()
		if node_list is not None:
			self.network.add_nodes_from(node_list)
		self.network.add_edges_from(edge_list)

	def reset_random_generator(self):
		self.random_generator = np.random.RandomState(self.random_seed)

	def run(self,force=False,project_id=None,full_results=True):
		'''
		Given a specific node, computes a resulting vector of failed nodes, based on probabilistic process.
		Calling the propagate method for each new failed node, and iterating
		NB: indexing is used to keep track of the order of nodes vs. their origin IDs. This might already be done automatically in networkx, and using it could reduce computation time

		NB2: project_id is optional, and should in general not be used, as a default failing project exists already.
		This is just possible when you want to manually run simulations with different sources without recreating the object each time.
		A possible option would be to change the attribute self.failing_project to this, to automatically be able to submit the right results and simulation when needed
		'''
		if not force and self.results is not None:
			if self.verbose:
				logger.info('Simulation already ran, skipping')
		else:
			if project_id is None:
				project_id = self.failing_project
			if all(False for _ in self.network.predecessors(project_id)): # checking if element is dependency of no other package (checking if at least one element in iterator)

				total_nodes = len(self.network.nodes())
				failed_nodes = np.zeros((total_nodes,),dtype=np.bool)
				project_nb = self.index_reverse[project_id]
				failed_nodes[project_nb] = 1

			elif self.implementation=='classic':
				self.reset_random_generator()
				total_nodes = len(self.network.nodes())
				# index_nodes = np.sort(self.network.nodes()) # building indexes to match order in the vector and id in network
				# index_nodes = {i:n for i,n in enumerate(sorted(self.network.nodes()))} # building indexes to match order in the vector and id in network
				# index_reverse = {n:i for i,n in enumerate(sorted(self.network.nodes()))} # sorted ensures that the process is deterministic (given the random seed)

				# failed nodes and new_failed are vectors with ones (instead of sets or dicts)
				# initial state: a one only for the source project
				project_nb = self.index_reverse[project_id]

				failed_nodes = np.zeros((total_nodes,),dtype=np.bool)
				failed_nodes[project_nb] = 1

				new_failed = np.zeros((total_nodes,),dtype=np.bool)
				new_failed[project_nb] = 1


				iteration = 0

				# while new_failed.sum()>0:
				while new_failed.any()>0:
					iteration += 1
					source_nb_list = np.where(new_failed>0)[0]
					new_failed = np.zeros((total_nodes,),dtype=np.bool)
					for source_nb in sorted(source_nb_list):  # sorted ensures that the process is deterministic (given the random seed)
						source_id = self.index_nodes[source_nb]
						propagated = self.propagate(source_id=source_id)
						for p_id in propagated:
							p_nb = self.index_reverse[p_id]
							new_failed[p_nb] = 1 - failed_nodes[p_nb]
							failed_nodes[p_nb] = 1
					if self.verbose:
						logger.info('Iteration {}, new failing {}, total failing {}, total nodes {}'.format(iteration,new_failed.sum(),failed_nodes.sum(),total_nodes))

			elif self.implementation == 'matrix':
				self.reset_random_generator()
				total_nodes = len(self.network.nodes())
				project_nb = self.index_reverse[project_id]

				failed_nodes = np.zeros((total_nodes,),dtype=np.bool)
				failed_nodes[project_nb] = 1

				new_failed = np.zeros((total_nodes,),dtype=np.bool)
				new_failed[project_nb] = 1

				# mat = copy.deepcopy(self.propag_mat)
				mat = self.propag_mat



				iteration = 0

				# while new_failed.sum()>0:
				while new_failed.any():
					iteration += 1
					# random_mat = copy.deepcopy(mat)
					# random_mat.data = self.random_generator.random(mat.data.shape)
					# intermediary_vec = (mat-random_mat).dot(new_failed)
					intermediary_vec = scipy.sparse.csr_matrix(mat.dot(new_failed))
					# intermediary_vec = mat.dot(scipy.sparse.csr_matrix(new_failed).transpose())
					# intermediary_vec = mat.dot(new_failed)
					intermediary_vec.data = intermediary_vec.data>self.random_generator.random(intermediary_vec.data.shape)
					# new_failed = (intermediary_vec>self.random_generator.random(new_failed.shape))

					new_failed = intermediary_vec.toarray().reshape((total_nodes,))
					# print('mat',mat.shape,mat.sum())
					# print(new_failed.shape,new_failed.sum())
					new_failed = np.logical_and(new_failed,np.logical_not(failed_nodes))
					# print(new_failed.shape,new_failed.sum())
					# print(failed_nodes.shape,failed_nodes.sum())
					failed_nodes = np.logical_or(failed_nodes,new_failed)

					if self.verbose:
						logger.info('Iteration {}, new failing {}, total failing {}, total nodes {}'.format(iteration,new_failed.sum(),failed_nodes.sum(),total_nodes))


			else:
				raise ValueError('Unknown implementation: {}'.format(implementation))


			if full_results:
				# self.results = {'raw':failed_nodes,'ids':[int(self.index_nodes[p_nb]) for p_nb in np.where(failed_nodes)[0]],'failing_project':project_id}
				self.results = {'raw':failed_nodes,'ids':self.index_nodes[np.where(failed_nodes)[0]],'failing_project':project_id}
			else:
				self.results = {'raw':failed_nodes}

	def propagate(self,source_id):
		'''
		propagation from one node to its neighbors

		NB: The network is directed, from projects using to projects used. Propagation of failure therefore goes up the links, not down
		'''
		ans = set()
		for n in sorted(self.network.predecessors(source_id)): # sorted ensures that the process is deterministic (given the random seed)
			nb_parents = len(list(self.network.successors(n)))
			proba = self.propag_proba/nb_parents**self.norm_exponent #nb_parents is >=1, the source node at least is in this set
			if self.random_generator.random()<=proba:
				ans.add(n)
		return ans

	def propagate_exact(self,state_vector,source_id,count=None):
		'''
		propagation from one node to its neighbors, flow of probabilities

		CAUTION: This is a recursive function, could trigger a cascade of calls
		'''
		for n in sorted(self.network.predecessors(source_id)): # sorted ensures that the process is deterministic (given the random seed)
			nb_parents = len(list(self.network.successors(n)))
			proba = self.propag_proba/nb_parents**self.norm_exponent #nb_parents is >=1, the source node at least is in this set
			s_id = self.index_reverse[source_id]
			t_id = self.index_reverse[n]
			state_vector[t_id] = 1.- (1.-state_vector[t_id])*(1.-proba*state_vector[s_id])
			if count is not None:
				count += 1
				if count % 10**4 == 0:
					logger.info(count)
			self.propagate_exact(state_vector=state_vector,source_id=n,count=count)


	def compute_exact(self,implementation='network'):
		'''
		Given a specific node, computes a resulting vector of probabilities of failure, based on a given process.
		'''
		if self.network_diameter is None:
			raise ValueError('network diameter (or longest path length) is not well defined, network has cycles')
		else:
			if implementation == 'matrix': # still inexact, needs intermediate multiplicative state
				mat = self.propag_mat.copy()
				fp_id = self.index_reverse[self.failing_project]
				with warnings.catch_warnings():
					warnings.simplefilter('ignore',SparseEfficiencyWarning)
					mat[fp_id,fp_id] = 1
				N = 2*self.network_diameter
				ans = (mat**N)[:,fp_id].toarray()
				ans = ans.reshape((ans.size,))
				return ans

			elif implementation == 'network': # can be quite long to compute, and considers variables independent
				state_vector = np.zeros((len(self.network.nodes(),)))
				fp_id = self.index_reverse[self.failing_project]
				state_vector[fp_id]=1.
				self.propagate_exact(state_vector=state_vector,source_id=self.failing_project)
				return state_vector
			else:
				raise ValueError('Unknown implementation for compute_exact: {}'.format(implementation))



	def make_copies(self,nb=1):
		'''
		Returns a list of nb copies of itself, with different random seeds and without results
		The actual copying mechanism is in the copy method
		'''
		ans = []
		ans.append(self.copy())
		return ans

	def copy(self):
		'''
		Copying the object, changing random seed and cleaning results
		'''
		ans = self.__class__(propag_proba=self.propag_proba,norm_exponent=self.norm_exponent,verbose=self.verbose)
		ans.set_network(self.network)
		return ans


