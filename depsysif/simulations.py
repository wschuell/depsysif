
import logging
import networkx as nx
import copy
import numpy as np

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
	def __init__(self,failing_project,network=None,propag_proba=0.9,norm_exponent=0.,random_seed=None,verbose=False,snapshot_id=None):
		if network is not None:
			self.set_network(network=network)
		
		if random_seed is None:
			self.random_seed = np.random.randint(2**32-1)
		else:
			self.random_seed = random_seed

		self.sim_cfg = {'propag_proba':propag_proba,
						'norm_exponent':norm_exponent}

		self.propag_proba = propag_proba # probability of failure propagation along an edge if no effect of number of dependencies
		self.norm_exponent = norm_exponent # exponent of the number of dependencies to be applied in the probability

		self.results = None
		self.verbose = verbose
		self.snapshot_id = snapshot_id

		self.failing_project = failing_project

	def reset_random_generator(self):
		self.random_generator = np.random.RandomState(self.random_seed)

	def run(self,force=False,project_id=None):
		'''
		Given a specific node, computes a resulting vector of failed nodes, based on probabilistic process.
		Calling the propagate method for each new failed node, and iterating
		NB: indexing is used to keep track of the order of nodes vs. their origin IDs. This might already be done automatically in networkx, and using it could reduce computation time
		
		NB2: project_id is optional, and should in general not be used, as a default failing project exists already.
		This is just possible when you want to manually run simulations with different sources without recreating the object each time.
		A possible option would be to change the attribute self.failing_project to this, to automatically be able to submit the right results and simulation when needed
		'''
		if not force and self.results is not None:
			logger.info('Simulation already ran, skipping')
		else:
			self.reset_random_generator()
			total_nodes = len(self.network.nodes())
			index_nodes = {i:n for i,n in enumerate(sorted(self.network.nodes()))} # building indexes to match order in the vector and id in network
			index_reverse = {n:i for i,n in enumerate(sorted(self.network.nodes()))} # sorted ensures that the process is deterministic (given the random seed)
			
			# failed nodes and new_failed are vectors with ones (instead of sets or dicts)
			# initial state: a one only for the source project
			if project_id is None:
				project_id = self.failing_project
			project_nb = index_reverse[project_id]
			
			failed_nodes = np.zeros((total_nodes,),dtype=np.bool)
			failed_nodes[project_nb] = 1

			new_failed = np.zeros((total_nodes,),dtype=np.bool)
			new_failed[project_nb] = 1
			
	
			iteration = 0
	
			while new_failed.sum()>0:
				iteration += 1
				source_nb_list = np.where(new_failed>0)[0]
				new_failed = np.zeros((total_nodes,),dtype=np.bool)
				for source_nb in sorted(source_nb_list):  # sorted ensures that the process is deterministic (given the random seed)
					source_id = index_nodes[source_nb]
					propagated = self.propagate(source_id=source_id)
					for p_id in propagated:
						p_nb = index_reverse[p_id]
						new_failed[p_nb] = 1 - failed_nodes[p_nb]
						failed_nodes[p_nb] = 1
				if self.verbose:
					logger.info('Iteration {}, new failing {}, total failing {}, total nodes {}'.format(iteration,new_failed.sum(),failed_nodes.sum(),total_nodes))

		self.results = {'raw':failed_nodes,'ids':[index_nodes[p_nb] for p_nb in np.where(failed_nodes>0)[0]],'failing_project':project_id}

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


	def compute_exact(self,project_id=None):
		'''
		Given a specific node, computes a resulting vector of probabilities of failure, based on a given process.
		'''
		if project_id is None:
			project_id = self.failing_project
		pass

	def set_network(self,network):
		self.network = network

	def set_from_edge_list(self,edge_list,node_list=None):
		'''
		Similar as set_network, but from an edge list
		Can be useful for different implementations (eg sparse matrices)
		'''
		self.network = nx.DiGraph()
		if node_list is not None:
			self.network.add_nodes_from(node_list)
		self.network.add_edges_from(edge_list)

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
