
import logging
import networkx as nx



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
	def __init__(self,network=None,algo=None,propag_proba=0.5,norm_exponent=0.5,random_seed=None):
		if network is not None:
			self.set_network(network=network)
		self.propag_proba = propag_proba # probability of failure propagation along an edge if no effect of number of dependencies
		self.norm_exponent = norm_exponent # exponent of the number of dependencies to be applied in the probability

	def run(self,project_id):
		'''
		Given a specific node, computes a resulting vector of failed nodes, based on probabilistic process.
		'''
		pass

	def compute_exact(self,project_id):
		'''
		Given a specific node, computes a resulting vector of probabilities of failure, based on a given process.
		'''
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
		ans = self.__class__(propag_proba=self.propag_proba,norm_exponent=norm_exponent)
		ans.set_network(self.network)
		return ans
