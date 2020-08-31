
import logging


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
	def __init__(self,network=None,algo=None,propag_proba=0.5,norm_exponent=0.5):
		if network is not None:
			self.set_network(network=network)
		self.propag_proba = propag_proba # probability of failure propagation along an edge if no effect of number of dependencies
		self.norm_exponent = norm_exponent # exponent of the number of dependencies to be applied in the probability

	def run(self,project_id):
		'''
		Given a specific node, computes a resulting vector of failed nodes, based on probabilistic process.
		'''


	def compute_exact(self,project_id):
		'''
		Given a specific node, computes a resulting vector of probabilities of failure, based on a given process.
		'''
		pass

	def set_network(self,network):
		self.network = network

