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



	# def __getattr__(self,arg):
	# 	'''
	# 	Falling back on database object to avoid reimplementing parts of the interface.
	# 	'''
	# 	if hasattr(self.db,attr):
	# 		return getattr(self.db,attr)
	# 	else:
	# 		raise AttributeError('No such attribute for class {}: {}'.format(self.__class__,attr))