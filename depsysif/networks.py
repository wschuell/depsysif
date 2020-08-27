import dynetworkx as dnx
import networkx as nx
import pandas as pd
import datetime

class Network(object):
	'''
	A class to manipulate dynamical networks of hierarchies of dependencies
	'''
	def __init__(self):
		self.graph = self.get_empty_graph()
		self.projects = pd.Dataframe() #TODO: set proper columns: name, id, version name, version date, version id
		self.next_id = 0

	def get_empty_graph(self):
		'''
		generating an empty network
		This method is taken out of the global __init__ for several reasons:
		 - reuse in another context (regenerate another similar network to compare it to?)
		 - overwriting the method in subclasses (eg using non-dynamical networks)
		'''
		return dnx.IntervalDiGraph()

	def add_project(self,project_name=None,version_tuples=[],project_id=None,throw_errors=True):
		'''
		Add projects to the Network object, particularly to keep track of different versions
		Projects are identified by an id number. If none provided, it uses an internal counter.
		version_tuples is a list of (version_id,created_at) NB: created_at is a datetime object
		name could be included but not relevant at this point
		'''
		versions = sorted(version_tuples,key=lambda v,t:t)
		# set project id, name

	@classmethod
	def from_file(self):
		'''
		Importing the network data from a csv file
		'''
		pass

	@classmethod
	def from_cratesdb(self):
		'''
		Importing the network data from a CratesDB connection
		'''
		pass

	@classmethod
	def from_libiodb(self):
		'''
		Importing the network data from a libraries.io DB connection
		'''
		pass

	def get_snapshot(self,time=None):
		'''
		get a snapshot of the network at a given time
		'''
		if time is None:
			time = datetime.datetime.now()





class FixedNetwork(Network):
	'''
	A subclass for networks that have no info on time dynamics. Slices/snapshots would get always the whole network
	'''
	def __init__(self):
		Network.__init__(self)

	def get_empty_graph(self):
		return nx.DiGraph()

	def get_snapshot(self,time=None):
		'''
		get a snapshot of the network at a given time; here return the full net
		'''
		if time is None:
			time = datetime.datetime.now()
		return self.graph


