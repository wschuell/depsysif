import dynetworkx as dnx
import pandas as pd

class Network(object):
	'''
	A class to manipulate dynamical networks of hierarchies of dependencies
	'''
	def __init__(self):
		self.net = dnx.IntervalDiGraph()
		self.projects = pd.Dataframe() #TODO: set proper columns: name, id, version name, version date, version id
		self.next_id = 0

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