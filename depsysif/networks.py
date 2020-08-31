import dynetworkx as dnx
import networkx as nx
import pandas as pd
import datetime

import logging
import sqlite3

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class Network(object):
	'''
	A class to manipulate dynamical networks of hierarchies of dependencies
	'''
	def __init__(self):
		self.graph = self.get_empty_graph()
		self.projects = pd.DataFrame() #TODO: set proper columns: name, id, version name, version date, version id
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
	def from_cratesdb_cursor(self):
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


	def analyze_edges(self):
		'''
		Detecting cycles

		'''
		logger.info('Analyzing edges')
		onecycle = 0
		for n1,n2 in self.graph.edges():
			if n2 == n1:
				onecycle += 1
		logger.info('1-cycles found: {}'.format(onecycle))

		twocycle = 0
		for n1,n2 in self.graph.edges():
			if (n2,n1) in self.graph.edges():
				twocycle += 1
		logger.info('2-cycles found: {}'.format(twocycle))

		# threecycle = 0
		# count = 0
		# for n1,n2 in self.graph.edges():
		# 	print(count)
		# 	count+=1
		# 	for n3,n4 in self.graph.edges():
		# 		if n3 == n2 and (n4,n1) in self.graph.edges():
		# 			threecycle += 1
		# logger.info('3-cycles found: {}'.format(threecycle))


	def clean_edges(self):
		'''
		Filtering the edges to avoid cycles. For the moment, only using raw node_id, which is an arbitrary assumption

		'''
		logger.info('Removing edges (using nodeid, arbitrary assumption)')
		removed = 0
		for n1,n2 in list(self.graph.edges()):
			if n1 <= n2:
				removed += 1
				self.graph.remove_edge(n1,n2)
		logger.info('Removed {} edges'.format(removed))

	def get_empty_graph(self):
		return nx.DiGraph()

	def get_snapshot(self,time=None):
		'''
		get a snapshot of the network at a given time; here return the full net
		'''
		if time is None:
			time = datetime.datetime.now()
		return self.graph


	@classmethod
	def from_file(self):
		'''
		Importing the network data from a csv file
		'''
		pass

	@classmethod
	def from_cratesdb(self,cratesdb_cursor):
		'''
		Importing the network data from a CratesDB connection
		'''
		ans = FixedNetwork()
		ans.cratesdb_cursor = cratesdb_cursor

		cratesdb_cursor.execute(''' SELECT id,name FROM crates;''')
		nodes = [(i,n) for i,n in cratesdb_cursor.fetchall()]

		cratesdb_cursor.execute(''' SELECT c.id,cd.id FROM dependencies d
								INNER JOIN versions v
									ON v.id=d.version_id
								INNER JOIN crates c
									ON c.id=v.crate_id
								INNER JOIN crates cd
									ON d.crate_id=cd.id
								GROUP BY c.id,cd.id;''')
		edges = list(cratesdb_cursor.fetchall())

		ans.graph.add_nodes_from(n[0] for n in nodes)
		ans.graph.add_edges_from(edges)


		ans.analyze_edges()
		ans.clean_edges()

		return ans


	@classmethod
	def from_libiodb(self,libiodb):
		'''
		Importing the network data from a libraries.io DB connection
		'''
		pass
