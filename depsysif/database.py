import os
import datetime
import logging
import sqlite3
import psycopg2
from psycopg2 import extras
import networkx as nx

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class Database(object):
	'''
	A simple database to store the data and query it efficiently.
	Network objects are not sufficient, especially because of their dynamical properties.

	By default SQLite is used, but PostgreSQL is also an option
	'''

	def __init__(self,db_type='sqlite',db_name='depsysif',db_folder='.',db_user='postgres',port='5432',host='localhost'):
		self.db_type = db_type
		if db_type == 'sqlite':
			self.connection = sqlite3.connect(os.path.join(db_folder,'{}.db'.format(db_name)))
			self.cursor = self.connection.cursor()
		elif db_type == 'postgres':
			self.connection = psycopg2.connect(user=db_user,port=port,host=host,database=db_name)
			self.cursor = self.connection.cursor()
		else:
			raise ValueError('Unknown DB type: {}'.format(db_type))

		self.init_db()

	def init_db(self):
		'''
		Initializing the database, with correct tables, constraints and indexes.
		'''
		logger.info('Creating database table and indexes')
		if self.db_type == 'sqlite':
			DB_INIT = '''
				CREATE TABLE IF NOT EXISTS projects(
				id INTEGER PRIMARY KEY,
				name TEXT UNIQUE,
				created_at DATE NOT NULL
				);

				CREATE INDEX IF NOT EXISTS proj_date ON projects(created_at);
				CREATE INDEX IF NOT EXISTS proj_name ON projects(name);

				CREATE TABLE IF NOT EXISTS versions(
				id INTEGER PRIMARY KEY,
				project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				name TEXT,
				created_at DATE NOT NULL
				);

				CREATE INDEX IF NOT EXISTS versions_date ON versions(project_id,created_at);

				CREATE TABLE IF NOT EXISTS dependencies(
				version_id INTEGER REFERENCES versions(id) ON DELETE CASCADE,
				project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				PRIMARY KEY(version_id,project_id)
				);

				CREATE INDEX IF NOT EXISTS dep_reverse ON dependencies(project_id,version_id);

				CREATE TABLE IF NOT EXISTS snapshots(
				id INTEGER PRIMARY KEY,
				name TEXT,
				full_network BOOL NOT NULL,
				snapshot_time DATE NOT NULL,
				UNIQUE(snapshot_time,full_network)
				);

				CREATE INDEX IF NOT EXISTS snap_name ON snapshots(name);

				CREATE TABLE IF NOT EXISTS snapshot_data(
				snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE CASCADE,
				project_using INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				project_used INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				PRIMARY KEY(snapshot_id,project_using,project_used)
				);

				CREATE INDEX IF NOT EXISTS snapdat_used ON snapshot_data(snapshot_id,project_used,project_using);
				'''
			for q in DB_INIT.split(';')[:-1]:
				self.cursor.execute(q)
			self.connection.commit()
		elif self.db_type == 'postgres':
			self.cursor.execute('''
				CREATE TABLE IF NOT EXISTS projects(
				id BIGSERIAL PRIMARY KEY,
				name TEXT UNIQUE,
				created_at TIMESTAMP NOT NULL
				);

				CREATE INDEX IF NOT EXISTS proj_date ON projects(created_at);
				CREATE INDEX IF NOT EXISTS proj_name ON projects(name);

				CREATE TABLE IF NOT EXISTS versions(
				id BIGSERIAL PRIMARY KEY,
				project_id BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				name TEXT,
				created_at TIMESTAMP NOT NULL
				);

				CREATE INDEX IF NOT EXISTS versions_date ON versions(project_id,created_at);

				CREATE TABLE IF NOT EXISTS dependencies(
				version_id BIGINT REFERENCES versions(id) ON DELETE CASCADE,
				project_id BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				PRIMARY KEY(version_id,project_id)
				);

				CREATE INDEX IF NOT EXISTS dep_reverse ON dependencies(project_id,version_id);

				CREATE TABLE IF NOT EXISTS snapshots(
				id BIGSERIAL PRIMARY KEY,
				name TEXT,
				full_network BOOLEAN NOT NULL,
				snapshot_time TIMESTAMP NOT NULL,
				UNIQUE(snapshot_time,full_network)
				);

				CREATE INDEX IF NOT EXISTS snap_name ON snapshots(name);


				CREATE TABLE IF NOT EXISTS snapshot_data(
				snapshot_id BIGINT REFERENCES snapshots(id) ON DELETE CASCADE,
				project_using BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				project_used BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				PRIMARY KEY(snapshot_id,project_using,project_used)
				);

				CREATE INDEX IF NOT EXISTS snapdat_used ON snapshot_data(snapshot_id,project_used,project_using);
				''')
			self.connection.commit()

	def clean_db(self):
		'''
		Dropping tables
		If there is a change in structure in the init script, this method should be called to 'reset' the state of the database
		'''
		logger.info('Cleaning database')
		self.cursor.execute('DROP TABLE IF EXISTS snapshot_data;')
		self.cursor.execute('DROP TABLE IF EXISTS snapshots;')
		self.cursor.execute('DROP TABLE IF EXISTS dependencies;')
		self.cursor.execute('DROP TABLE IF EXISTS versions;')
		self.cursor.execute('DROP TABLE IF EXISTS projects;')
		self.connection.commit()

	def is_empty(self,table):
		'''
		checking if a table is empty, result as a boolean
		'''
		if table not in ['projects','dependencies','versions','snapshots','snapshot_data']:
			raise ValueError('Not valid table name: {}'.format(table))
		else:
			self.cursor.execute('SELECT * FROM {} LIMIT 1;'.format(table))
			if self.cursor.fetchone() is None:
				return True
			else:
				return False

	def fill_from_crates(self,cratesdb_cursor=None,port=5432,user='postgres',database='crates_db',host='localhost'):
		'''
		Fill projects, versions and deps from crates.io database
		'''
		if cratesdb_cursor is None:
			conn = psycopg2.connect(user=user,port=port,database=database,host=host)
			cratesdb_cursor = conn.cursor()
		
		# PROJECTS
		if not self.is_empty(table='projects'):
			logger.info('Table projects already filled')
		else:
			logger.info('Filling projects')
			cratesdb_cursor.execute(''' SELECT id,name,created_at FROM crates;''')
			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO projects(id,name,created_at) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING;',cratesdb_cursor.fetchall())
			else:
				self.cursor.executemany('INSERT OR IGNORE INTO projects(id,name,created_at) VALUES(?,?,?);',cratesdb_cursor.fetchall())
			self.connection.commit()
			logger.info('Filled projects')


		# VERSIONS
		if not self.is_empty(table='versions'):
			logger.info('Table versions already filled')
		else:
			logger.info('Filling versions')
			cratesdb_cursor.execute(''' SELECT id,num,crate_id,created_at FROM versions;''')
			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO versions(id,name,project_id,created_at) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING;',cratesdb_cursor.fetchall())
			else:
				self.cursor.executemany('INSERT OR IGNORE INTO versions(id,name,project_id,created_at) VALUES(?,?,?,?);',cratesdb_cursor.fetchall())
	
			self.connection.commit()
			logger.info('Filled versions')

		# DEPENDENCIES
		if not self.is_empty(table='dependencies'):
			logger.info('Table dependencies already filled')
		else:
			logger.info('Filling dependencies')
			cratesdb_cursor.execute(''' SELECT version_id,crate_id FROM dependencies;''')
			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO dependencies(version_id,project_id) VALUES(%s,%s) ON CONFLICT DO NOTHING;',cratesdb_cursor.fetchall())
			else:
				self.cursor.executemany('INSERT OR IGNORE INTO dependencies(version_id,project_id) VALUES(?,?);',cratesdb_cursor.fetchall())
	
			self.connection.commit()
			logger.info('Filled dependencies')


	def fill_from_libio(self):
		'''
		Fill from libraries.io database
		'''
		pass


	def fill_from_csv(self):
		'''
		Fill from csv file
		'''
		pass

	def build_snapshot(self,t,full_network=False,name=None):
		'''
		Build a snapshot in the database, by reference to a datetime object t.
		If t is a string, intenting to convert it to datetime first.
		Should be YYYY-MM-DD.

		full_network is used to tell if the snapshot uses the dependencies of all past versions, or just the latest versions of projects
		'''

		# Converting timestamp if necessary
		if isinstance(t,str):
			try:
				if len(t) == 10:
					snapshot_time = datetime.datetime.strptime(t, '%Y-%m-%d')
				elif len(t) == 19:
					snapshot_time = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
				else:
					raise Exception # Just used to trigger the error handling, the specific message is only written once this way
			except:
				raise ValueError('Unknown timestamp format {} : Should be datetime object, or YYYY-MM-DD or YYYY-MM-DD HH:MM:SS'.format(t))
		else:
			snapshot_time = t		



		# Checking if snapshot already exists
		if self.db_type == 'postgres':
			self.cursor.execute('SELECT id,name FROM snapshots WHERE full_network=%s AND snapshot_time=%s;',(full_network,snapshot_time))
		else:
			self.cursor.execute('SELECT id,name FROM snapshots WHERE full_network=? AND snapshot_time=?;',(full_network,snapshot_time))

		ans = self.cursor.fetchone()

		if ans is not None:
			snapid,snapname = ans
			logger.info('Snapshot with full_network={} and snapshot_time={} already exists. Id: {}, Name: {}'.format(full_network,snapshot_time,snapid,snapname))
		else:
			logger.info('Creating snapshot with full_network={} and snapshot_time={}'.format(full_network,snapshot_time))
			if self.db_type == 'postgres':
				self.cursor.execute('INSERT INTO snapshots(name,full_network,snapshot_time) VALUES(%s,%s,%s);',(name,full_network,snapshot_time))
			else:
				self.cursor.execute('INSERT INTO snapshots(name,full_network,snapshot_time) VALUES(?,?,?);',(name,full_network,snapshot_time))
			if self.db_type == 'postgres':
				self.cursor.execute('SELECT id,name FROM snapshots WHERE full_network=%s AND snapshot_time=%s;',(full_network,snapshot_time))
			else:
				self.cursor.execute('SELECT id,name FROM snapshots WHERE full_network=? AND snapshot_time=?;',(full_network,snapshot_time))

			snapid,snapname = self.cursor.fetchone()
			logger.info('Created snapshot with full_network={} and snapshot_time={}. Id: {}, Name: {}'.format(full_network,snapshot_time,snapid,snapname))

		# Queries. Full network gets all links that existed at some point in the past. When it is set to false, it looks only at dependencies of the last version. 
		if full_network:
			if self.db_type == 'postgres':
				self.cursor.execute('''SELECT DISTINCT v.project_id,pused.id
						FROM dependencies d
						INNER JOIN projects pused
							ON pused.id=d.project_id
						INNER JOIN versions v
							ON v.id=d.version_id AND v.created_at<=%s
					;''',(snapshot_time,))
			else:
				self.cursor.execute('''SELECT DISTINCT v.project_id,pused.id
						FROM dependencies d
						INNER JOIN projects pused
							ON pused.id=d.project_id
						INNER JOIN versions v
							ON v.id=d.version_id AND (SELECT DATETIME(v.created_at))<=(SELECT DATETIME(?))
					;''',(snapshot_time,))
		else:
			if self.db_type == 'postgres':
				self.cursor.execute('''SELECT DISTINCT v.project_id,pused.id
						FROM dependencies d
						INNER JOIN projects pused
							ON pused.id=d.project_id
						INNER JOIN (SELECT p.id AS project_id,lv.id,lv.name,lv.created_at FROM projects p
 										JOIN LATERAL
										(SELECT v.id,v.name,v.created_at FROM versions v WHERE project_id =p.id
										AND created_at<=%s
										ORDER BY created_at DESC 
										LIMIT 1) lv ON true
										ORDER BY created_at
									) v
							ON v.id=d.version_id
					;''',(snapshot_time,))
			else:
				self.cursor.execute('''SELECT DISTINCT v.project_id,pused.id
						FROM dependencies d
						INNER JOIN projects pused
							ON pused.id=d.project_id
						JOIN versions v
							ON v.id in 
								(SELECT v1.id FROM versions v1 
									WHERE v1.project_id=v.project_id 
									AND (SELECT DATETIME(v1.created_at))<=(SELECT DATETIME(?))
									ORDER BY created_at DESC LIMIT 1)
							AND v.id=d.version_id
					;''',(snapshot_time,))
		# print(len(list(self.cursor.fetchall())))

		# Insert query results
		if self.db_type == 'postgres':
			extras.execute_batch(self.cursor,'''
				INSERT INTO snapshot_data(snapshot_id,project_using,project_used) VALUES(%s,%s,%s);
				''',((snapid,using,used) for using,used in self.cursor.fetchall()))
		else:
			self.cursor.executemany('''
				INSERT INTO snapshot_data(snapshot_id,project_using,project_used) VALUES(?,?,?);
				''',((snapid,using,used) for using,used in self.cursor.fetchall()))
	
		#Final commit to the DB
		self.connection.commit()

	def get_snapshot_id(self,snapshot_name=None,snapshot_time=None,full_network=False):
		'''
		Returns the id if existing, None otherwise
		If no args are provided for the time, max time is used. Otherwise name has priority.
		'''
		if snapshot_name is not None:
			if self.db_type == 'postgres':
				self.cursor.execute('SELECT id FROM snapshots WHERE name=%s;',(snapshot_name,))
			else:
				self.cursor.execute('SELECT id FROM snapshots WHERE name=?;',(snapshot_name,))
			id_list = [r[0] for r in self.cursor.fetchall()]
			if len(id_list)==1:
				return id_list[0]
			elif len(id_list) > 1:
				raise ValueError('The database has several ({}) snapshots with the same name: {}'.format(len(id_list),snapshot_name))
			else:
				return None

		else:
			if snapshot_time is None:
				self.cursor.execute('SELECT MAX(created_at) FROM versions;')
				snapshot_time = self.cursor.fetchone()[0]

			if self.db_type == 'postgres':
				self.cursor.execute('SELECT id FROM snapshots WHERE full_network=%s AND snapshot_time=%s;',(full_network,snapshot_time))
			else:
				self.cursor.execute('SELECT id FROM snapshots WHERE full_network=? AND snapshot_time=(SELECT DATETIME(?));',(full_network,snapshot_time))
			
			id_list = [r[0] for r in self.cursor.fetchall()]
			if len(id_list)==1:
				return id_list[0]
			elif len(id_list) > 1:
				raise ValueError('The database returned several ({}) snapshots for the parameters: full_network {}, snapshot_time {}'.format(len(id_list),full_network, snapshot_time))
			else:
				return None




	def get_network(self,snapshot_name=None,snapshot_time=None,full_network=False,as_nx_obj=False):
		'''
		Returns a snapshotted network in the form of an edge list.
		If no args are provided, max time is used. Otherwise name has priority.
		If time is provided and does not exist in the database, build_snapshot is called.
		'''
		snapid = self.get_snapshot_id(snapshot_name=snapshot_name,snapshot_time=snapshot_time,full_network=full_network)
		if snapid is None:
			self.build_snapshot(t=snapshot_time,full_network=full_network,name=snapshot_name)
			snapid = self.get_snapshot_id(snapshot_name=snapshot_name,snapshot_time=snapshot_time,full_network=full_network)
		
		logger.info('Getting elements of snapshot {}'.format(snapid))

		if self.db_type == 'postgres':
			self.cursor.execute('SELECT project_using,project_used FROM snapshot_data WHERE snapshot_id=%s;',(snapid,))
		else:
			self.cursor.execute('SELECT project_using,project_used FROM snapshot_data WHERE snapshot_id=?;',(snapid,))

		edge_list = list(self.cursor.fetchall()) # Could be used/returned as a generator
		if as_nx_obj:
			g = nx.DiGraph()
			g.add_edges_from(edge_list)
			return g
		else:
			return edge_list

