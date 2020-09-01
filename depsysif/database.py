import os

import logging
import sqlite3
import psycopg2

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
				name TEXT UNIQUE,
				full BOOL NOT NULL,
				snapshot_time DATE NOT NULL
				);

				CREATE INDEX IF NOT EXISTS snap_time ON snapshots(snapshot_time);

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
				created_at TIME NOT NULL
				);

				CREATE INDEX IF NOT EXISTS proj_date ON projects(created_at);
				CREATE INDEX IF NOT EXISTS proj_name ON projects(name);

				CREATE TABLE IF NOT EXISTS versions(
				id BIGSERIAL PRIMARY KEY,
				project_id BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				name TEXT,
				created_at TIME NOT NULL
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
				snapshot_time TIME NOT NULL
				);

				CREATE INDEX IF NOT EXISTS snap_time ON snapshots(snapshot_time);
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
		self.cursor.execute('DROP TABLE IF EXISTS snapshot_data;')
		self.cursor.execute('DROP TABLE IF EXISTS snapshots;')
		self.cursor.execute('DROP TABLE IF EXISTS dependencies;')
		self.cursor.execute('DROP TABLE IF EXISTS versions;')
		self.cursor.execute('DROP TABLE IF EXISTS projects;')
		self.connection.commit()
		
	def fill_from_crates(self):
		'''
		Fill from crates.io database
		'''
		pass

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

	def build_snapshot(self,t,full_bool=False,name=None):
		'''
		Build a snapshot in the database, by reference to a datetime object t.
		If t is a string, intenting to convert it to datetime first.
		Should be YYYY-MM-DD.

		full_bool is used to tell if the snapshot uses the dependencies of all past versions, or just the latest versions of projects
		'''
		pass

	def get_network(self,snapshot_name=None,snapshot_time=None,full_bool=False):
		'''
		Returns a snapshotted network in the form of an edge list.
		If no args are provided, max time is used. Otherwise name has priority.
		If time is provided and does not exist in the database, build_snapshot is called.
		'''
		pass
