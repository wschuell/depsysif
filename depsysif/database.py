import os
import datetime
import logging
import sqlite3

import networkx as nx
import csv
import copy
import json

import sys
csv.field_size_limit(sys.maxsize)

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)

try:
	import psycopg2
	from psycopg2 import extras
except ImportError:
	logger.warning('Package psycopg2 could not be imported, please pip install psycopg2 or psycopg2-binary it if you want to use/import from PostgreSQL databases')

from . import utils

import numpy as np
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.float64, AsIs)
register_adapter(np.int64, AsIs)
sqlite3.register_adapter(np.int64, int)

class Database(object):
	'''
	A simple database to store the data and query it efficiently.
	Network objects are not sufficient, especially because of their dynamical properties.

	By default SQLite is used, but PostgreSQL is also an option
	'''

	def __init__(self,db_type='sqlite',db_name='depsysif',db_folder='.',db_user='postgres',port='5432',host='localhost',password=None,clean_first=False):
		self.db_type = db_type
		if db_type == 'sqlite':
			if db_name.startswith(':memory:'):
				self.connection = sqlite3.connect(db_name)
				self.in_ram = True
			else:
				self.in_ram = False
				self.db_path = os.path.join(db_folder,'{}.db'.format(db_name))
				if not os.path.exists(db_folder):
					os.makedirs(db_folder)
				self.connection = sqlite3.connect(self.db_path)
			self.cursor = self.connection.cursor()
		elif db_type == 'postgres':
			if password is not None:
				logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
			self.connection = psycopg2.connect(user=db_user,port=port,host=host,database=db_name,password=password)
			self.cursor = self.connection.cursor()
		else:
			raise ValueError('Unknown DB type: {}'.format(db_type))

		if clean_first:
			self.clean_db()
		self.init_db()

	def init_db(self):
		'''
		Initializing the database, with correct tables, constraints and indexes.
		'''
		logger.info('Creating database ({}) table and indexes'.format(self.db_type))
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
				full_network BOOLEAN NOT NULL,
				snapshot_time DATE NOT NULL,
				created_at DATE DEFAULT CURRENT_TIMESTAMP,
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

				CREATE TABLE IF NOT EXISTS simulations(
				id INTEGER PRIMARY KEY,
				snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE CASCADE,
				created_at DATE DEFAULT CURRENT_TIMESTAMP,
				sim_cfg TEXT,
				random_seed INTEGER,
				executed BOOLEAN DEFAULT false,
				failing_project INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				UNIQUE(snapshot_id,sim_cfg,random_seed,failing_project)
				);

				-- CREATE INDEX IF NOT EXISTS sim_algocfg ON simulations(sim_cfg);
				-- CREATE INDEX IF NOT EXISTS sim_time ON simulations(created_at);
				-- CREATE INDEX IF NOT EXISTS sim_snapid ON simulations(snapshot_id);
				-- CREATE INDEX IF NOT EXISTS sim_seed ON simulations(snapshot_id,random_seed);
				-- CREATE INDEX IF NOT EXISTS sim_exec ON simulations(snapshot_id,executed);
				-- CREATE INDEX IF NOT EXISTS sim_proj ON simulations(failing_project);
				-- CREATE INDEX IF NOT EXISTS sim_snapid_proj ON simulations(snapshot_id,failing_project);
				CREATE INDEX IF NOT EXISTS sim_extended_idx ON simulations(snapshot_id,sim_cfg,failing_project,executed,id);

				CREATE TABLE IF NOT EXISTS simulation_results(
				simulation_id INTEGER REFERENCES simulations(id) ON DELETE CASCADE,
				failing INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				PRIMARY KEY(simulation_id,failing)
				);


				CREATE TABLE IF NOT EXISTS deleted_dependencies(
				project_using INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				project_used INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				deleted_at DATE DEFAULT CURRENT_TIMESTAMP,
				deletions INTEGER,
				PRIMARY KEY(project_using,project_used)
				);

				CREATE INDEX IF NOT EXISTS deleted_used ON deleted_dependencies(project_used,project_using);
				CREATE INDEX IF NOT EXISTS deleted_time ON deleted_dependencies(deleted_at);

				CREATE TABLE IF NOT EXISTS measure_types(
				id INTEGER PRIMARY KEY,
				name TEXT,
				cfg TEXT,
				UNIQUE(name,cfg)
				);

				CREATE TABLE IF NOT EXISTS computed_measures(
				measure_id INTEGER REFERENCES measure_types(id) ON DELETE CASCADE,
				snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE CASCADE,
				created_at DATE DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(measure_id,snapshot_id)
				);

				CREATE TABLE IF NOT EXISTS measures(
				measure_id INTEGER REFERENCES measure_types(id) ON DELETE CASCADE,
				snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE CASCADE,
				project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
				value REAL,
				PRIMARY KEY (measure_id,snapshot_id,project_id)
				);

				CREATE INDEX IF NOT EXISTS measures_byproj ON measures(measure_id,project_id,snapshot_id);

				CREATE TABLE IF NOT EXISTS exact_computation(
				id INTEGER PRIMARY KEY,
				snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE CASCADE,
				cfg TEXT,
				UNIQUE(snapshot_id,cfg)
				);


				CREATE TABLE IF NOT EXISTS exact_computation_values(
				exact_comp_id BIGINT REFERENCES exact_computation(id) ON DELETE CASCADE,
				source_id  BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				target_id  BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				proba_value REAL NOT NULL,
				PRIMARY KEY(exact_comp_id,source_id,target_id)
				);
				CREATE INDEX IF NOT EXISTS ex_comp_val_idx2 ON exact_computation_values(exact_comp_id,target_id,source_id);
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
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

				CREATE TABLE IF NOT EXISTS simulations(
				id BIGSERIAL PRIMARY KEY,
				snapshot_id BIGINT REFERENCES snapshots(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				sim_cfg JSONB,
				random_seed BIGINT,
				executed BOOLEAN DEFAULT false,
				failing_project BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				UNIQUE(snapshot_id,sim_cfg,random_seed,failing_project)
				);

				-- CREATE INDEX IF NOT EXISTS sim_algocfg ON simulations USING GIN(sim_cfg);
				-- CREATE INDEX IF NOT EXISTS sim_time ON simulations(created_at);
				-- CREATE INDEX IF NOT EXISTS sim_snapid ON simulations(snapshot_id);
				-- CREATE INDEX IF NOT EXISTS sim_seed ON simulations(snapshot_id,random_seed);
				-- CREATE INDEX IF NOT EXISTS sim_exec ON simulations(snapshot_id,executed);
				-- CREATE INDEX IF NOT EXISTS sim_proj ON simulations(failing_project);
				-- CREATE INDEX IF NOT EXISTS sim_snapid_proj ON simulations(snapshot_id,failing_project);
				CREATE INDEX IF NOT EXISTS sim_extended_idx ON simulations(snapshot_id,sim_cfg,failing_project,executed,id);

				CREATE TABLE IF NOT EXISTS simulation_results(
				simulation_id BIGINT REFERENCES simulations(id) ON DELETE CASCADE,
				failing BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				PRIMARY KEY(simulation_id,failing)
				);

				CREATE TABLE IF NOT EXISTS deleted_dependencies(
				project_using BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				project_used BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				deletions BIGINT,
				PRIMARY KEY(project_using,project_used)
				);

				CREATE INDEX IF NOT EXISTS deleted_used ON deleted_dependencies(project_used,project_using);
				CREATE INDEX IF NOT EXISTS deleted_time ON deleted_dependencies(deleted_at);

				CREATE TABLE IF NOT EXISTS measure_types(
				id BIGSERIAL PRIMARY KEY,
				name TEXT,
				cfg JSONB,
				UNIQUE(name,cfg)
				);

				CREATE TABLE IF NOT EXISTS computed_measures(
				measure_id BIGINT REFERENCES measure_types(id) ON DELETE CASCADE,
				snapshot_id BIGINT REFERENCES snapshots(id) ON DELETE CASCADE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY(measure_id,snapshot_id)
				);

				CREATE TABLE IF NOT EXISTS measures(
				measure_id BIGINT REFERENCES measure_types(id) ON DELETE CASCADE,
				snapshot_id BIGINT REFERENCES snapshots(id) ON DELETE CASCADE,
				project_id BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				value REAL,
				PRIMARY KEY (measure_id,snapshot_id,project_id)
				);

				CREATE INDEX IF NOT EXISTS measures_byproj ON measures(measure_id,project_id,snapshot_id);

				CREATE TABLE IF NOT EXISTS exact_computation(
				id BIGSERIAL PRIMARY KEY,
				snapshot_id BIGINT REFERENCES snapshots(id) ON DELETE CASCADE,
				cfg JSONB,
				UNIQUE(snapshot_id,cfg)
				);


				CREATE TABLE IF NOT EXISTS exact_computation_values(
				exact_comp_id BIGINT REFERENCES exact_computation(id) ON DELETE CASCADE,
				source_id  BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				target_id  BIGINT REFERENCES projects(id) ON DELETE CASCADE,
				proba_value REAL NOT NULL,
				PRIMARY KEY(exact_comp_id,source_id,target_id)
				);
				CREATE INDEX IF NOT EXISTS ex_comp_val_idx2 ON exact_computation_values(exact_comp_id,target_id,source_id);
				''')

			self.connection.commit()

	def move_to_ram(self):
		'''
		For sqlite DBs, when doing extensive inserts etc (run_simulations for example), may be useful.
		get_back_from_ram needed though to
		'''
		if not self.db_type == 'sqlite':
			logger.info('The DB cannot be moved to the RAM, only available for SQLite DBs')
		elif not self.in_ram:
			logger.info('Moving DB to RAM')
			if not hasattr(self,'ram_connection'):
				self.ram_connection = sqlite3.connect('file:{}?mode=memory&cache=shared'.format(self.db_path),uri=True)
				self.ram_cursor = self.ram_connection.cursor()
				self.file_connection = self.connection
				self.file_cursor = self.cursor
			self.connection.backup(self.ram_connection)

			self.connection = self.ram_connection
			self.cursor = self.ram_cursor

			self.in_ram = True
			logger.info('Moved DB to RAM')

	def get_back_from_ram(self):
		'''
		See comment at move_to_ram
		'''
		if not self.db_type == 'sqlite':
			logger.info('The DB cannot be moved to/retrieved from the RAM, only available for SQLite DBs')
		elif self.in_ram:
			logger.info('Retrieving DB from RAM')
			self.connection.backup(self.file_connection)

			self.connection = self.file_connection
			self.cursor = self.file_cursor

			self.in_ram = False
			logger.info('Retrieved DB from RAM')


	def clean_db(self):
		'''
		Dropping tables
		If there is a change in structure in the init script, this method should be called to 'reset' the state of the database
		'''
		logger.info('Cleaning database')
		self.cursor.execute('DROP TABLE IF EXISTS exact_computation_values;')
		self.cursor.execute('DROP TABLE IF EXISTS exact_computation;')
		self.cursor.execute('DROP TABLE IF EXISTS measures;')
		self.cursor.execute('DROP TABLE IF EXISTS computed_measures;')
		self.cursor.execute('DROP TABLE IF EXISTS measure_types;')
		self.cursor.execute('DROP TABLE IF EXISTS simulation_results;')
		self.cursor.execute('DROP TABLE IF EXISTS simulations;')
		self.cursor.execute('DROP TABLE IF EXISTS snapshot_data;')
		self.cursor.execute('DROP TABLE IF EXISTS snapshots;')
		self.cursor.execute('DROP TABLE IF EXISTS dependencies;')
		self.cursor.execute('DROP TABLE IF EXISTS deleted_dependencies;')
		self.cursor.execute('DROP TABLE IF EXISTS versions;')
		self.cursor.execute('DROP TABLE IF EXISTS projects;')
		self.connection.commit()

	def remove_results(self):
		self.cursor.execute('DELETE FROM simulation_results CASCADE;')
		self.cursor.execute('DELETE FROM simulations CASCADE;')
		self.connection.commit()

	def remove_snapshots(self):
		self.cursor.execute('DELETE FROM snapshot_data CASCADE;')
		self.cursor.execute('DELETE FROM snapshots CASCADE;')
		self.connection.commit()

	def remove_exact_comp(self):
		self.cursor.execute('DELETE FROM exact_computation CASCADE;')
		self.connection.commit()

	def remove_measures(self,measure=None):
		'''
		Removing measures and associated data from the database
		'''
		if measure is None:
			self.cursor.execute('DELETE FROM measure_types CASCADE;')
		else:
			if self.db_type == 'postgres':
				self.cursor.execute('DELETE FROM measure_types WHERE name=%s CASCADE;',(measure,))
			else:
				self.cursor.execute('DELETE FROM measure_types WHERE name=? CASCADE;',(measure,))
		self.connection.commit()

	def remove_measure(self,measure):
		'''
		Just a wrapper for the syntax
		'''
		self.remove_measures(measure=measure)

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

	def fill_from_crates(self,cratesdb_cursor=None,port=5432,user='postgres',database='crates_db',host='localhost',password=None,optional_deps=False,dependency_types=None,delete_autodeps=True):
		'''
		Fill projects, versions and deps from crates.io database
		'''
		if not optional_deps:
			optional_deps_check = True
		else:
			optional_deps_check = False

		if dependency_types is None:
			dependency_types_check = False
			dependency_types = []
		else:
			dependency_types_check = True

		if cratesdb_cursor is None:
			if password is not None:
				logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
			conn = psycopg2.connect(user=user,port=port,database=database,host=host,password=password)
			cratesdb_cursor = conn.cursor()

		# PROJECTS
		if not self.is_empty(table='projects'):
			logger.info('Table projects already filled')
		else:
			logger.info('Filling projects from {}'.format(database))
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
			logger.info('Filling versions from {}'.format(database))
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
			logger.info('Filling dependencies from {}'.format(database))
			cratesdb_cursor.execute('''SELECT version_id,crate_id FROM dependencies
											WHERE (NOT %s OR NOT optional)
											AND (NOT %s OR kind IN %s)
											;''',(optional_deps_check,dependency_types_check,tuple(dependency_types)))
			# if optional_deps:
			# 	cratesdb_cursor.execute(''' SELECT version_id,crate_id FROM dependencies;''')
			# else:
			# 	cratesdb_cursor.execute(''' SELECT version_id,crate_id FROM dependencies WHERE NOT optional ;''')
			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO dependencies(version_id,project_id) VALUES(%s,%s) ON CONFLICT DO NOTHING;',cratesdb_cursor.fetchall())
			else:
				self.cursor.executemany('INSERT OR IGNORE INTO dependencies(version_id,project_id) VALUES(?,?);',cratesdb_cursor.fetchall())

			self.connection.commit()
			logger.info('Filled dependencies')


		if delete_autodeps:
			self.delete_auto_dependencies()


	def fill_from_libio(self,libio_cursor=None,port=5432,user='postgres',database='librariesio_db',host='localhost',password=None,platform=None,dependency_types=None,optional_deps=False,delete_autodeps=True):
		'''
		Fill from libraries.io database
		created_at is by default chosen for a reference date in the versions table, but published_at could be selected as an alternative
		we do not check here the 'dependency_platform', only the origin project platform, this might cause issues

		'''

		if libio_cursor is None:
			if password is not None:
				logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
			conn = psycopg2.connect(user=user,port=port,database=database,host=host,password=password)
			libio_cursor = conn.cursor()

		# Setting bool variables for checks
		# NB: dependency_types has to be transformed into a tuple, not a list, before being 'fed' to psycopg2
		if not optional_deps:
			optional_deps_check = True
		else:
			optional_deps_check = False

		if dependency_types is None:
			dependency_types_check = False
		else:
			dependency_types_check = True

		if platform is None:
			platform_check = False
		else:
			platform_check = True


		# PROJECTS
		if not self.is_empty(table='projects'):
			logger.info('Table projects already filled')
		else:
			logger.info('Filling projects from {}'.format(database))
			libio_cursor.execute(''' SELECT id,name,created_at FROM projects WHERE (NOT %s OR platform=%s);''',(platform_check,platform))######
			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO projects(id,name,created_at) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING;',libio_cursor.fetchall())
			else:
				self.cursor.executemany('INSERT OR IGNORE INTO projects(id,name,created_at) VALUES(?,?,?);',libio_cursor.fetchall())
			self.connection.commit()
			logger.info('Filled projects')


		# VERSIONS
		if not self.is_empty(table='versions'):
			logger.info('Table versions already filled')
		else:
			logger.info('Filling versions from {}'.format(database))
			libio_cursor.execute(''' SELECT id,number,project_id,created_at FROM versions WHERE (NOT %s OR platform=%s);''',(platform_check,platform))

			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO versions(id,name,project_id,created_at) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING;',libio_cursor.fetchall())
			else:
				self.cursor.executemany('INSERT OR IGNORE INTO versions(id,name,project_id,created_at) VALUES(?,?,?,?);',libio_cursor.fetchall())

			self.connection.commit()
			logger.info('Filled versions')

		# DEPENDENCIES
		if not self.is_empty(table='dependencies'):
			logger.info('Table dependencies already filled')
		else:
			logger.info('Filling dependencies from {}'.format(database))


			libio_cursor.execute(''' SELECT version_id,dependency_project_id FROM dependencies
											WHERE (NOT %s OR NOT optional_dependency)
											AND (NOT %s OR platform=%s)
											AND (NOT %s OR dependency_kind IN %s)
											;''',(optional_deps_check,platform_check,platform,dependency_types_check,tuple(dependency_types))) # cf remarks at bool vars definition
			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO dependencies(version_id,project_id) VALUES(%s,%s) ON CONFLICT DO NOTHING;',libio_cursor.fetchall())
			else:
				self.cursor.executemany('INSERT OR IGNORE INTO dependencies(version_id,project_id) VALUES(?,?);',libio_cursor.fetchall())

			self.connection.commit()
			logger.info('Filled dependencies')



		if delete_autodeps:
			self.delete_auto_dependencies()

	def output_struct_as_csvs(self,folder='.',projects_file='projects.csv',versions_file='versions.csv',dependencies_file='dependencies.csv',headers_present=False,delimiter=',',overwrite=False):
		'''
		Extracts the infos of the DB as CSVs, to be reused elsewhere using the method fill_from_csv
		'''
		p_file = os.path.join(folder,projects_file)
		v_file = os.path.join(folder,versions_file)
		d_file = os.path.join(folder,dependencies_file)

		if not os.path.exists(folder):
			os.makedirs(folder)

		if not overwrite and os.path.exists(p_file):
			logger.info('Projects file {} already exists, skipping'.format(p_file))
		else:
			self.cursor.execute('''
				SELECT id,name,created_at FROM projects ORDER BY id
				;''')
			with open(p_file,'w') as f:
				if headers_present:
					f.write('id, name, created_at\n')
				for r in self.cursor.fetchall():
					f.write('{},"{}","{}"\n'.format(*r))
			logger.info('Extracted projects in {}'.format(p_file))

		if not overwrite and os.path.exists(v_file):
			logger.info('Versions file {} already exists, skipping'.format(v_file))
		else:
			self.cursor.execute('''
				SELECT id,name,project_id,created_at FROM versions ORDER BY id
				;''')
			with open(v_file,'w') as f:
				if headers_present:
					f.write('id, number, project_id, created_at\n')
				for r in self.cursor.fetchall():
					f.write('{},"{}",{},"{}"\n'.format(*r))
			logger.info('Extracted versions in {}'.format(v_file))

		if not overwrite and os.path.exists(d_file):
			logger.info('Dependencies file {} already exists, skipping'.format(d_file))
		else:
			self.cursor.execute('''
				SELECT version_id,project_id FROM dependencies
				;''')
			with open(d_file,'w') as f:
				if headers_present:
					f.write('version_id,project_id\n')
				for r in self.cursor.fetchall():
					f.write('{},{}\n'.format(*r))
			logger.info('Extracted dependencies in {}'.format(d_file))

	def fill_from_csv(self,folder='.',projects_file='projects.csv',versions_file='versions.csv',dependencies_file='dependencies.csv',headers_present=False,delimiter=',',delete_autodeps=True):
		'''
		Fill from csv files, organized as:
		 projects_file: id, name, created_at
		 versions_file: id, number, project_id, created_at
		 dependencies_file: version_id,project_id

		with or without headers.

		Other templates could be used in theory, but would need another implementation of this method.
		'''

		# PROJECTS
		if not self.is_empty(table='projects'):
			logger.info('Table projects already filled')
		else:
			logger.info('Filling projects from file {}'.format(projects_file))
			with open(os.path.join(folder,projects_file),'r') as f:
				reader = csv.reader(f,delimiter=delimiter)
				if headers_present:
					next(reader)

				if self.db_type == 'postgres':
					extras.execute_batch(self.cursor,'INSERT INTO projects(id,name,created_at) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING;',(r for r in reader))
				else:
					self.cursor.executemany('INSERT OR IGNORE INTO projects(id,name,created_at) VALUES(?,?,?);',(r for r in reader))
				self.connection.commit()
			logger.info('Filled projects')


		# VERSIONS
		if not self.is_empty(table='versions'):
			logger.info('Table versions already filled')
		else:
			logger.info('Filling versions from file {}'.format(versions_file))
			with open(os.path.join(folder,versions_file),'r') as f:
				reader = csv.reader(f,delimiter=delimiter)
				if headers_present:
					next(reader)

				if self.db_type == 'postgres':
					extras.execute_batch(self.cursor,'INSERT INTO versions(id,name,project_id,created_at) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING;',reader)
				else:
					self.cursor.executemany('INSERT OR IGNORE INTO versions(id,name,project_id,created_at) VALUES(?,?,?,?);',reader)

				self.connection.commit()
			logger.info('Filled versions')

		# DEPENDENCIES
		if not self.is_empty(table='dependencies'):
			logger.info('Table dependencies already filled')
		else:
			logger.info('Filling dependencies from file {}'.format(dependencies_file))



			with open(os.path.join(folder,dependencies_file),'r') as f:
				reader = csv.reader(f,delimiter=delimiter)
				if headers_present:
					next(reader)

				if self.db_type == 'postgres':
					extras.execute_batch(self.cursor,'INSERT INTO dependencies(version_id,project_id) VALUES(%s,%s) ON CONFLICT DO NOTHING;',reader)
				else:
					self.cursor.executemany('INSERT OR IGNORE INTO dependencies(version_id,project_id) VALUES(?,?);',reader)

				self.connection.commit()
			logger.info('Filled dependencies')



		if delete_autodeps:
			self.delete_auto_dependencies()

	def fill_from_singlecsv(self,folder='.',filename='raw_dependencies.csv',headers_present=True,delimiter=',',delete_autodeps=True):
		'''
		Fill from a single csv files, as provided for pypi network

		expected syntax:
		name,version,date,deps,raw_dependencies
		'''

		# PROJECTS
		if not self.is_empty(table='projects'):
			logger.info('Table projects already filled')
		else:
			logger.info('Filling projects from file {}'.format(filename))
			with open(os.path.join(folder,filename),'r') as f:
				reader = csv.reader(f,delimiter=delimiter)
				if headers_present:
					next(reader)

				if self.db_type == 'postgres':
					extras.execute_batch(self.cursor,'INSERT INTO projects(name,created_at) VALUES(%s,%s) ON CONFLICT DO NOTHING;',((r[0],r[2]) for r in reader))
				else:
					self.cursor.executemany('INSERT OR IGNORE INTO projects(name,created_at) VALUES(?,?);',((r[0],r[2]) for r in reader))
				self.connection.commit()
			logger.info('Filled projects')


		# VERSIONS
		if not self.is_empty(table='versions'):
			logger.info('Table versions already filled')
		else:
			logger.info('Filling versions from file {}'.format(filename))
			with open(os.path.join(folder,filename),'r') as f:
				reader = csv.reader(f,delimiter=delimiter)
				if headers_present:
					next(reader)

				if self.db_type == 'postgres':
					extras.execute_batch(self.cursor,'INSERT INTO versions(name,project_id,created_at) VALUES(%s,(SELECT id FROM projects WHERE name=%s),%s) ON CONFLICT DO NOTHING;',((r[1],r[0],r[2]) for r in reader))
				else:
					self.cursor.executemany('INSERT OR IGNORE INTO versions(name,project_id,created_at) VALUES(?,(SELECT id FROM projects WHERE name=?),?);',((r[1],r[0],r[2]) for r in reader))

				self.connection.commit()
			logger.info('Filled versions')

		# DEPENDENCIES
		if not self.is_empty(table='dependencies'):
			logger.info('Table dependencies already filled')
		else:
			logger.info('Filling dependencies from file {}'.format(filename))



			with open(os.path.join(folder,filename),'r') as f:
				reader = csv.reader(f,delimiter=delimiter)
				if headers_present:
					next(reader)

				def new_reader():
					for r in reader:
						if r[3] != '':
							for d in r[3].split(','):
								yield (r[0],r[1],d)

				if self.db_type == 'postgres':
					extras.execute_batch(self.cursor,'''INSERT INTO dependencies(version_id,project_id)
									 VALUES((SELECT v.id FROM versions v
									 			INNER JOIN projects p
									 			ON p.name=%s AND v.name=%s AND v.project_id=p.id),
					 						(SELECT id FROM projects WHERE name=%s)) ON CONFLICT DO NOTHING;''',new_reader())
				else:
					self.cursor.executemany('''INSERT OR IGNORE INTO dependencies(version_id,project_id)
									 VALUES((SELECT v.id FROM versions v
									 			INNER JOIN projects p
									 			ON p.name=? AND v.name=? AND v.project_id=p.id),
					 						(SELECT id FROM projects WHERE name=?));''',new_reader())

				self.connection.commit()
			logger.info('Filled dependencies')



		if delete_autodeps:
			self.delete_auto_dependencies()



	def build_snapshot(self,snapshot_time,full_network=False,name=None):
		'''
		Build a snapshot in the database, by reference to a datetime object t.
		If t is a string, intenting to convert it to datetime first.
		Should be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.

		full_network is used to tell if the snapshot uses the dependencies of all past versions, or just the latest versions of projects
		'''

		# Converting timestamp if necessary
		snapshot_time = utils.clean_timestamp(snapshot_time)



		# Checking if snapshot already exists
		if self.db_type == 'postgres':
			self.cursor.execute('SELECT id,name FROM snapshots WHERE full_network=%s AND snapshot_time=%s;',(full_network,snapshot_time))
		else:
			self.cursor.execute('SELECT id,name FROM snapshots WHERE full_network=? AND snapshot_time=?;',(full_network,snapshot_time))

		ans = self.cursor.fetchone()

		if ans is not None:
			snapid,snapname = ans
			logger.info('Snapshot with full_network={} and snapshot_time={} already exists. Id: {}, Name: {}'.format(full_network,snapshot_time,snapid,snapname))

			###### The following lines are commented because snapshot data is automatically field after creatin, and no commits happen between snapshot creation and data filling
			###### This speeds up the process when reexecuting build_snapshot
			# if self.db_type == 'postgres':
			# 	self.cursor.execute('SELECT * FROM snapshot_data WHERE snapshot_id=%s LIMIT 1;',(snapid,))
			# else:
			# 	self.cursor.execute('SELECT * FROM snapshot_data WHERE snapshot_id=? LIMIT 1;',(snapid,))
			# if self.cursor.fetchone() is not None:
			# 	logger.info('Snapshot data already filled, skipping')
			# 	return
			return

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

	def get_project_id(self,project_name,raise_error=True):
		'''
		returns the id of a project given the name
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('SELECT id FROM projects WHERE name=%s;',(project_name,))
		else:
			self.cursor.execute('SELECT id FROM projects WHERE name=?;',(project_name,))
		ans = list(self.cursor.fetchall())
		if len(ans)==0:
			if raise_error:
				raise ValueError('No project with name {}'.format(project_name))
			else:
				return None
		elif len(ans)>1:
			raise ValueError('Several projects with name {}: {}'.format(project_name,len(ans)))
		else:
			return ans[0][0]

	def get_project_name(self,project_id,raise_error=True):
		'''
		returns the name of a project given the id
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('SELECT name FROM projects WHERE id=%s;',(project_id,))
		else:
			self.cursor.execute('SELECT name FROM projects WHERE id=?;',(project_id,))
		ans = self.cursor.fetchone()
		if ans is None:
			if raise_error:
				raise ValueError('No project with id {}'.format(project_id))
			else:
				return None
		else:
			return ans[0]


	def get_snapshot_id(self,snapshot_id=None,snapshot_name=None,snapshot_time=None,full_network=False,create=True):
		'''
		Returns the id if existing, None otherwise
		If no args are provided for the time, max time is used. Otherwise name has priority.
		'''
		if snapshot_id is not None:
			return snapshot_id
		elif snapshot_name is not None:
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
				if self.db_type == 'postgres':
					self.cursor.execute('''SELECT MAX(created_at) + interval '1 second' FROM versions;''')
				else:
					self.cursor.execute('''SELECT DATETIME(MAX(created_at),'+1 second') FROM versions;''')
				snapshot_time = self.cursor.fetchone()[0]

			snapshot_time = utils.clean_timestamp(snapshot_time)

			if self.db_type == 'postgres':
				self.cursor.execute('SELECT id FROM snapshots WHERE full_network=%s AND snapshot_time=%s;',(full_network,snapshot_time))
			else:
				self.cursor.execute('SELECT id FROM snapshots WHERE full_network=? AND (SELECT DATETIME(snapshot_time))=(SELECT DATETIME(?));',(full_network,snapshot_time))

			id_list = [r[0] for r in self.cursor.fetchall()]
			if len(id_list)==1:
				return id_list[0]
			elif len(id_list) > 1:
				raise ValueError('The database returned several ({}) snapshots for the parameters: full_network {}, snapshot_time {}'.format(len(id_list),full_network, snapshot_time))
			elif create:
				self.build_snapshot(snapshot_time=snapshot_time,full_network=full_network)
				return self.get_snapshot_id(snapshot_time=snapshot_time,full_network=full_network)
			else:
				raise ValueError('No snapshot corresponding to description: snapshot name {}, snapshot_time {}, full_network {}'.format(snapshot_name,snapshot_time,full_network))


	def get_nodes(self,snapshot_id=None,snapshot_time=None,full_network=False):
		'''
		Gets the list of existing projects at a given time, or at the time of a given snapshot
		Based on 'created_at' attribute of projects
		Priority to snapshot_time
		'''
		if snapshot_time is not None:
			snaptime = snapshot_time
		elif snapshot_id is not None:
			if self.db_type == 'postgres':
				self.cursor.execute('SELECT snapshot_time FROM snapshots WHERE id=%s;',(snapshot_id,))
			else:
				self.cursor.execute('SELECT snapshot_time FROM snapshots WHERE id=?;',(snapshot_id,))
			query_result = self.cursor.fetchone()
			if query_result is None:
				raise ValueError('Snapshot id not found in database: {}'.format(snapshot_id))
			else:
				snaptime = query_result[0]
		else:
			raise ValueError('You should provide either snapshot_id or snapshot_time')

		snaptime = utils.clean_timestamp(snaptime)

		logger.info('Getting nodes at time {}'.format(snaptime.strftime('%Y-%m-%d %H:%M:%S')))
		if self.db_type == 'postgres':
			self.cursor.execute('SELECT id FROM projects WHERE created_at<=%s ORDER BY id;',(snaptime,))
		else:
			self.cursor.execute('SELECT id FROM projects WHERE created_at<=(SELECT DATETIME(?)) ORDER BY id;',(snaptime,))
		return [r[0] for r in self.cursor.fetchall()]



	def get_network(self,snapshot_id=None,snapshot_name=None,snapshot_time=None,full_network=False,as_nx_obj=True,create=True):
		'''
		Returns a snapshotted network in the form of an edge list.
		If no args are provided, max time is used. Otherwise name has priority.
		If time is provided and does not exist in the database, build_snapshot is called.

		NB: The network is directed, from projects using to projects used. Propagation of failure therefore goes up the links, not down
		'''
		snapid = self.get_snapshot_id(snapshot_id=snapshot_id,snapshot_name=snapshot_name,snapshot_time=snapshot_time,full_network=full_network,create=create)

		logger.info('Getting elements of snapshot {}'.format(snapid))

		if self.db_type == 'postgres':
			self.cursor.execute('SELECT project_using,project_used FROM snapshot_data WHERE snapshot_id=%s;',(snapid,))
		else:
			self.cursor.execute('SELECT project_using,project_used FROM snapshot_data WHERE snapshot_id=?;',(snapid,))

		edge_list = list(self.cursor.fetchall()) # Could be used/returned as a generator
		if as_nx_obj:
			g = nx.DiGraph()
			node_list = self.get_nodes(snapshot_id=snapid,snapshot_time=snapshot_time)
			g.add_nodes_from(node_list)
			g.add_edges_from(edge_list)
			return g
		else:
			return edge_list

	def detect_cycles(self,snapshot_id,cycle_length=None,convert_to_names=True):
		'''
		detecting cycles in a particular snapshot
		Length 1,2,3 and 4 supported so far

		Returning a tuple per (distinct) cycle, beginning at the lowest project id for each cycle.
		Auto-excluding smaller length subcycles.
		'''
		if cycle_length == 1:
			if self.db_type == 'postgres':
				self.cursor.execute('SELECT project_used FROM snapshot_data WHERE snapshot_id=%s AND project_used=project_using;',(snapshot_id,))
			else:
				self.cursor.execute('SELECT project_used FROM snapshot_data WHERE snapshot_id=? AND project_used=project_using;',(snapshot_id,))
		elif cycle_length == 2:
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT sd1.project_using,sd1.project_used FROM snapshot_data sd1
						INNER JOIN snapshot_data sd2
							ON sd1.snapshot_id=%s AND sd2.snapshot_id=%s
							AND sd1.project_using<sd1.project_used -- uniqueness of results
							AND sd1.project_used=sd2.project_using  -- propagation to sd2
							AND sd2.project_used=sd1.project_using  -- propagation to sd1
							AND sd1.project_using!=sd1.project_used -- no 1-cycle
							;''',(snapshot_id,snapshot_id,))
			else:
				self.cursor.execute('''
					SELECT sd1.project_using,sd1.project_used FROM snapshot_data sd1
						INNER JOIN snapshot_data sd2
							ON sd1.snapshot_id=? AND sd2.snapshot_id=?
							AND sd1.project_using<sd1.project_used -- uniqueness of results
							AND sd1.project_used=sd2.project_using  -- propagation to sd2
							AND sd2.project_used=sd1.project_using  -- propagation to sd1
							AND sd1.project_using!=sd1.project_used -- no 1-cycle
							;''',(snapshot_id,snapshot_id,))
		elif cycle_length == 3:
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT sd1.project_using,sd2.project_using,sd3.project_using FROM snapshot_data sd1
						INNER JOIN snapshot_data sd2
							ON sd1.snapshot_id=%s AND sd2.snapshot_id=%s
							AND sd1.project_used=sd2.project_using  -- propagation to sd2
							AND sd1.project_using!=sd1.project_used  -- no 1-cycle in sd1
							AND sd2.project_using!=sd2.project_used  -- no 1-cycle in sd2
							AND sd1.project_using!=sd2.project_used  -- no 2-cycle in sd1-sd2
							AND sd1.project_using<sd2.project_using -- uniqueness first step -- might be combined with other statements
						INNER JOIN snapshot_data sd3
							ON sd3.snapshot_id=%s
							AND sd2.project_used=sd3.project_using   -- propagation to sd3
							AND sd3.project_used=sd1.project_using    -- propagation to sd1
							AND sd3.project_using!=sd3.project_used  -- no 1-cycle in sd3
							AND sd2.project_using!=sd3.project_used  -- no 2-cycle in sd2-sd3
							AND sd3.project_using!=sd1.project_used  -- no 2-cycle in sd3-sd1
							AND sd1.project_using<sd3.project_using -- uniqueness second step -- might be combined with other statements
						;''',(snapshot_id,snapshot_id,snapshot_id,))
			else:
				self.cursor.execute('''
					SELECT sd1.project_using,sd2.project_using,sd3.project_using FROM snapshot_data sd1
						INNER JOIN snapshot_data sd2
							ON sd1.snapshot_id=? AND sd2.snapshot_id=?
							AND sd1.project_used=sd2.project_using  -- propagation to sd2
							AND sd1.project_using!=sd1.project_used  -- no 1-cycle in sd1
							AND sd2.project_using!=sd2.project_used  -- no 1-cycle in sd2
							AND sd1.project_using!=sd2.project_used  -- no 2-cycle in sd1-sd2
							AND sd1.project_using<sd2.project_using -- uniqueness first step -- might be combined with other statements
						INNER JOIN snapshot_data sd3
							ON sd3.snapshot_id=?
							AND sd2.project_used=sd3.project_using   -- propagation to sd3
							AND sd3.project_used=sd1.project_using    -- propagation to sd1
							AND sd3.project_using!=sd3.project_used  -- no 1-cycle in sd3
							AND sd2.project_using!=sd3.project_used  -- no 2-cycle in sd2-sd3
							AND sd3.project_using!=sd1.project_used  -- no 2-cycle in sd3-sd1
							AND sd1.project_using<sd3.project_using -- uniqueness second step -- might be combined with other statements
						;''',(snapshot_id,snapshot_id,snapshot_id,))
		elif cycle_length == 4:
			if self.db_type == 'postgres':
				self.cursor.execute('''
					SELECT sd1.project_using,sd2.project_using,sd3.project_using FROM snapshot_data sd1
						INNER JOIN snapshot_data sd2
							ON sd1.snapshot_id=%s AND sd2.snapshot_id=%s
							AND sd1.project_used=sd2.project_using  -- propagation to sd2
							AND sd1.project_using!=sd1.project_used  -- no 1-cycle in sd1
							AND sd2.project_using!=sd2.project_used  -- no 1-cycle in sd2
							AND sd1.project_using!=sd2.project_used  -- no 2-cycle in sd1-sd2
							AND sd1.project_using<sd2.project_using -- uniqueness first step -- might be combined with other statements
						INNER JOIN snapshot_data sd3
							ON sd3.snapshot_id=%s
							AND sd2.project_used=sd3.project_using   -- propagation to sd3
							AND sd3.project_using!=sd3.project_used  -- no 1-cycle in sd3
							AND sd2.project_using!=sd3.project_used  -- no 2-cycle in sd2-sd3
							AND sd1.project_using<sd3.project_using -- uniqueness second step -- might be combined with other statements
							AND sd3.project_used!=sd1.project_using  -- no 3-cycle in sd1-sd3
						INNER JOIN snapshot_data sd4
							ON sd4.snapshot_id=%s
							AND sd3.project_used=sd3.project_using   -- propagation to sd4
							AND sd4.project_used=sd1.project_using    -- propagation to sd1
							AND sd4.project_using!=sd4.project_used  -- no 1-cycle in sd4
							AND sd3.project_using!=sd4.project_used  -- no 2-cycle in sd3-sd4
							AND sd4.project_using!=sd1.project_used  -- no 2-cycle in sd4-sd1
							AND sd1.project_using<sd4.project_using -- uniqueness third step -- might be combined with other statements
							AND sd4.project_used!=sd2.project_using  -- no 3-cycle in sd2-sd4
						;''',(snapshot_id,snapshot_id,snapshot_id,snapshot_id,))
			else:
				self.cursor.execute('''
					SELECT sd1.project_using,sd2.project_using,sd3.project_using FROM snapshot_data sd1
						INNER JOIN snapshot_data sd2
							ON sd1.snapshot_id=? AND sd2.snapshot_id=?
							AND sd1.project_used=sd2.project_using  -- propagation to sd2
							AND sd1.project_using!=sd1.project_used  -- no 1-cycle in sd1
							AND sd2.project_using!=sd2.project_used  -- no 1-cycle in sd2
							AND sd1.project_using!=sd2.project_used  -- no 2-cycle in sd1-sd2
							AND sd1.project_using<sd2.project_using -- uniqueness first step -- might be combined with other statements
						INNER JOIN snapshot_data sd3
							ON sd3.snapshot_id=?
							AND sd2.project_used=sd3.project_using   -- propagation to sd3
							AND sd3.project_using!=sd3.project_used  -- no 1-cycle in sd3
							AND sd2.project_using!=sd3.project_used  -- no 2-cycle in sd2-sd3
							AND sd1.project_using<sd3.project_using -- uniqueness second step -- might be combined with other statements
							AND sd3.project_used!=sd1.project_using  -- no 3-cycle in sd1-sd3
						INNER JOIN snapshot_data sd4
							ON sd4.snapshot_id=?
							AND sd3.project_used=sd3.project_using   -- propagation to sd4
							AND sd4.project_used=sd1.project_using    -- propagation to sd1
							AND sd4.project_using!=sd4.project_used  -- no 1-cycle in sd4
							AND sd3.project_using!=sd4.project_used  -- no 2-cycle in sd3-sd4
							AND sd4.project_using!=sd1.project_used  -- no 2-cycle in sd4-sd1
							AND sd1.project_using<sd4.project_using -- uniqueness third step -- might be combined with other statements
							AND sd4.project_used!=sd2.project_using  -- no 3-cycle in sd2-sd4
						;''',(snapshot_id,snapshot_id,snapshot_id,snapshot_id,))
		elif cycle_length is None:
			net = self.get_network(snapshot_id=snapshot_id,as_nx_obj=True)
			logger.info('Trying to find a cycle, of any length')

			if not nx.algorithms.dag.is_directed_acyclic_graph(net): # using this to have the fastest implementation in the longest running case: no cycles. Doubling the computation time when cycles are present, but anyway it takes way less time and should not be happening anyway.
				cycle = nx.algorithms.cycles.find_cycle(net)
				ans = [tuple([r[0] for r in cycle])]
				logger.info('Found a cycle, of length {}: {}'.format(len(ans[0]),ans[0]))
			else:
				logger.info('No cycles found')
				ans = []
		else:
			raise ValueError('Unsupported cycle length: {}'.format(cycle_length))

		if cycle_length is not None:
			ans = list(self.cursor.fetchall())
			logger.info('Found {} cycles of length {}'.format(len(ans),cycle_length))
		if convert_to_names:
			return [ tuple([self.get_project_name(rr) for rr in r]) for r in ans]
		else:
			return ans


	def register_simulation(self,simulation,snapshot_id=None,commit=True):
		'''
		Registers a simulation object into the database
		If results are available, puts results as well

		TODO: Should forbid to register if results are not available (=attr set to None) to avoid the necessity of the executed attr in the DB
		'''
		if snapshot_id is None:
			snapshot_id = simulation.snapshot_id
		if snapshot_id is None:
			raise ValueError('Provide a snapshot_id to register the simulation, or set it within the simulation object, or get the simulation from an experiment manager object')
		else:
			if self.db_type == 'postgres':
				self.cursor.execute(''' INSERT INTO simulations(snapshot_id,sim_cfg,random_seed,failing_project)
					VALUES(%s,%s,%s,%s)
					ON CONFLICT DO NOTHING;
					;''',(snapshot_id,json.dumps(simulation.sim_cfg, indent=None, sort_keys=True),simulation.random_seed,simulation.failing_project))
			else:
				self.cursor.execute('''INSERT OR IGNORE INTO simulations(snapshot_id,sim_cfg,random_seed,failing_project)
					VALUES(?,?,?,?)
					;''',(snapshot_id,json.dumps(simulation.sim_cfg, indent=None, sort_keys=True),simulation.random_seed,simulation.failing_project))

			if simulation.results is not None:
				self.submit_simulation_results(simulation=simulation,snapshot_id=snapshot_id,commit=False)
			if commit:
				self.connection.commit()


	def submit_simulation_results(self,simulation,snapshot_id=None,sim_id=None,commit=True):
		'''
		Puts the results of given simulation in the database
		'''
		if snapshot_id is None:
			snapshot_id = simulation.snapshot_id
		if snapshot_id is None:
			raise ValueError('Provide a snapshot_id to register the simulation, or set it within the simulation object, or get the simulation from an experiment manager object')
		elif simulation.results is None:
			logger.info('Simulation has no results to be submitted')
		else:
			if self.db_type == 'postgres':
				if sim_id is None:
					self.cursor.execute('''SELECT id,executed FROM simulations
								WHERE snapshot_id=%s
								AND sim_cfg=%s
								AND random_seed=%s
								AND failing_project=%s
					;''',(snapshot_id,json.dumps(simulation.sim_cfg, indent=None, sort_keys=True),simulation.random_seed,simulation.failing_project))

					sim_id_list = self.cursor.fetchone()
					if sim_id_list is None:
						self.register_simulation(simulation=simulation,snapshot_id=snapshot_id)
						return
					else:
						sim_id,executed = sim_id_list
						if executed:
							logger.info('Simulation results already filled in')
							return
				extras.execute_batch(self.cursor,'''
						INSERT INTO simulation_results(simulation_id,failing)
						VALUES(%s,%s)
						;''',((sim_id,fp) for fp in simulation.results['ids']))
				self.cursor.execute('''UPDATE simulations SET executed=TRUE WHERE id=%s;''',(sim_id,))
			else:
				if sim_id is None:
					self.cursor.execute('''SELECT id,executed FROM simulations
								WHERE snapshot_id=?
								AND sim_cfg=?
								AND random_seed=?
								AND failing_project=?
					;''',(snapshot_id,json.dumps(simulation.sim_cfg, indent=None, sort_keys=True),simulation.random_seed,simulation.failing_project))

					sim_id_list = self.cursor.fetchone()
					if sim_id_list is None:
						self.register_simulation(simulation=simulation,snapshot_id=snapshot_id,commit=False)
						return
					else:
						sim_id,executed = sim_id_list
						if executed:
							logger.info('Simulation results already filled in')
							return
				self.cursor.executemany('''
						INSERT INTO simulation_results(simulation_id,failing)
						VALUES(?,?)
						;''',((sim_id,fp) for fp in simulation.results['ids']))

				self.cursor.execute('''UPDATE simulations SET executed=1 WHERE id=?;''',(sim_id,))
			if commit:
				self.connection.commit()

	def delete_dependency(self,source,target):
		'''
		deletes all dependencies between any version of source to target
		stores the couple in deleted_dependencies_table only if there was in fact a dependency to delete
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''
				DELETE FROM dependencies d
					USING versions v
						WHERE v.id=d.version_id
							AND v.project_id=%s
							AND d.project_id=%s
				;''',(source,target))
			deleted = self.cursor.rowcount
		else:
			self.cursor.execute('''
				DELETE FROM dependencies
					WHERE EXISTS (SELECT * FROM versions v
							WHERE v.id=dependencies.version_id
							AND v.project_id=?)
						AND dependencies.project_id=?
				;''',(source,target))

			self.cursor.execute('SELECT changes();')
			deleted = self.cursor.fetchone()[0]


		if deleted > 0:
			if self.db_type == 'postgres':
				self.cursor.execute('''
					INSERT INTO deleted_dependencies(project_using,project_used,deletions) VALUES(%s,%s,%s)
					;''',(source,target,deleted))
			else:
				self.cursor.execute('''
					INSERT INTO deleted_dependencies(project_using,project_used,deletions) VALUES(?,?,?)
					;''',(source,target,deleted))
			self.connection.commit()
			logger.info('Deleted dependency links from {} to {}'.format(source,target))

	def delete_auto_dependencies(self):
		'''
		Removing length one cycles
		'''
		self.cursor.execute('''
			SELECT d.project_id FROM dependencies AS d
				INNER JOIN versions v
					ON v.id = d.version_id
					AND d.project_id = v.project_id
			;''')
		for autoref in self.cursor.fetchall():
			self.delete_dependency(source=autoref[0],target=autoref[0])

	def delete_from_list(self,dep_list=[],filename=None):
		'''
		Delete dependencies from given list and filename (both can be used at the same time)
		'''
		dep_list = copy.deepcopy(dep_list)
		if filename is not None:
			with open(filename,'r') as f:
				dep_list += [l.split(',') for l in f.read().split('\n') if ',' in l and not l.startswith('#')]
		for s,t in dep_list:
			try:
				s_id = int(s)
			except:
				s_id = self.get_project_id(project_name=s,raise_error=False)
			try:
				t_id = int(t)
			except:
				t_id = self.get_project_id(project_name=t,raise_error=False)
			if s_id is not None and t_id is not None:
				self.delete_dependency(source=s_id,target=t_id)

	def fill_measures(self,measure,snapshot_id,value_vec,projid_vec,**measure_cfg):
		'''
		Fills in results of a measure
		TODO: autocomplete measure_cfg
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('INSERT INTO measure_types(name,cfg) VALUES(%s,%s) ON CONFLICT DO NOTHING;',(measure,json.dumps(measure_cfg, indent=None, sort_keys=True)))
			self.cursor.execute('SELECT id FROM measure_types WHERE name=%s AND cfg=%s;',(measure,json.dumps(measure_cfg, indent=None, sort_keys=True)))
		else:
			self.cursor.execute('INSERT OR IGNORE INTO measure_types(name,cfg) VALUES(?,?);',(measure,json.dumps(measure_cfg, indent=None, sort_keys=True)))
			self.cursor.execute('SELECT id FROM measure_types WHERE name=? AND cfg=?;',(measure,json.dumps(measure_cfg, indent=None, sort_keys=True)))

		measure_id = self.cursor.fetchone()[0]

		if self.db_type == 'postgres':
			self.cursor.execute('SELECT * FROM  computed_measures WHERE measure_id=%s AND snapshot_id=%s;',(measure_id,snapshot_id))
		else:
			self.cursor.execute('SELECT * FROM  computed_measures WHERE measure_id=? AND snapshot_id=?;',(measure_id,snapshot_id))

		if self.cursor.fetchone() is not None:
			logger.info('Measure {} for snapshot {} already filled in'.format(measure,snapshot_id))
		else:
			logger.info('Filling in measure {} for snapshot {}'.format(measure,snapshot_id))

			if self.db_type == 'postgres':
				self.cursor.execute('INSERT INTO computed_measures(measure_id,snapshot_id) VALUES(%s,%s);',(measure_id,snapshot_id))
			else:
				self.cursor.execute('INSERT INTO computed_measures(measure_id,snapshot_id) VALUES(?,?);',(measure_id,snapshot_id))

			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO measures(measure_id,snapshot_id,project_id,value) VALUES(%s,%s,%s,%s);',((measure_id,snapshot_id,p_id,val) for p_id,val in zip(projid_vec,value_vec)))
			else:
				self.cursor.executemany('INSERT INTO measures(measure_id,snapshot_id,project_id,value) VALUES(?,?,?,?);',((measure_id,snapshot_id,p_id,val) for p_id,val in zip(projid_vec,value_vec)))

			self.connection.commit()
			logger.info('Filled in measure {} for snapshot {}'.format(measure,snapshot_id))


	def fill_exact_comp(self,snapshot_id,source_id,value_vec,projid_vec,commit=True,**sim_cfg):
		'''
		Fills in results of an exact proba distrib computation
		TODO: autocomplete cfg
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('INSERT INTO exact_computation(snapshot_id,cfg) VALUES(%s,%s) ON CONFLICT DO NOTHING;',(snapshot_id,json.dumps(sim_cfg, indent=None, sort_keys=True)))
			self.cursor.execute('SELECT id FROM exact_computation WHERE snapshot_id=%s AND cfg=%s;',(snapshot_id,json.dumps(sim_cfg, indent=None, sort_keys=True)))
		else:
			self.cursor.execute('INSERT OR IGNORE INTO exact_computation(snapshot_id,cfg) VALUES(?,?);',(snapshot_id,json.dumps(sim_cfg, indent=None, sort_keys=True)))
			self.cursor.execute('SELECT id FROM exact_computation WHERE snapshot_id=? AND cfg=?;',(snapshot_id,json.dumps(sim_cfg, indent=None, sort_keys=True)))

		excomp_id = self.cursor.fetchone()[0]

		# if self.db_type == 'postgres':
		# 	self.cursor.execute('SELECT * FROM  computed_measures WHERE measure_id=%s AND snapshot_id=%s;',(measure_id,snapshot_id))
		# else:
		# 	self.cursor.execute('SELECT * FROM  computed_measures WHERE measure_id=? AND snapshot_id=?;',(measure_id,snapshot_id))

		if self.cursor.fetchone() is not None:
			logger.info('Proba distrib for snapshot {} for source_id already filled in'.format(snapshot_id,source_id))
		else:
			logger.info('Filling in proba distrib for snapshot {} for source_id {}'.format(snapshot_id,source_id))

			# if self.db_type == 'postgres':
			# 	self.cursor.execute('INSERT INTO computed_measures(measure_id,snapshot_id) VALUES(%s,%s);',(measure_id,snapshot_id))
			# else:
			# 	self.cursor.execute('INSERT INTO computed_measures(measure_id,snapshot_id) VALUES(?,?);',(measure_id,snapshot_id))

			if self.db_type == 'postgres':
				extras.execute_batch(self.cursor,'INSERT INTO exact_computation_values(exact_comp_id,source_id,target_id,proba_value) VALUES(%s,%s,%s,%s);',((excomp_id,source_id,p_id,val) for p_id,val in zip(projid_vec,value_vec) if val!=0))
			else:
				self.cursor.executemany('INSERT INTO exact_computation_values(exact_comp_id,source_id,target_id,proba_value) VALUES(?,?,?,?);',((excomp_id,source_id,p_id,val) for p_id,val in zip(projid_vec,value_vec) if val!=0))

			if commit:
				self.connection.commit()
			logger.info('Filled in proba_distrib for snapshot {} for source_id {}'.format(snapshot_id,source_id))



	def check_measure(self,measure,snapshot_id,**measure_cfg):
		'''
		Check if a measure has already been computed, returns bool
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''SELECT * FROM computed_measures cm
									INNER JOIN measure_types mt
									ON cm.snapshot_id=%s AND mt.id=cm.measure_id
									AND mt.name=%s AND mt.cfg=%s
									LIMIT 1
								;''',(snapshot_id,measure,json.dumps(measure_cfg, indent=None, sort_keys=True)))
		else:
			self.cursor.execute('''SELECT * FROM computed_measures cm
									INNER JOIN measure_types mt
									ON cm.snapshot_id=? AND mt.id=cm.measure_id
									AND mt.name=? AND mt.cfg=?
									LIMIT 1
								;''',(snapshot_id,measure,json.dumps(measure_cfg, indent=None, sort_keys=True)))
		ans = self.cursor.fetchone()
		if ans is not None:
			return True
		else:
			return False

	def check_excomp(self,snapshot_id,**cfg):
		'''
		Check if a proba_distrib has already been computed, returns bool
		'''
		if self.db_type == 'postgres':
			self.cursor.execute('''SELECT * FROM exact_computation
									WHERE snapshot_id=%s AND cfg=%s
									LIMIT 1
								;''',(snapshot_id,json.dumps(cfg, indent=None, sort_keys=True)))
		else:
			self.cursor.execute('''SELECT * FROM exact_computation
									WHERE snapshot_id=? AND cfg=?
									LIMIT 1
								;''',(snapshot_id,json.dumps(cfg, indent=None, sort_keys=True)))
		ans = self.cursor.fetchone()
		if ans is not None:
			return True
		else:
			return False
