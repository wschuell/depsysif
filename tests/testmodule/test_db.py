import depsysif
import pytest

import os

#### Parameters
dbtype_list = [
	'sqlite',
	'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
	return request.param

##############

#### Tests

def test_createdb(dbtype):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=dbtype)

def test_cleandb(dbtype):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=dbtype)
	db.clean_db()

def test_doublecleandb(dbtype):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=dbtype)
	db.clean_db()
	db.clean_db()

def test_filldb(dbtype):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=dbtype)
	db.clean_db()
	db.init_db()
	current_folder = os.path.dirname(os.path.abspath(__file__))
	csv_folder = os.path.join(current_folder,'test_csvs','basic')
	db.fill_from_csv(folder=csv_folder,headers_present=True)