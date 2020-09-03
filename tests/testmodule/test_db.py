import depsysif
import pytest
import datetime
import os

#### Parameters
dbtype_list = [
	'sqlite',
	'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
	return request.param

fullnetwork_list = [
	True,
	False
	]
@pytest.fixture(params=fullnetwork_list)
def fullnetwork(request):
	return request.param


date_list = [
	'2014-01-05',
	'2014-02-05 00:11:22',
	datetime.datetime.now()
	]
@pytest.fixture(params=date_list)
def timestamp(request):
	return request.param

@pytest.fixture(params=dbtype_list)
def testdb(request):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=request.param)
	db.clean_db()
	db.init_db()
	current_folder = os.path.dirname(os.path.abspath(__file__))
	csv_folder = os.path.join(current_folder,'test_csvs','basic')
	db.fill_from_csv(folder=csv_folder,headers_present=True)
	return db

##############

#### Tests

def test_createdb(dbtype):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=dbtype,clean_first=True)

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

def test_snapshot(testdb,timestamp,fullnetwork):
	testdb.build_snapshot(t=timestamp,full_network=fullnetwork)


def test_snapshot_getnet(testdb,timestamp,fullnetwork):
	testdb.build_snapshot(t=timestamp,full_network=fullnetwork)
	testdb.get_network(snapshot_time=timestamp,full_network=fullnetwork)