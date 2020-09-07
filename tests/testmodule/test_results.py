import depsysif
import pytest
import datetime
import os
import time

#### Parameters
dbtype_list = [
	'sqlite',
	'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
	return request.param

date_list = [
	'2014-02-05 00:11:22',
	]
@pytest.fixture(params=date_list)
def timestamp(request):
	return request.param

implementation_list = [
	'classic',
	'matrix'
	]
@pytest.fixture(params=implementation_list)
def implementation(request):
	return request.param

@pytest.fixture(params=dbtype_list)
def testdb(request):
	time.sleep(0.2) # Tests on travis can fail because of operations being too quick and sqlite db being locked
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=request.param)
	db.clean_db()
	db.init_db()
	current_folder = os.path.dirname(os.path.abspath(__file__))
	csv_folder = os.path.join(current_folder,'test_csvs','basic')
	db.fill_from_csv(folder=csv_folder,headers_present=True)
	return db

@pytest.fixture(params=dbtype_list)
def testnetdb(request):
	time.sleep(0.2) # Tests on travis can fail because of operations being too quick and sqlite db being locked
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=request.param)
	db.clean_db()
	db.init_db()
	current_folder = os.path.dirname(os.path.abspath(__file__))
	csv_folder = os.path.join(current_folder,'test_csvs','smalltestnet')
	db.fill_from_csv(folder=csv_folder,headers_present=True)
	return db


propag_proba_list = [
	0.1,
	0.28,
	0.5,
	1
	]
@pytest.fixture(params=propag_proba_list)
def propag_proba(request):
	return request.param

normexp_list = [
	0,
	1,
	0.5,
	0.3,
	2
	]
@pytest.fixture(params=normexp_list)
def norm_exponent(request):
	return request.param

##############

#### Tests


def test_exp_manager(testdb,timestamp):
	xp_man = depsysif.experiment_manager.ExperimentManager(db=testdb)
	xp_man.run_simulations(snapshot_time=timestamp,nb_sim=10)
	snapid = testdb.get_snapshot_id(snapshot_id=timestamp)
	xp_man.get_results(snapshot_id=snapid,result_type='count',failing_project=1,nb_sim=10)
	xp_man.get_results(snapshot_id=snapid,result_type='nb_failing',failing_project=1,nb_sim=10)
	xp_man.get_results_full(snapshot_id=snapid,result_type='count',nb_sim=10)
	xp_man.get_results_full(snapshot_id=snapid,result_type='nb_failing',nb_sim=10)
