import depsysif
import pytest
import datetime
import os
import time

#### Parameters
dbtype_list = [
	'sqlite',
	# 'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
	return request.param

date_list = [
	'2014-02-05 00:11:22',
	datetime.datetime.now(),
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

def test_network(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)

def test_cycles(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	snapid = testdb.get_snapshot_id(snapshot_time=timestamp)
	assert testdb.detect_cycles(snapshot_id=snapid) == []

def test_create_simulation(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1)


def test_run_simulation(testdb,timestamp,implementation):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1,implementation=implementation)
	sim.run()

def test_register_simulation(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1)
	snapid = testdb.get_snapshot_id(snapshot_time=timestamp)
	testdb.register_simulation(simulation=sim,snapshot_id=snapid)


def test_submit_results(testdb,timestamp,implementation):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1,implementation=implementation)
	snapid = testdb.get_snapshot_id(snapshot_time=timestamp)
	sim.run()
	testdb.submit_simulation_results(simulation=sim,snapshot_id=snapid)


def test_submit_with_snapid_set(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1)
	snapid = testdb.get_snapshot_id(snapshot_time=timestamp)
	sim.run()
	sim.snapshot_id = snapid
	testdb.submit_simulation_results(simulation=sim,snapshot_id=snapid)

def test_exp_manager(testdb,timestamp):
	xp_man = depsysif.experiment_manager.ExperimentManager(db=testdb)
	xp_man.run_simulations(snapshot_time=timestamp,nb_sim=10,failing_project=1)

def test_exp_manager_allprojects(testdb,timestamp):
	xp_man = depsysif.experiment_manager.ExperimentManager(db=testdb)
	xp_man.run_simulations(snapshot_time=timestamp,nb_sim=10,failing_project=None)



## On testnet
def test_simresult(testnetdb,implementation):
	net = testnetdb.get_network(snapshot_time=None) # when None, taking max time in db
	sim = depsysif.simulations.Simulation(network=net,failing_project=3,propag_proba=1,implementation=implementation)
	sim.run()
	assert (sim.results['ids'] == [3,4,5,6]).all()

	sim = depsysif.simulations.Simulation(network=net,failing_project=7,propag_proba=1,implementation=implementation)
	sim.run()
	assert (sim.results['ids'] == [4,7]).all()


def test_sim_mat(testnetdb,propag_proba,norm_exponent):
	net = testnetdb.get_network(snapshot_time=None) # when None, taking max time in db
	sim = depsysif.simulations.Simulation(network=net,failing_project=3,propag_proba=propag_proba,norm_exponent=norm_exponent,implementation=implementation)
	sim.propag_mat[2-1,3-1] == propag_proba
	sim.propag_mat[2-1,5-1] == 0
	sim.propag_mat[7-1,4-1] == propag_proba/2**norm_exponent
