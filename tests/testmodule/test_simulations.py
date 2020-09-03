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

date_list = [
	'2014-02-05 00:11:22',
	datetime.datetime.now(),
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

def test_create_simulation(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1)


def test_run_simulation(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1)
	sim.run()

def test_register_simulation(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1)
	snapid = testdb.get_snapshot_id(snapshot_time=timestamp)
	testdb.register_simulation(simulation=sim,snapshot_id=snapid)


def test_submit_results(testdb,timestamp):
	net = testdb.get_network(snapshot_time=timestamp)
	sim = depsysif.simulations.Simulation(network=net,failing_project=1)
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
