import depsysif
import pytest

dbtype_list = [
	'sqlite',
	'postgres'
	]
@pytest.fixture(params=dbtype_list)
def dbtype(request):
	return request.param

def test_createdb(dbtype):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=dbtype)

def test_cleandb(dbtype):
	db = depsysif.database.Database(db_name='travis_ci_test_depsysif',db_type=dbtype)
	db.clean_db()