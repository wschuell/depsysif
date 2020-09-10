import numpy as np
from . import simulations

###################
###################
def in_degree(snapshot_id,xp_man):
	'''
	in degree of each project
	'''

	net = xp_man.db.get_network(snapshot_id=snapshot_id)

	value_vec = []
	projid_vec = []
	for k,v in dict(net.in_degree()).items():
		value_vec.append(v)
		projid_vec.append(k)

	return np.asarray(value_vec),np.asarray(projid_vec)

###################
def complete_cfg_in_degree(**measure_cfg):
	return {}
###################
###################


###################
###################
def out_degree(snapshot_id,xp_man):
	'''
	out degree of each project
	'''
	net = xp_man.db.get_network(snapshot_id=snapshot_id)

	value_vec = []
	projid_vec = []
	for k,v in dict(net.out_degree()).items():
		value_vec.append(v)
		projid_vec.append(k)

	return np.asarray(value_vec),np.asarray(projid_vec)

###################
def complete_cfg_out_degree(**measure_cfg):
	return {}
###################
###################


###################
###################
def mean_cascade_length(snapshot_id,xp_man,nb_sim,**sim_cfg):
	'''
	mean_cascade_length for each project as in number of affected projects when used as a source of failure, average over number of available simulations
	'''
	results = xp_man.get_results(result_type='nb_failing',aggregated=True,snapshot_id=snapshot_id,nb_sim=nb_sim,**sim_cfg)
	# if xp_man.db.db_type == 'postgres':
	# 	xp_man.db.cursor.execute('SELECT COUNT(*) FROM simulations WHERE snapshot_id=%s GROUP BY failing_project LIMIT 1;',(snapshot_id,))
	# else:
	# 	xp_man.db.cursor.execute('SELECT COUNT(*) FROM simulations WHERE snapshot_id=? GROUP BY failing_project LIMIT 1;',(snapshot_id,))
	# nb_sim = xp_man.db.cursor.fetchone()[0]
	# results = results/nb_sim
	id_vec = xp_man.get_id_vector(snapshot_id=snapshot_id)

	return results,id_vec

###################
def complete_cfg_mean_cascade_length(nb_sim,**measure_cfg):
	cfg = simulations.Simulation.complete_sim_cfg(in_place=False,**measure_cfg)
	cfg['nb_sim'] = nb_sim
	return cfg
###################
###################