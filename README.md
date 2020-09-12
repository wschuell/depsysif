# DepSysIf

Studying systemic risk in dependency hierarchies of open-source software development.


# Installing

Master is stable, develop is the latest common version.

To install, clone the repo, checkout to the wanted branch, and run:
`python3 setup.py develop`

You can then import the module elsewhere with `import depsysif`.

# Testing

Run pytest at the root of the repository.
Tests automatically try to access a postgres DB named 'travis_ci_test_depsysif', with user postgres on port 5432.
No implemented way yet to modify the port/user/dbname or deactivate the corresponding tests altogether.


# Structure and classes

### Database:

Wrapper around data manipulation.

### ExperimentManager:

Wrapper around higher-level data manipulation: launching simulations, getting results, measures etc.
Could be included in Database as a super-class, but in this way the two interfaces are more clearly separated.

### Simulation:


# Importing data

```
db = Database(db_type='sqlite',db_name='depsysif',db_folder='./')

# Filling alternatives, just run one of them:
# From a DB created from crates.io data. You may need to change args for postgres: username, port, host, db_name
db.fill_from_crates()
db.fill_from_crates(optional=True)
db.fill_from_crates(optional=True,dependency_types=['0'])
# From a DB created from libraries.io  You may need to change args for postgres: username, port, host, db_name
db.fill_from_libio()
db.fill_from_libio(platform='Cargo')
db.fill_from_libio(dependency_types=['runtime','normal'])

# Trimming cycles (deleted_dependencies table in the DB keeps track of them)
db.delete_autorefs() # cycles of length 1
db.delete_from_list(dep_list=) # deleting specific
db.delete_from_list(filename=depsysif_path+'/depsysif/deps_to_delete.txt') # This example file contains known cycle-inducing dependencies for cargo projects, you can provide any other file with a list of lines with this syntax :<source id or name>,<target id or name>

```

# Processing data

```
# Building snapshots of the state of the dependency network
db.build_snapshot(snapshot_time='2018-12-10')
db.build_snapshot(snapshot_time='2019-09-02')

# Getting the experiment manager
xp_man = ExperimentManager(db=db)

# Running simulations, nb_sim iterations for each configuration (ie for all snapshots, for all source project IDs):
xp_man.run_simulations(nb_sim=10) # You can provide other arguments here as well, like norm_exponent (default 0), implementation ('classic' or 'matrix') and propag_proba (default 0.9)

# Computing measures
xp_man.compute_measure(measure='in_degree')
xp_man.compute_measure(measure='mean_cascade_length')

# Plot measures (measures can be seen in the measures.py file, others are to be implemented)
from matplotlib import pyplot as plt
for p in ['chrono','datetime','serde','log','rand']:
    xp_man.plot_measure(measure='mean_cascade_length',project_name=p,show=False)
plt.legend()
plt.show() # shows a plot with a curve for each package, (x,y)=(time,measure), one point per snapshot

#Compute exact probability distributions
xp_man.compute_exact_proba(implementation='network') # or implementation='matrix'

#Plot proba distribution
<measure to be implemented>
```
