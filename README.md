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
db = Database(**kwargs)

# alternatives:
db.fill_from_crates()
db.fill_from_crates(optional=True)
db.fill_from_crates(optional=True,dependency_types=['0'])
db.fill_from_libio()
```
