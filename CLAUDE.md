# pg_reflex: A postgres incremental view maintenance package

The goal of this package is to create new tables, that replace Materialized views and enable users with fast and automatic updates.
This package is built in rust using pgrx.

Read more on the purpose of this package in the README.md file.


# Test suite

We can test and validate the package with:
- cargo pgrx check
- cargo pgrx test
- cargo clippy
- cargo fmt


# Benchmarks

There is a whole folder of benchmark scripts. Some of them use the local postgres, localhost and postgres user.
The db is called db_clone, and there's quite a lot of materialized views that could be replaced by IMVs
Check all the files in `/home/diviyan/fentech/algorithm/api/base-db-anchor-evm/base_db/sql`
to have all the view and index definitions.


# Development

You need to be really critic on the proposed modifications or approaches and fully comprehend the scope of the modifications ; what does it imply?
Is there a need for such modification? Is it worth the hassle/additional complexity in the code?

The development is always in these steps:

- Code tests (and do not modify them afterwards)
- Implement
- Test correctness
- Benchmark
- Evaluate the worth of the new development? it it worth it?
- If modification kept, then try to optimize it entering in a new development cycle.

There's a few development journals in `discussion*.md` files.


# Priorities

The main goal of this package is correctness. There is no use in such a package if the results are not correct. And trust in it would be broken.
The second goal is not over-complexifying the code of the package. Be simple and straightforward in the implementation.
And the third goal is performance

# Coding practices

We value code that explains itself through clear class, method, and variable names. Comments may be used when necessary to explain some tricky logic or for documentation, but should be avoided otherwise.
