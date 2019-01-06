## Motivation

We noticed that locks set with [mongoid-locker](https://github.com/mongoid/mongoid-locker) gem allows more than one process
to enter a critical section. We noticed that it happens when a locked record is updated by two threads
both inside and outside the lock.
This repository is a [MCVE](https://stackoverflow.com/help/mcve) for this bug.

## Test scenario

`Wallet` model has multiple `Transactions`. When a transaction is added to a wallet, `Wallet#balance` should
be updated according to the new transaction. Saving a transaction and updating a wallet should be performed
inside a lock, to ensure wallet consistency. After saving a transaction and updating the balance, addiional
data is saves in walled (because it causes the bug we're hunting).

We run multiple threads and processes concurrently, every thread adds a transaction with amount of 10.
When all them finish, we check if wallet balance is changed by `10*thread_count*process_count`. If it's
not, it's a sign that more than one thread were inside the critical section.

We have prepared multiple test cases to find out where the bug is introduced.

1. `test/mongoid_locker_internal_test.rb` - mongoid locker used in `Wallet` model
1. `test/mongoid_locker_external_test.rb` - mongoid locker used in a model associated with`Wallet` model
1. `test/raw_test.rb` - mongoid locker approach to locking reimplemented using raw mongo queries;
without mongoid, just mongo ruby driver
1. `test/mongoid_expirable_test.rb` - a lock implemented using mongo unique expirable indexes instead of timestamp fields

## Results

Our tests show that only the last approach (lock based on unique index) is reliable. All implementation based on timestamp fields cause concurrency issues. As we were able to reproduce it in raw setting (just ruby driver, without mongoid nor mongoid-locker), the most probable source of the bug lays in mongo ruby driver or in mongo datastore itself.

## Executing tests

Number of threads and processes is confiugured with env vars  `THREAD_COUNT` and `PROCESS_COUNT`

All tests:
```
$ rake
```

### Failure configurations
```
THREAD_COUNT=100 PROCESS_COUNT=2 bundle exec ruby test/mongoid_locker_internal_test.rb
THREAD_COUNT=500 PROCESS_COUNT=2 bundle exec ruby test/raw_test.rb

THREAD_COUNT=500 PROCESS_COUNT=4 bundle exec ruby test/mongoid_locker_external_test.rb
# the last one cause connection limit error on mongo 4, but on mongo 2 it run, but caused
# locking issues
```
