Failure scenarios:

```
bundle exec ruby run.rb 500 4 raw
# recorded transactions count is OK
# balance should be 20000 but it is 13370.
# counter is OK

bundle exec ruby run.rb 100 1 locker mongoid
# recorded transactions count is OK
# balance should be 1000 but it is 860.
# counter is OK

bundle exec ruby run.rb 100 4 raw
# recorded transactions count is OK
# balance should be 4000 but it is 3210.
# counter is OK

```
