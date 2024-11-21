# GCP-distributed-lock
A robust algorithm for a distributed lock on google cloud storage.

This algorithm is based on this <a href="https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html">article</a>. 

The basic parameters are:
- ttl: how long a lock will be considered active
- refresh_interval: how often the daemon will update the expiration timestamp on the file lock
- max_refresh_failures: how many times will the daemon retry to update the lock metadata in case of an error
- identity: the process id, needs to be unique across the different processes that want to acquire the lock

