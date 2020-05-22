# Load tester/fuzzer for memcached.

---

### Goals:

- Allow easy creation of programs to test specific load patterns for long-term
  stability evaluation.
- Generally for testing against a single instance of memcached
- High but not maximum performance. mc-crusher should instead be used for stress
  testing performance.
- Can be used in combination with fault generating systems (ie; toxiproxy)

---

```
cd cmd/basic ; go build
./basic -h
```
create new load patterns by making new directories under cmd/

---

### Kitchen-sink phobic.

Testing for a novel load pattern should be done by copying top level
binaries (ie; cmd/basic) into a new directory and adding anything specific
into the code. Add or remove command line options as necessary.

Common tricks should filter up into library code.

### Server loader.

Under `cmd/server` is a load
tester that runs as an HTTP server. It lets you start multiple types of
workloads, update current workloads, and stop workloads.

By default it listens on port 11210 with HTTP

example:

```
cd cmd/server ; go build
# Print out usage and defaults
./server

# Starts the server, listening on default port
./server start

# start memcached somewhere else
memcached -m 256 -d

# Get a template JSON file for the basic loader
./server show --type basic

# modify the output to your liking, save as "foo.json"
# Set the name to "toast" inside foo.json

# Start a workload
./server set --file ./foo.json

# A persistent workload is now running.
# Edit the .json file slightly, but keep the name the same.
./server set --file ./foo.json

# The existing workload has been updated.
# Note: not all parameters can be updated at runtime.
# File a bug if something doesn't update and is important to you!

# Now, stop the workload.
./server delete --name "toast"

# Workload is now stopped
```

Once you have a workload tuned, keep a few json files around and write a shell
script to execute your workload. This can be used to emulate multiple types of
clients running against the same server, peak/antipeak, fluctuations, mass
spikes, and so on.
