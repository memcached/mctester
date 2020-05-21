# Load tester/fuzzer for memcached.

---

### Goals:

- Allow easy creation of programs to test specific load patterns for long-term
  stability and performance
- Generally for testing against a single instance of memcached
- High but not maximum performance. mc-crusher should instead be used for stress
  testing performance.
- To be used in combination with fault generating systems (ie; toxiproxy)

---

Still in early development.
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

As of writing still essentially a prototype. Under `cmd/server` is a load
tester that runs as an HTTP server. It lets you start multiple types of
workloads, update the current workloads, and stop workloads.

By default it listens on port 11210

example:

```
cd cmd/server ; go build
./server # this currently prints out an example of the JSON blob it expects

curl -X POST -H "Content-Type: application/json" -d '{"name":"toast","type":"basic","worker":{"Servers":["127.0.0.1:11211"],"DesiredConnCount":1,"RequestsPerSleep":1,"RequestBundlesPerConn":1,"SleepPerBundle":1000000,"DeletePercent":0,"KeyLength":10,"KeyPrefix":"mctoaster:","KeySpace":1000,"KeyTTL":180,"UseZipf":false,"ZipfS":1.01,"ZipfV":500,"ValueSize":1000,"ClientFlags":0}}' http://localhost:11210/set
# Test is now running.
curl -X POST -H "Content-Type: application/json" -d '{"name":"toast"}' http://localhost:11210/delete
# Test is now stopped.
```

An incomplete high level TODO for server mode:

- clean up the outputs.
- add some slightly better error handling with backoffs.
- commandline help output and tooling for generating/submitting loaders.
- flesh out the basic text loader a bit.
- add more working examples
- path for listing current loaders.
- easing arguments for slewing between two sets of parameters
  - this one will be a fair bit of work.
- more loader worksets: multiget's, noreply's, meta protocol, etc.
