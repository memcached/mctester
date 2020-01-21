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

cd cmd/basic ; go build
./basic -h

create new load patterns by making new directories under cmd/

---

### YAML-phobic.

Testing for a specific load pattern should be done by copying top level
binaries (ie; cmd/basic) into a new directory and adding anything specific
into the code. Add or remove command line options as necessary.

I prefer to start from a simple baseline and branch instead of
configuration files and endless boilerplate code for what end up
ultimately being one-offs.

Common tricks should filter up into library code.

Switching between load patterns during a live test can/should be accomplished
by stopping/starting different binaries. Graceful stop/starts in the library
could help with this.
