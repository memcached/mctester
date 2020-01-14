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

create new load patterns by making new directories under cmd/
