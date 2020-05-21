package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"net/http"
	"log"
	"encoding/json"
	"io/ioutil"
)

var cpuprofile = flag.String("cpuprofile", "", "dump cpu profile to file")
var memprofile = flag.String("memprofile", "", "dump memory profile")
var updateChan chan *Loader

func main() {
	// TODO: use FlagSet's to separate stuff.
	// "global" flag.
	addr := flag.String("address", ":11210", "addr:port to listen on")

	// "basic" flags.
	fmt.Printf("Example worker definition:")
	{
		bl := newBasicLoader()
		b, err := json.Marshal(bl)
		if err != nil {
			panic(err)
		}
		wr := WorkerWrapper{Name: "example", WType: "basic", Worker: b}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(&wr); err != nil {
			log.Println(err)
		}
	}

	flag.Parse()

	fmt.Printf("starting on: %s\n", *addr)
	updateChan = make(chan *Loader)

	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/delete", deleteHandler)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println(err)
			return
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		// TODO: Use a real timer with channel.
		//now := time.Now()
		//bl.stopAfter = now.Add(time.Second * 10)
		//fmt.Printf("time: %v\n", bl.stopAfter)
	}

	// TODO: how to stop the server?
	go loaderManager()
	log.Fatal(http.ListenAndServe(*addr, nil))
	//bl.Run()
}

type Loader struct {
	Name string
	WType string
	Stop bool
	Worker interface{}
	Update chan interface{}
}

func loaderManager() {
	loaders := make(map[string]*Loader)
	for {
		update := <-updateChan
		fmt.Printf("loaderManager update: %+v\n", update)
		if loader, ok := loaders[update.Name]; ok {
			// loader already exists, update it.
			// Type must match though.
			// TODO: can't surface error to user :(
			if update.Stop {
				fmt.Printf("stopping loader: %s\n", update.Name)
				close(loader.Update)
				delete(loaders, update.Name)
			} else {
				if update.WType != loader.WType {
					// TODO: pass name?
					// TODO: Not Fatal?
					log.Fatal("WType (worker type) didn't match for existing loader")
					continue
				}

				fmt.Printf("shipping update to: %s\n", update.Name)
				loader.Update <- update.Worker
			}
		} else {
			loaders[update.Name] = update
			update.Update = make(chan interface{})
			// run the correct loader for type supplied
			// TODO: necessary? let the loader type assert instead?
			switch update.WType {
			case "basic":
				go runBasicLoader(update.Update, update.Worker)
			default:
				log.Fatal("unknown loader type")
				continue
			}
		}
	}
}

type WorkerWrapper struct {
	Name string `json:"name"`
	WType string `json:"type"`
	Worker json.RawMessage `json:"worker"`
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	// We'll potentially be decoding into different base types
	// ie; different protocols/etc.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// have the body out, so can do a RawMessage decode.
	var wrap WorkerWrapper

	if err := json.Unmarshal(body, &wrap); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Unwrap here since we can still ship an error to the user.
	switch wrap.WType {
	case "basic":
		t := newBasicLoader()
		if err := json.Unmarshal(wrap.Worker, t); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		l := Loader{Name: wrap.Name, WType: wrap.WType, Worker: t}
		updateChan <- &l
	default:
		http.Error(w, "unknown worker type", http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "set complete\n")
}

// At some point I'll grok Go well enough to not do shit like this.
type WorkerStopper struct {
	Name string `json:"name"`
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	var stop WorkerStopper
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&stop); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Now that we know the name, create a special empty Loader with a stop
	// indicator. If this isn't idiomatic I've no idea what is.
	updateChan <- &Loader{Name: stop.Name, Stop: true}

	fmt.Fprintf(w, "delete issued\n")
}
