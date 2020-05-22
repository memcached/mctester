package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"time"
	"bytes"
)

// TODO: think we can pass this to loaderManager() from main()?
var updateChan chan *Loader

// Note to self: can two different flagsets be parsed? if so, a global set
// could always be parsed, but need to supply own usage() output.
func main() {
	// top command: start
	startCmd := flag.NewFlagSet("start", flag.ExitOnError)
	startAddr := startCmd.String("address", "127.0.0.1:11210", "addr:port to listen on")
	cpuprofile := startCmd.String("cpuprofile", "", "dump cpu profile to file")
	startStopAfter := startCmd.Duration("duration", 0, "amount of time to run the test for")

	showCmd := flag.NewFlagSet("show", flag.ExitOnError)
	showType := showCmd.String("type", "basic", "type of loader to show an example of")

	setCmd := flag.NewFlagSet("set", flag.ExitOnError)
	setAddr := setCmd.String("address", "http://localhost:11210", "base URL the load server is running on")
	setFile := setCmd.String("file", "-", "file to read loader config from. stdin by default")

	delCmd := flag.NewFlagSet("delete", flag.ExitOnError)
	delAddr := delCmd.String("address", "http://localhost:11210", "base URL the load server is running on")
	delName := delCmd.String("name", "", "name of loader to stop")

	usage := func() {
		fmt.Println("Usage: server <command> [<args>]")
		fmt.Println("Top level commands are:\n")
		fmt.Println("  start [start the load generator server process]")
		startCmd.PrintDefaults()
		fmt.Println("\n  show [example JSON dumps for available loader types]")
		showCmd.PrintDefaults()
		fmt.Println("\n  set [start or adjust existing workload generator from json file]")
		setCmd.PrintDefaults()
		fmt.Println("\n  delete [stop and remove a workload generator]")
		delCmd.PrintDefaults()
	}

	// Parse out commands
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "start":
		startCmd.Parse(os.Args[2:])
	case "show":
		showCmd.Parse(os.Args[2:])
	case "set":
		setCmd.Parse(os.Args[2:])
	case "delete":
		delCmd.Parse(os.Args[2:])
		if *delName == "" {
			delCmd.PrintDefaults()
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(1)
	}

	if showCmd.Parsed() {
		fmt.Println("Example worker definition")
		fmt.Println("types available: basic")
		wr := WorkerWrapper{Name: "example", LType: "basic"}
		switch *showType {
		case "basic":
			// TODO: Call into loader_basic.go for help output.
			bl := newBasicLoader()
			b, err := json.Marshal(bl)
			if err != nil {
				panic(err)
			}
			wr.Worker = b
		default:
			fmt.Printf("Unknown loader type: %s\n", *showType)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(&wr); err != nil {
			log.Println(err)
		}
	} else if setCmd.Parsed() {
		var data []byte
		var err error
		if *setFile == "-" {
			data, err = ioutil.ReadAll(os.Stdin)
			if err != nil {
				fmt.Println("Error reading file", err)
				os.Exit(1)
			}
		} else {
			data, err = ioutil.ReadFile(*setFile)
			if err != nil {
				fmt.Println("Error reading file", err)
				os.Exit(1)
			}
		}
		resp, err := http.Post(*setAddr + "/set", "Content-Type: application/json", bytes.NewReader(data))
		if err != nil {
			fmt.Println("Error sending loader config to server:", err)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusOK {
			fmt.Printf("successfully send loader update\n")
		} else {
			fmt.Printf("Error: status code [%d]\n", resp.StatusCode)
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Result: %s\n", string(b))
		}
		resp.Body.Close()
	} else if delCmd.Parsed() {
		ws := WorkerStopper{Name: *delName}
		data, err := json.Marshal(ws)
		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.Post(*delAddr + "/delete", "Content-Type: application/json", bytes.NewReader(data))
		if resp.StatusCode == http.StatusOK {
			fmt.Printf("successfully send loader update\n")
		} else {
			fmt.Printf("Error: status code [%d]\n", resp.StatusCode)
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Result: %s\n", string(b))
		}

		resp.Body.Close()
	} else if startCmd.Parsed() {
		fmt.Printf("starting server on: %s\n", *startAddr)
		timeout := time.Second * 0
		if startStopAfter != nil {
			timeout = *startStopAfter
		}
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
			timeout = time.Second * 10
		}

		if timeout != 0 {
			fmt.Printf("limiting runtime.\n")
			go func() {
				time.Sleep(timeout)
				fmt.Printf("time limit reached, exiting.\n")
				os.Exit(0)
			}()
		}

		// TODO: how to stop the server?
		go loaderManager()
		log.Fatal(http.ListenAndServe(*startAddr, nil))
	}
}

type Loader struct {
	Name   string
	LType  string
	Stop   bool
	Worker interface{}
	Update chan interface{}
}

func loaderManager() {
	loaders := make(map[string]*Loader)
	for {
		update := <-updateChan
		//fmt.Printf("loaderManager update: %+v\n", update)
		fmt.Printf("received update for [%s]", update.Name)
		if loader, ok := loaders[update.Name]; ok {
			// loader already exists, update it.
			// Type must match though.
			// TODO: can't surface error to user :(
			if update.Stop {
				fmt.Printf("stopping loader: %s\n", update.Name)
				close(loader.Update)
				delete(loaders, update.Name)
			} else {
				if update.LType != loader.LType {
					fmt.Printf("LType (loader type) didn't match for existing loader: %s", update.LType)
					continue
				}

				fmt.Printf("shipping update to: %s\n", update.Name)
				loader.Update <- update.Worker
			}
		} else if !update.Stop {
			loaders[update.Name] = update
			update.Update = make(chan interface{})
			// spawn run the correct loader for type supplied
			switch update.LType {
			case "basic":
				go runBasicLoader(update.Update, update.Worker)
			default:
				fmt.Printf("unknown loader type: %s", update.LType)
				continue
			}
		}
	}
}

type WorkerWrapper struct {
	Name   string          `json:"name"`
	LType  string          `json:"type"`
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
	switch wrap.LType {
	case "basic":
		t := newBasicLoader()
		if err := json.Unmarshal(wrap.Worker, t); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		l := Loader{Name: wrap.Name, LType: wrap.LType, Worker: t}
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
