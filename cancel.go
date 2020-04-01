package main

import (
    "log"
    "net/http"
    //"time"
    "io"
    "fmt"
)

func main() {

    // create a server

    // convert it to http2

    srv := &http.Server{
	Addr:	":5000",
	Handler: defaultHandler{},
    }

    log.Fatal(srv.ListenAndServeTLS("server.crt", "server.key"))

}

type defaultHandler struct {}

func (hand defaultHandler) ServeHTTP(w http.ResponseWriter, r *http.Request){
	// r.Context().Done()
	// goroutine to handle cancellation
	// go func() {}

	if r.ProtoMajor != 2 {

	    fmt.Fprintf(w, "Not a HTTP/2 Request")
	    return
	}

	buf := make([]byte, 10)

	f, flushOK := w.(http.Flusher)

	if !flushOK {
	    fmt.Fprintf(w, "Can't Stream Requests")
	    return
	}

	for {
	    n, err := r.Body.Read(buf)
	    if n > 0 {
		w.Write(buf[:n])
		log.Printf("Flushing Message")
		f.Flush()
	    }

	    if err != nil {
		if err == io.EOF {
		    w.Header().Set("Status", "200 OK")
		    r.Body.Close()
		}
		break
	    }
	}



	/*
	payload := []byte(`{"message": "this is json"}`)
	waiting := time.After(time.Duration(1) * time.Second)

	select {
		case<-waiting:

		    for i:=0; i<len(payload)/5; i++{
			start := i*5
			end := i*5+5
			log.Printf("Wrote 5 Bits")
			w.Write(payload[start:end])
		    }

		case <- r.Context().Done():
		    log.Printf("Client Gave Up")
	}
	*/

	// await a slow response
	/*select {
	case <- time.After(time.Duration(3) * time.Second):
	    fmt.Fprintf(w, "Hello World")

	case <- r.Context().Done():
	    log.Printf("Client Gave Up")
	}*/


	// slowly stream a response back

}
