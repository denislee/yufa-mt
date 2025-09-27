// main.go
package main

import (
	"fmt"
	"log"
	"net/http"
)

// homeHandler is our function for handling requests to the root URL ("/").
// It takes two arguments:
// 1. http.ResponseWriter: This is where you write your response to (like a pen).
// 2. *http.Request: This contains all the information about the incoming request.
func homeHandler(w http.ResponseWriter, r *http.Request) {
	// We only want to handle requests to the root path.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	// Fprintf writes a formatted string to the ResponseWriter.
	fmt.Fprintf(w, "Hello, World from my Go Web App! 🚀")
}

func main() {
	// http.HandleFunc registers our homeHandler function to handle all requests to the web root ("/")
	http.HandleFunc("/", homeHandler)

	// Announce that the server is starting.
	log.Println("Starting web server on http://localhost:8080")

	// http.ListenAndServe starts an HTTP server on a specific port.
	// It will block until the program is terminated or an error occurs.
	// We use log.Fatal to print any errors and exit the program.
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
