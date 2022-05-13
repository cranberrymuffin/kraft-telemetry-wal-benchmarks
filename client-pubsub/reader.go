package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func readJSon() {
	// Open our jsonFile
	file, err := os.Open("../data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K

	for scanner.Scan() {
		fmt.Println(scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}
