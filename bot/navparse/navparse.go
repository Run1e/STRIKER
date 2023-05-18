package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"encoding/json"
	"io/ioutil"
	"path/filepath"

	"github.com/mrazza/gonav"
)

func ParseNav(file string) map[string][][]float32 {
	_, filename := filepath.Split(file)
	mapname := strings.Split(filename, ".")[0]

	f, ok := os.Open(file) // Open the file

	data := map[string][][]float32{}

	if ok != nil {
		fmt.Printf("Failed to open file: %v\n", ok)
		return nil
	}

	defer f.Close()
	start := time.Now()
	parser := gonav.Parser{Reader: f}
	mesh, nerr := parser.Parse() // Parse the file
	elapsed := time.Since(start)

	if nerr != nil {
		fmt.Printf("Failed to parse: %v\n", nerr)
		return nil
	}

	fmt.Printf("%s: parse OK in %v\n", mapname, elapsed)

	for _, curr := range mesh.Places {
		a := [][]float32{}

		for _, area := range curr.Areas {
			vec := area.GetCenter()
			tmp := []float32{vec.X, vec.Y, vec.Z}
			a = append(a, tmp)
		}

		data[curr.Name] = a
	}

	if len(data) == 0 {
		return nil
	}

	return data
}

func main() {
	csgo_dir := os.Args[1]

	files, err := ioutil.ReadDir(csgo_dir)
	if err != nil {
		fmt.Printf("Failed reading directory")
		return
	}

	all := map[string]map[string][][]float32{}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".nav") {
			data := ParseNav(csgo_dir + "/" + file.Name())
			if data != nil {
				_, filename := filepath.Split(file.Name())
				mapname := strings.Split(filename, ".")[0]
				all[mapname] = data
			}
		}
	}

	empData, _ := json.Marshal(all)
	jsonStr := string(empData)
	f, _ := os.Create("nav.json")
	f.WriteString(jsonStr)
	defer f.Close()
}
