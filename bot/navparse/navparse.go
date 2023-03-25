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

func ParseNav(save_at string, file string) {
	_, filename := filepath.Split(file)
	mapname := strings.Split(filename, ".")[0]

	f, ok := os.Open(file) // Open the file

	if ok != nil {
		fmt.Printf("Failed to open file: %v\n", ok)
		return
	}

	defer f.Close()
	start := time.Now()
	parser := gonav.Parser{Reader: f}
	mesh, nerr := parser.Parse() // Parse the file
	elapsed := time.Since(start)

	if nerr != nil {
		fmt.Printf("Failed to parse: %v\n", nerr)
		return
	}

	fmt.Printf("%s: parse OK in %v\n", mapname, elapsed)

	data := map[string][][]float32{}

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
		return
	}

	empData, _ := json.Marshal(data)
	jsonStr := string(empData)

	f, _ = os.Create(save_at + "/" + mapname + ".json")

	f.WriteString(jsonStr)

	defer f.Close()
}

func main() {
	save_at := os.Args[1]
	csgo_dir := os.Args[2]

	files, err := ioutil.ReadDir(csgo_dir)
	if err != nil {
		fmt.Printf("Failed reading directory")
		return
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".nav") {
			ParseNav(save_at, csgo_dir+"/"+file.Name())

		}
	}

}
