/*
	gonav - A Source Engine navigation mesh file parser written in Go.
	Copyright (C) 2016  Matt Razza

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published
	by the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"fmt"
	"os"
	"time"
	"strings"

	"path/filepath"
	"encoding/json"

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

	empData, _ := json.Marshal(data)
	jsonStr := string(empData)

	f, _ = os.Create(save_at + "/" + mapname + ".json")

	f.WriteString(jsonStr)

	defer f.Close()
}

func main() {
	save_at := os.Args[1]
	args := os.Args[2:]

	for _, file := range args {
		ParseNav(save_at, file)
	}
}