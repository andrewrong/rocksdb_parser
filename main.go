package main

import (
	"flag"
	"fmt"
	"log"

	"rocksdb_parser/rocksdb"
)

func main() {
	var ldbPath string
	var sstdumpPath string
	var dbPath string
	var manifestPath string
	var level string
	var numLevel int

	flag.StringVar(&ldbPath, "ldb", "", "ldb path")
	flag.StringVar(&sstdumpPath, "sstdump", "", "sstdump path")
	flag.StringVar(&dbPath, "dbPath", "", "db path")
	flag.StringVar(&manifestPath, "manifest", "", "manifest path")
	flag.StringVar(&level, "level", "", "指定level")
	flag.IntVar(&numLevel, "numLevel", 0, "0-numLevel")

	flag.Parse()

	log.Println("ldbPath:", ldbPath)
	log.Println("sstdumpPath:", sstdumpPath)
	log.Println("dbPath:", dbPath)
	log.Println("manifestPath:", manifestPath)
	log.Println("level:", level)
	log.Println("level num:", numLevel)

	if level != "" && numLevel != 0 {
		log.Printf("level and numLevel can't be set at the same time")
		return
	}

	if level == "" && numLevel == 0 {
		log.Printf("level and numLevel can't be empty at the same time")
		return
	}

	manifest, err := rocksdb.GetManifest(manifestPath, ldbPath, sstdumpPath, dbPath)
	if err != nil {
		log.Printf("GetManifest is err:%s \n", err.Error())
		return
	}

	if level != "" {
		err = manifest.ParseSstables(level)
		if err != nil {
			log.Printf("parse sstable is error:%s\n", err.Error())
			return
		}
	} else {
		for i := 0; i <= numLevel; i++ {
			err = manifest.ParseSstables("level " + fmt.Sprintf("%d", i))
			if err != nil {
				log.Printf("parse sstable is error:%s\n", err.Error())
				return
			}
		}
	}

	manifest.Analysis()

}
