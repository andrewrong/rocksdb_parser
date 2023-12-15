package rocksdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"
)

type OpType = string

const (
	TypeDelete        OpType = "delete"
	TypeValue         OpType = "value"
	TypeMerge         OpType = "merge"
	TypeSingeDeletion OpType = "singleDeletion"
	TypeElse          OpType = "else"
)

type SstableInfo struct {
	Filename string
	Max      string
	Min      string
	Size     string
}

type Manifest struct {
	LevelNum      int32
	LevelFiles    map[string][]*SstableInfo
	Valid         bool
	LevelSstables map[string]map[string]*Sstable
	Dir           string
	LdbPath       string
	SstDumpPath   string
	lock          sync.Mutex //保护LevelSstables
}

func GetManifest(filePath, ldbPath, sstPath, dbPath string) (*Manifest, error) {
	if len(filePath) == 0 || len(ldbPath) == 0 || len(sstPath) == 0 {
		return nil, errors.New("filePath/cmdPath is invalid")
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, errors.New(filePath + " is not exist")
	}
	//./ldb manifest_dump --hex --path=/disk5/mgo_lifecycle_v2/db/MANIFEST-5881114
	cmd := exec.Command(ldbPath, "manifest_dump", "--hex", "--path="+filePath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Manifest dump %s is error: %s\n", filePath, err)
		return nil, err
	}
	contentStr := string(out)
	log.Printf("Manifest dump %s is success, len is %d\n", filePath, len(contentStr))

	tmp := Manifest{
		LevelNum:      0,
		LevelFiles:    make(map[string][]*SstableInfo),
		Valid:         true,
		LevelSstables: make(map[string]map[string]*Sstable),
		Dir:           dbPath,
		LdbPath:       ldbPath,
		SstDumpPath:   sstPath,
	}

	startIdx := 0
	endIdx := len(contentStr)
	valid := true
	scope := false
	currentLevel := ""

	for {
		if startIdx >= endIdx {
			break
		}
		idx := strings.Index(contentStr[startIdx:], "\n")
		if idx == -1 {
			valid = false
			log.Printf("manifest index '\n' not found\n")
			break
		} else {
			log.Printf("line:%s\n", contentStr[startIdx:startIdx+idx])
		}

		levelIdx := strings.Index(contentStr[startIdx:startIdx+idx], "level ")
		if levelIdx == -1 {
			if scope {
				elems := strings.Split(contentStr[startIdx:startIdx+idx], ":")
				if len(elems) < 2 {
					scope = false
				} else {
					if _, ok := tmp.LevelFiles[currentLevel]; !ok {
						tmp.LevelFiles[currentLevel] = make([]*SstableInfo, 0)
					}
					info := SstableInfo{
						Filename: strings.TrimSpace(elems[0]),
					}
					// 312169:270758074['00000011648465F100000026' seq:52258151830, type:1 .. '000000116484845D000000B3' seq:52270611945, type:1]
					sli := strings.Split(strings.TrimSpace(contentStr[startIdx:startIdx+idx]), "'")
					info.Min = sli[1]
					info.Max = sli[3]
					info.Size = strings.TrimSuffix(strings.Split(sli[0], ":")[1], "[")

					tmp.LevelFiles[currentLevel] = append(tmp.LevelFiles[currentLevel], &info)
				}
			}
		} else {
			level := contentStr[startIdx+levelIdx : startIdx+levelIdx+7]
			currentLevel = level
			tmp.LevelNum++
			scope = true
		}
		startIdx += idx + 1
	}

	if valid {
		log.Printf("parse manifest is ok\n")
		tmp.Valid = true

		data, _ := json.Marshal(tmp)
		log.Printf("%s\n", string(data))
	} else {
		tmp.Valid = false
		log.Printf("parse manifest is error\n")
	}
	return &tmp, nil
}

func (this *Manifest) ParseSstables(level string) error {
	elems := this.LevelFiles[level]
	if len(elems) == 0 {
		log.Printf("this %s level not files", level)
		return nil
	}
	this.LevelSstables[level] = make(map[string]*Sstable)

	wg := sync.WaitGroup{}
	wg.Add(len(elems))
	s := semaphore.NewWeighted(5)

	for _, f := range elems {
		tmpf := strings.TrimSpace(f.Filename)
		s.Acquire(context.Background(), 1)
		go func(info *SstableInfo) {
			log.Printf("current parse sstable:%s \n", this.Dir+"/"+tmpf+".sst")
			tmp, err := GetSStable(this.Dir, tmpf+".sst", this.SstDumpPath, level)
			if err == nil {
				this.lock.Lock()
				tmp.Min = info.Min
				tmp.Max = info.Max
				tmp.Size = info.Size
				this.LevelSstables[level][tmpf] = tmp
				this.lock.Unlock()

				data, _ := json.Marshal(tmp)
				log.Printf("sstable:%s\n", string(data))
			} else {
				fmt.Errorf("parse %s sstable is error: %s\n", this.Dir+"/"+tmpf+".sst", err.Error())
			}
			s.Release(1)
			wg.Done()
		}(f)
	}

	wg.Wait()
	log.Printf("level %s sstables parse are ok\n", level)

	data, _ := json.Marshal(this.LevelSstables)
	log.Printf("sstables:%s\n", string(data))
	return nil
}

func (this *Manifest) Analysis() {
	for k, v := range this.LevelSstables {
		log.Printf("%s \n", k)
		delete := int64(0)
		val := int64(0)
		merge := int64(0)
		singleD := int64(0)
		els := int64(0)

		if v == nil {
			continue
		}

		for _, item := range v {
			if item == nil {
				continue
			}

			delete += item.OpsCount[TypeDelete]
			val += item.OpsCount[TypeValue]
			merge += item.OpsCount[TypeMerge]
			singleD += item.OpsCount[TypeSingeDeletion]
			els += item.OpsCount[TypeElse]
		}
		log.Printf("delete: %v \n", delete)
		log.Printf("val: %v \n", val)
		log.Printf("singleD: %v \n", singleD)
		log.Printf("merge: %v \n", merge)
		log.Printf("els: %v \n", els)
	}
}

type Sstable struct {
	KeyCount int64
	OpsCount map[OpType]int64
	FileName string
	LevelIdx string
	Min      string
	Max      string
	Size     string
}

var findReg = regexp.MustCompile(`type:[0-9]`)

func GetSStable(dir, filename string, cmdPath string, levelIdx string) (*Sstable, error) {
	if len(dir) == 0 || len(filename) == 0 || len(cmdPath) == 0 {
		return nil, errors.New("dir/filename is invalid")
	}

	filePath := dir + "/" + filename
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, errors.New(filePath + " is not exist")
	}

	tmp := Sstable{
		KeyCount: 0,
		OpsCount: make(map[OpType]int64),
		FileName: filename,
		LevelIdx: levelIdx,
	}

	tmp.OpsCount[TypeDelete] = 0
	tmp.OpsCount[TypeValue] = 0
	tmp.OpsCount[TypeMerge] = 0
	tmp.OpsCount[TypeSingeDeletion] = 0
	tmp.OpsCount[TypeElse] = 0

	cmd := exec.Command(cmdPath, "--file="+filePath, "--command=scan", "--output_hex")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("sstable dump %s is error: %s\n", filePath, err)
		return nil, err
	}
	contentStr := string(out)
	log.Printf("sstable dump %s is success, len is %d\n", filePath, len(contentStr))

	if len(contentStr) == 0 {
		log.Printf("sstable %s content is 0\n", filePath)
		return &tmp, nil
	}
	res := findReg.FindAllString(contentStr, -1)
	log.Printf("type len:%d \n", len(res))
	if res == nil {
		return &tmp, nil
	} else {
		tmp.KeyCount = int64(len(res))
		for _, item := range res {
			if item == "type:0" {
				tmp.OpsCount[TypeDelete]++
			} else if item == "type:1" {
				tmp.OpsCount[TypeValue]++
			} else if item == "type:2" {
				tmp.OpsCount[TypeMerge]++
			} else if item == "type:7" {
				tmp.OpsCount[TypeSingeDeletion]++
			} else {
				if _, ok := tmp.OpsCount[item]; ok {
					tmp.OpsCount[item]++
				} else {
					tmp.OpsCount[item] = 1
				}
				tmp.OpsCount[TypeElse]++
			}
		}
	}

	return &tmp, nil
}
