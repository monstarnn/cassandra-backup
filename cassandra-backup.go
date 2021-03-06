package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ghodss/yaml"
)

const (
	pathSeparator = string(filepath.Separator)
	backupPrefix  = "backup-"
	extTarGz      = ".tar.gz"
)

var (
	awsBucket           string
	dataFileDirectories []string
)

func init() {

	var err error
	if dataFileDirectories, err = cassandraDataFileDirectories(); err != nil {
		log.Fatal("Cassandra error: ", err)
		os.Exit(2)
	}

	_, _, _, awsBucket =
		getEnv("AWS_ACCESS_KEY_ID"), getEnv("AWS_SECRET_ACCESS_KEY"),
		getEnv("AWS_DEFAULT_REGION"), getEnv("AWS_BUCKET")
	if err := awsTestConnection(); err != nil {
		log.Fatal("S3 connection error: ", err)
		os.Exit(2)
	}

}

func getEnv(name string) string {
	val := os.Getenv(name)
	if len(val) == 0 {
		log.Fatalf("Environment variable %s not defined", name)
		os.Exit(2)
	}
	return val
}

func main() {

	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	switch args[0] {
	case "single":
		msgIfErr(doBackup())
	case "start":
		seconds := 86400
		if len(args) > 1 {
			if s, err := strconv.Atoi(args[1]); err == nil {
				seconds = s
			}
		}
		if err := msgIfErr(doBackup()); err != nil {
			log.Fatalf("Initial backup error, job will not repeat: %s", err)
			os.Exit(2)
		}
		for _ = range time.NewTicker(time.Duration(seconds) * time.Second).C {
			msgIfErr(doBackup())
		}
	case "restore":
		snapshot := ""
		if len(args) > 1 {
			snapshot = args[1]
		}
		msgIfErr(doRestore(snapshot))
	default:
		usage()
	}
}

func usage() {
	fmt.Fprint(os.Stderr, "usage: cassandra-backup [ single | start [seconds] | restore [snapshot] ]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func msgIfErr(err error) error {
	if err != nil {
		log.Println("Error: ", err)
	}
	return err
}

func doBackup() error {

	snapshot, err := cassandraMakeSnapshot()
	if err != nil {
		err = errors.New("Snapshot error: " + err.Error())
		return err
	}
	snapshotId := strconv.Itoa(snapshot)

	backupDir := pathSeparator + backupPrefix + snapshotId
	if _, err := exec.Command("mkdir", backupDir).Output(); err != nil {
		err = errors.New("Create backup dir error: " + err.Error())
		return err
	}

	u, g := "", ""
	for _, dir1 := range dataFileDirectories {
		dirs2, _ := ioutil.ReadDir(dir1)
		for _, dir2 := range dirs2 {
			if !dir2.IsDir() {
				continue
			}
			dirs3, _ := ioutil.ReadDir(dir1 + pathSeparator + dir2.Name())
			for _, dir3 := range dirs3 {
				if !dir3.IsDir() {
					continue
				}
				snapshotDir := dir1 + pathSeparator + dir2.Name() + pathSeparator + dir3.Name() +
					pathSeparator + "snapshots"
				if len(u) == 0 {
					if r, err := exec.Command("stat", "-c", "%U %G", snapshotDir).Output(); err != nil {
						err = errors.New("Unable to get owner and group: " + err.Error())
						return err
					} else {
						ug := strings.Split(strings.TrimSpace(string(r)), " ")
						u, g = ug[0], ug[1]
					}
				}
				info, err := os.Stat(snapshotDir + pathSeparator + snapshotId)
				if err != nil || !info.IsDir() {
					continue
				}
				if _, err := exec.Command("mkdir", "-p", backupDir+snapshotDir).Output(); err != nil {
					err = errors.New("Create snapshot dir error: " + err.Error())
					return err
				}
				if _, err := exec.Command("mv", snapshotDir+pathSeparator+snapshotId, backupDir+snapshotDir).Output(); err != nil {
					err = errors.New("Move snapshot dir error: " + err.Error())
					return err
				}
			}
		}

	}
	if _, err := exec.Command("chown", "-R", u+":"+g, backupDir).Output(); err != nil {
		err = errors.New("Chown snapshot dir error: " + err.Error())
		return err
	}

	if _, err := exec.Command("tar", "-cvzf", backupDir+extTarGz, "-C", backupDir, ".").Output(); err != nil {
		err = errors.New("Compress backup error: " + err.Error())
		return err
	}

	if _, err := exec.Command("rm", "-R", backupDir).Output(); err != nil {
		err = errors.New("Remove backup dir error: " + err.Error())
		return err
	}

	if err = awsFileToBucket(backupDir+extTarGz, backupDir+extTarGz); err != nil {
		err = errors.New("Send snapshot to storage error: " + err.Error())
		return err
	}

	log.Println("Backup created: ", snapshot)

	return nil
}

func doRestore(snapshotId string) (err error) {

	var snapshot int
	if len(snapshotId) > 0 {
		if snapshot, err = strconv.Atoi(snapshotId); err != nil {
			err = errors.New("Snapshot should be int: " + err.Error())
			return
		}
	}
	snapshots, err := awsSnapshotsList()
	if err != nil {
		return
	}
	if len(snapshots) == 0 {
		err = errors.New("Empty backuped snapshots: " + err.Error())
		return
	}

	if len(snapshotId) > 0 {
		found := false
		for _, s := range snapshots {
			if s == snapshot {
				found = true
				break
			}
		}
		if !found {
			err = errors.New("Specified backuped snapshots is absent")
			return
		}
	} else {
		snapshot = 0
		for _, s := range snapshots {
			if s > snapshot {
				snapshot = s
			}
		}
		snapshotId = strconv.Itoa(snapshot)
	}

	var backupFile string
	if backupFile, err = awsFileFromBucket(pathSeparator + backupPrefix + strconv.Itoa(snapshot) + extTarGz); err != nil {
		err = errors.New("Get backuped snapshot error: " + err.Error())
		return
	}

	info, err := os.Stat(backupFile)
	if err != nil {
		err = errors.New("Read backuped snapshot error: " + err.Error())
		return
	} else if info.IsDir() {
		err = errors.New("Backuped snapshot is dir, not file")
		return
	}

	if _, err = exec.Command("tar", "-zxvf", backupFile, "-C", pathSeparator).Output(); err != nil {
		err = errors.New("Decompress backup error: " + err.Error())
		return
	}
	if _, err = exec.Command("rm", "-dr", backupFile).Output(); err != nil {
		err = errors.New("Delete snapshot file error: " + err.Error())
		return
	}

	for _, dir1 := range dataFileDirectories {
		dirs2, _ := ioutil.ReadDir(dir1)
		for _, dir2 := range dirs2 {
			if !dir2.IsDir() {
				continue
			}
			dirs3, _ := ioutil.ReadDir(dir1 + pathSeparator + dir2.Name())
			for _, dir3 := range dirs3 {
				if !dir3.IsDir() {
					continue
				}

				currentDir := dir1 + pathSeparator + dir2.Name() + pathSeparator + dir3.Name()

				// clear all data w/o snapshots
				entries, _ := ioutil.ReadDir(currentDir)
				for _, entry := range entries {
					if entry.IsDir() {
						continue
					}
					toDelete := dir1 + pathSeparator + dir2.Name() + pathSeparator + dir3.Name() +
						pathSeparator + entry.Name()
					if err = os.Remove(toDelete); err != nil {
						err = errors.New("File " + toDelete + " delete error: " + err.Error())
						return
					}
				}

				// restore snapshot
				snapshotDir := currentDir + pathSeparator + "snapshots" + pathSeparator + snapshotId
				info, err = os.Stat(snapshotDir)
				if err != nil {
					continue
				}
				if !info.IsDir() {
					continue
				}
				// exec.Command not works with stars:
				// http://stackoverflow.com/questions/31467153/golang-failed-exec-command-that-works-in-terminal
				//if _, err := exec.Command("mv", snapshotDir+pathSeparator+"*", currentDir).Output(); err != nil {
				if _, err = exec.Command("/bin/sh", "-c", "mv "+snapshotDir+pathSeparator+"* "+currentDir).Output(); err != nil {
					err = errors.New("Move data from snapshot dir error: " + err.Error())
					return
				}

				if _, err = exec.Command("rm", "-dr", snapshotDir).Output(); err != nil {
					err = errors.New("Delete snapshot dir error: " + err.Error())
					return
				}
			}
		}
	}

	log.Println("Backup restored: ", snapshot)

	return

}

func cassandraMakeSnapshot() (int, error) {
	snapshot := time.Now().Unix()
	snapshotId := strconv.FormatInt(snapshot, 10)
	if _, err := exec.Command("nodetool", "snapshot", "-t", snapshotId).Output(); err != nil {
		return 0, err
	}
	return strconv.Atoi(snapshotId)
}

func cassandraDataFileDirectories() ([]string, error) {
	cfgDir := os.Getenv("CASSANDRA_CONFIG")
	if len(cfgDir) == 0 {
		return nil, errors.New("CASSANDRA_CONFIG not set, does cassandra installed?")
	}
	data, err := ioutil.ReadFile(cfgDir + "/cassandra.yaml")
	if err != nil {
		return nil, errors.New("cassandra.yaml file read error: " + err.Error())
	}
	out := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &out); err != nil {
		return nil, errors.New("cassandra.yaml format error: " + err.Error())
	}
	dirs := out["data_file_directories"]
	switch dirs.(type) {
	case []interface{}:
		dirsStr := []string{}
		for _, d := range dirs.([]interface{}) {
			switch d.(type) {
			case string:
				dirsStr = append(dirsStr, d.(string))
			}
		}
		return dirsStr, nil
	}
	return []string{}, nil
}

func awsTestConnection() (err error) {
	_, err = exec.Command("aws", "s3", "ls", "s3://"+awsBucket).Output()
	return
}

func awsFileToBucket(src, dst string) (err error) {
	if !strings.HasPrefix(dst, "/") {
		err = errors.New("destination path should start with /")
		return
	}
	if _, err := exec.Command("aws", "s3", "cp", src, "s3://"+awsBucket+dst).Output(); err != nil {
		return err
	}
	return
}

func awsFileFromBucket(filename string) (string, error) {
	if !strings.HasPrefix(filename, "/") {
		return "", errors.New("destination path should start with /")
	}
	if _, err := exec.Command("aws", "s3", "cp", "s3://"+awsBucket+filename, filename).Output(); err != nil {
		return "", err
	}
	return filename, nil
}

func awsSnapshotsList() (snapshots []int, err error) {
	r, err := exec.Command("aws", "s3api", "list-objects", "--bucket", awsBucket, "--output", "json").Output()
	if err != nil {
		return
	}
	if len(r) == 0 {
		err = errors.New("No snapshots stored on S3")
		return
	}
	type contentsItem struct {
		Key string
	}
	var data map[string][]contentsItem
	if err = json.Unmarshal(r, &data); err != nil {
		return
	}
	var contents []contentsItem
	var ok bool
	if contents, ok = data["Contents"]; !ok {
		err = errors.New("Invalid data from S3")
		return
	}
	for _, c := range contents {
		if strings.HasPrefix(c.Key, backupPrefix) && strings.HasSuffix(c.Key, extTarGz) {
			snapshot, e := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(c.Key, backupPrefix), extTarGz))
			if e != nil {
				continue
			}
			snapshots = append(snapshots, snapshot)
		}
	}
	return
}
