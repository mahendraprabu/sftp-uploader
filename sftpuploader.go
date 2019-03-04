package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func main() {

	//Load SFTP connection details from config file
	initConfig := LoadConfig("config.json")
	startTime := time.Now()
	logFileName := initConfig.LogFile + "_" + startTime.Format("20060102") + ".txt"
	var f *os.File
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		//Log error and exit program
		log.Fatalf("ERROR: Could not create or Open log file:%s\r\n", logFileName)
	}
	log.SetOutput(f)
	defer f.Close()

	//Collect file and move to processing folder
	files, err := ioutil.ReadDir(initConfig.CollectionFolder)
	if err != nil {
		log.Fatalf("ERROR: Could not read collection folder %s\r\n", initConfig.CollectionFolder)
	}
	//var fileCount int
	var fileList []os.FileInfo

	if len(files) > 0 {
		for _, fileName := range files {
			if fileName.IsDir() {
				continue //skip directories
			}
			var validFile bool
			for _, filter := range initConfig.FileNameFilter {
				if strings.Contains(fileName.Name(), filter) {
					validFile = true
				}
			}
			if !validFile {
				continue
			}
			//move file to processing folder
			err := moveToDest(initConfig.CollectionFolder+fileName.Name(), initConfig.ProcessingFolder+fileName.Name())
			if err != nil {
				continue //skip the file if it cannot move to processing folder
			}
			fileList = append(fileList, fileName)
		}
	}

	//Quit if no files to collect
	if len(fileList) < 1 {
		log.Println("No files to collect")
		return
	}

	var retryCount int

ConnectToSFTP:

	sshConf := &ssh.ClientConfig{
		User: initConfig.User,
		Auth: []ssh.AuthMethod{ssh.Password(initConfig.Password)},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	sshCon, err := ssh.Dial("tcp", initConfig.Server, sshConf)
	if err != nil {
		log.Printf("SSH dial to %v failed\n", initConfig.Server)
		log.Println(err)
		retryCount++
		if retryCount > 3 {
			log.Printf("ERROR: Maximum connection retry attempt exceeded\n")
			return
		}
		goto ConnectToSFTP
	}
	defer sshCon.Close()

	sftpClient, err := sftp.NewClient(sshCon)
	if err != nil {
		//return "", errors.Wrapf(err, "SFTP client init %v:%v failed", Host, Port)
		log.Printf("SFTP client init %v failed", initConfig.Server)
		fmt.Println(err)
		retryCount++
		if retryCount > 3 {
			log.Printf("ERROR: Maximum connection retry attempt exceeded\n")
			return
		}
		goto ConnectToSFTP
	}
	defer sftpClient.Close()

	for _, localFile := range fileList {
		srcFile := filepath.Join(initConfig.ProcessingFolder, localFile.Name())
		dstFile := initConfig.RemoteSftpPath + localFile.Name()
		err1 := sftpUpload(sftpClient, dstFile, srcFile)
		if err1 != nil {
			log.Println("SFTP Upload returned error: ", err1)
			continue
			//To-DO: Handle upload error
		}
		//SFTP upload successful, move file from Processing to Done folder
		doneFile := filepath.Join(initConfig.DoneFolder, localFile.Name())
		err := moveToDest(srcFile, doneFile)
		if err != nil {
			log.Println("ERROR in moving file to Done folder")
		}
	}
}

func sftpUpload(sftpClient *sftp.Client, dstPath string, file string) error {
	t1 := time.Now()

	f, err := os.Open(file)
	if err != nil {
		log.Println("ERROR - sftpUpload - Source file could not be opened")
		log.Println(file)
		return errors.Wrapf(err, "Opening file %v failed", file)
	}
	defer f.Close()

	sf, err := sftpClient.Create(dstPath)
	if err != nil {
		log.Println("ERROR -  sftpUpload -Could not create SFTP remote file")
		log.Println(dstPath)
		return errors.Wrapf(err, "SFTP creating file failed")
	}

	_, err = io.Copy(sf, f)
	if err != nil {
		log.Println("ERROR -  sftpUpload - SFTP upload file failed")
		return errors.Wrapf(err, "SFTP upload file failed")
	}
	sf.Close()

	t2 := time.Now()
	msg := fmt.Sprintf("SFTP upload finished `%v` -> `%v` Duration: %v",
		file, dstPath, t2.Sub(t1))
	log.Println(msg)
	return nil
}

//Config holds data about configuration information
type Config struct {
	Title            string
	User             string
	Password         string
	Server           string
	CollectionFolder string
	FileNameFilter   []string
	ProcessingFolder string
	DoneFolder       string
	RemoteSftpPath   string
	LogFile          string
	ConnectRetry     int
	RetryInterval    int
}

//LoadConfig reads the config.json file and loads to Config
func LoadConfig(file string) Config {
	var cfg Config
	configFile, err := os.Open(file)
	if err != nil {
		log.Printf("ERROR: Could not open config file %v\r\n", file)
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&cfg)
	if err != nil {
		log.Printf("ERROR: JSON Parser Error: %v\r\n", file)
		log.Printf(err.Error())
	}
	//fmt.Println(cfg)
	return cfg
}

//copyToDest copies the src file to dst and delete local file
func copyToDest(src string, dst string) error {
	// Open the source file for reading
	s, err := os.Open(src)
	if err != nil {
		log.Printf("ERROR: copyToDest: Could not read file: %v\r\n", src)
		log.Printf("%v\r\n", err)
		return err
	}
	defer s.Close()

	// Open the destination file for writing
	d, err := os.Create(dst)
	if err != nil {
		log.Printf("ERROR: copyToDest: Could not create remote file: %v\r\n", dst)
		return err
	}

	// Copy the contents of the source file into the destination file
	if _, err := io.Copy(d, s); err != nil {
		log.Printf("ERROR: copyToDest: Error in copying file to %v\r\n", dst)
		d.Close()
		return err
	}

	// Return any errors that result from closing the destination file
	// Will return nil if no errors occurred
	if err := d.Close(); err != nil {
		log.Printf("ERROR: copyToDest: Error closing remote file: %v\r\n", dst)
	}
	if err := s.Close(); err != nil {
		log.Printf("ERROR: copyToDest: Error closing local file: %v\r\n", src)
	}

	log.Printf("copyToDest: File: %v copied to %v\r\n", src, dst)
	//return nil if success
	return nil
}

//moveToDest will use copyToDest to copy file and then delete the source file
func moveToDest(src string, dst string) error {
	err := copyToDest(src, dst)
	if err != nil {
		log.Printf("ERROR: moveToDest: ERROR in copying file: %v\r\n", src)
		return err
	}
	log.Printf("moveToDest: Deleting File: %v\r\n", src)
	err1 := os.Remove(src)
	if err1 != nil {
		log.Printf("ERROR: moveToDest: Error in Deleting File: %v\r\n", src)
		return err1
	}
	return nil
}
