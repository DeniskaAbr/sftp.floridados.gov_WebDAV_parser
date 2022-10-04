package main

import (
	"CorParser/internal/webdav"
	"CorParser/models"
	"bufio"
	"bytes"
	"crypto/tls"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/foundatn-io/go-pic"

	"github.com/omeid/pgerror"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

var LastProcessedFileName = ""
var Wdc *webdav.Client
var DB *sql.DB

const (
	ShellToUse = "bash"
	timeFormat = "01-01-0001"
)

// вынести параметры во флаги запуска
var connStr *string
var tableName *string
var WebDAVEndpoint *string
var WebDAVWorkingPath *string
var InfoFile *string
var partSize int
var inf = new(Info)
var mailTo *string

type Info struct {
	StringParseEr int
	Ok            int
	Er            int
}

func main() {
	var err error
	var FilesToParse []webdav.FileInfo

	ex, err := os.Executable()
	if err != nil {
		log.Fatalln(err)
	}
	exPath := filepath.Dir(ex)
	log.Println(exPath)
	fmt.Println(exPath)

	startTime := time.Now()
	startTimeForFilename := startTime.Format("2006-01-02")

	connStr = flag.String("connstring", "postgres://user:user@192.168.0.34:5433/dbname?sslmode=disable", "Connection string for PostgresSQL database. ")
	tableName = flag.String("tableName", "annual_microdata", "Table name.")
	WebDAVEndpoint = flag.String("webdavendpoint", "https://sftp.floridados.gov", "WebDAV endpoint URI.")
	WebDAVWorkingPath = flag.String("webdavworkingpath", "/Public/doc/cor", "Path to folder in WebDAV where new files are stored.")
	InfoFile = flag.String("infofilename", exPath+"/info.txt", "File name where last parsed file name is stored.")
	partSize = *flag.Int("partSize", 200, "Transaction part size.")
	mailTo = flag.String("mailTo", "", "Email address to send parse status")

	flag.Parse()

	mails := func(m string) []string {
		var mails []string
		if m != "" {
			mails = strings.Split(m, ",")
		}
		return mails
	}(*mailTo)

	f, err := os.OpenFile(exPath+"/"+startTimeForFilename+"_webdav.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	DB = PGDB()

	err = DB.Ping()

	if err != nil {
		log.Panicln(err.Error())
	}

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}

	LastProcessedFileName = GetLastProcessedFile()

	Wdc, err = webdav.NewClient(client, *WebDAVEndpoint)
	if err != nil {
		log.Println(err)
	}

	files, err := Wdc.Readdir(*WebDAVWorkingPath, false)
	if err != nil {
		log.Println(err)
		log.Println("Non connected to WebDAV")

		if len(mails) > 0 {
			sendmail(mails, "Error "+err.Error()+"Non connected to WebDAV")
		}
	} else {
		for _, file := range files {
			log.Println(file.Path)
			name := path.Base(file.Path)

			re, err := regexp.Compile(`\d{8}c.txt`)

			if err != nil {
				log.Println(err)
			}

			if !file.IsDir {
				matched := re.MatchString(name)
				if matched {
					filename := strings.ToLower(path.Base(file.Path))

					if filename > LastProcessedFileName {
						FilesToParse = append(FilesToParse, file)
					}
				}
			}
		}

		for _, file := range FilesToParse {
			ForAllFiles(file)
		}

		log.Printf("ok: %d  err: %d string parse errors: %d", inf.Ok, inf.Er, inf.StringParseEr)

		if len(mails) > 0 {
			sendmail(mails, fmt.Sprintf("ok: %d  err: %d string parse errors: %d", inf.Ok, inf.Er, inf.StringParseEr))
		}
	}

}

func PGDB() *sql.DB {
	db, err := sql.Open("postgres", *connStr)
	if err != nil {
		log.Fatalln("not connected to DB " + err.Error())
	}

	db.SetConnMaxLifetime(0)
	db.SetMaxOpenConns(100)
	// db.SetMaxIdleConns(100)

	return db
}

func ForAllFiles(file webdav.FileInfo) {
	var err error
	var datarecords []models.ANNUALMICRODATAREC

	fileData := ReadFileData(Wdc, file)

	reader := bytes.NewReader(fileData)

	// ansi to utf8
	decodingReader := transform.NewReader(reader, charmap.Windows1252.NewDecoder())

	scanner := bufio.NewScanner(decodingReader) // (decodingReader)

	scanner.Split(bufio.ScanLines)
	var txtlines []string

	var i int
	for scanner.Scan() {
		scanedText := scanner.Text()
		if len(scanedText) > 1440 || len(scanedText) < 1440 {
			log.Printf("E: "+"Anomaly data length %v in file: %s string index: %v. Ignored.", len(scanner.Text()), file.Path, i)
			log.Println("I: " + scanedText)
			inf.StringParseEr += 1
		} else {
			txtlines = append(txtlines, scanedText+``)
		}
		i++
	}

	for _, eachline := range txtlines {
		reader := bufio.NewReader(bytes.NewReader([]byte(eachline)))

		d := pic.NewDecoder(reader)
		c := &models.ANNUALMICRODATAREC{}
		if err := d.Decode(c); err != nil {
			log.Fatal(err)
		}

		c = RemoveWS(*c)

		datarecords = append(datarecords, *c)
	}

	start := 0
	end := partSize

	for i := 0; i < len(datarecords); i = i + partSize {

		if end > len(datarecords) {
			end = len(datarecords)
		}

		drslice := datarecords[start:end]
		err = AddToDB(drslice)
		if err != nil {
			// log.Println(err)
			if pgerror.CharacterNotInRepertoire(err) != nil {
				for i, annualmicrodatarec := range drslice {
					sl := []models.ANNUALMICRODATAREC{annualmicrodatarec}
					err = AddToDB(sl)
					if err != nil {
						if pgerror.CharacterNotInRepertoire(err) != nil {
							log.Printf("Error with insert to PostgreSQL string: %v from file: %s string index: %v part: from-%v to-%v", start+i+1, file.Path, i, start, end)
							log.Println(err.Error())
						} else {
							log.Println(err.Error())
						}
						inf.Er += 1
					} else {
						inf.Ok += 1
					}
				}
			} else {
				log.Println(err.Error())
			}
		} else {
			inf.Ok += end - start
		}

		start = end
		end = start + partSize
	}

	PutLastProcessedFile(path.Base(file.Path))
}

func AddToDB(records []models.ANNUALMICRODATAREC) error {
	// time.Sleep(time.Millisecond * 1)

	txn, err := DB.Begin()
	defer txn.Rollback()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(
		*tableName,
		"annualcornumber",
		"annualcorname",
		"annualcorstatus",
		"annualcorfilingtype",
		"annualcor2ndmailadd1n1",
		"annualcor2ndmailadd2n1",
		"annualcor2ndmailcityn1",
		"annualcor2ndmailstaten1",
		"annualcor2ndmailzipn1",
		"annualcor2ndmailcountryn1",
		"annualcor2ndmailadd1n2",
		"annualcor2ndmailadd2n2",
		"annualcor2ndmailcityn2",
		"annualcor2ndmailstaten2",
		"annualcor2ndmailzipn2",
		"annualcor2ndmailcountryn2",
		"annualcorfiledate",
		"annualcorfeinumber",
		"annualmorethansixoffflag",
		"annuallasttrxdate",
		"annualstatecountry",
		"annualreportyear1",
		"annualhouseflag1",
		"annualreportdate1",
		"annualreportyear2",
		"annualhouseflag2",
		"annualreportdate2",
		"annualreportyear3",
		"annualhouseflag3",
		"annualreportdate3",
		"annualraname",
		"annualranametype",
		"annualraadd1",
		"annualracity",
		"annualrastate",
		"annualrazip5",
		"annualrazip4",
		"annualprinctitlen1",
		"annualprincnametypen1",
		"annualprincnamen1",
		"annualprincadd1n1",
		"annualprinccityn1",
		"annualprincstaten1",
		"annualprinczip5n1",
		"annualprinczip4n1",
		"annualprinctitlen2",
		"annualprincnametypen2",
		"annualprincnamen2",
		"annualprincadd1n2",
		"annualprinccityn2",
		"annualprincstaten2",
		"annualprinczip5n2",
		"annualprinczip4n2",
		"annualprinctitlen3",
		"annualprincnametypen3",
		"annualprincnamen3",
		"annualprincadd1n3",
		"annualprinccityn3",
		"annualprincstaten3",
		"annualprinczip5n3",
		"annualprinczip4n3",
		"annualprinctitlen4",
		"annualprincnametypen4",
		"annualprincnamen4",
		"annualprincadd1n4",
		"annualprinccityn4",
		"annualprincstaten4",
		"annualprinczip5n4",
		"annualprinczip4n4",
		"annualprinctitlen5",
		"annualprincnametypen5",
		"annualprincnamen5",
		"annualprincadd1n5",
		"annualprinccityn5",
		"annualprincstaten5",
		"annualprinczip5n5",
		"annualprinczip4n5",
		"annualprinctitlen6",
		"annualprincnametypen6",
		"annualprincnamen6",
		"annualprincadd1n6",
		"annualprinccityn6",
		"annualprincstaten6",
		"annualprinczip5n6",
		"annualprinczip4n6"))

	if err != nil {
		return err
	}

	for _, record := range records {

		log.Printf("Take to Inserting: %s", record.ANNUALCORNUMBER)

		corFileDateTime, _ := TimeParse(record.ANNUALCORFILEDATE)
		corFileDate := pq.NullTime{
			Time:  corFileDateTime,
			Valid: true,
		}
		if corFileDate.Time.Format("01-02-2006") == timeFormat {
			corFileDate.Valid = false
		}

		lastTrxDateTime, _ := TimeParse(record.ANNUALLASTTRXDATE)
		lastTrxDate := pq.NullTime{
			Time:  lastTrxDateTime,
			Valid: true,
		}
		if lastTrxDate.Time.Format("01-02-2006") == timeFormat {
			lastTrxDate.Valid = false
		}

		reportDate1Time, _ := TimeParse(record.ANNUALLASTTRXDATE)
		reportDate1 := pq.NullTime{
			Time:  reportDate1Time,
			Valid: true,
		}
		if reportDate1.Time.Format("01-02-2006") == timeFormat {
			reportDate1.Valid = false
		}

		reportDate2Time, _ := TimeParse(record.ANNUALLASTTRXDATE)
		reportDate2 := pq.NullTime{
			Time:  reportDate2Time,
			Valid: true,
		}
		if reportDate2.Time.Format("01-02-2006") == timeFormat {
			reportDate2.Valid = false
		}

		reportDate3Time, _ := TimeParse(record.ANNUALLASTTRXDATE)
		reportDate3 := pq.NullTime{
			Time:  reportDate3Time,
			Valid: true,
		}
		if reportDate3.Time.Format("01-02-2006") == timeFormat {
			reportDate3.Valid = false
		}

		_, err = stmt.Exec(
			record.ANNUALCORNUMBER,
			record.ANNUALCORNAME,
			record.ANNUALCORSTATUS,
			record.ANNUALCORFILINGTYPE,
			record.ANNUALCOR2NDMAILADD1n1,
			record.ANNUALCOR2NDMAILADD2n1,
			record.ANNUALCOR2NDMAILCITYn1,
			record.ANNUALCOR2NDMAILSTATEn1,
			record.ANNUALCOR2NDMAILZIPn1,
			record.ANNUALCOR2NDMAILCOUNTRYn1,
			record.ANNUALCOR2NDMAILADD1n2,
			record.ANNUALCOR2NDMAILADD2n2,
			record.ANNUALCOR2NDMAILCITYn2,
			record.ANNUALCOR2NDMAILSTATEn2,
			record.ANNUALCOR2NDMAILZIPn2,
			record.ANNUALCOR2NDMAILCOUNTRYn2,
			corFileDate,
			record.ANNUALCORFEINUMBER,
			record.ANNUALMORETHANSIXOFFFLAG,
			lastTrxDate,
			record.ANNUALSTATECOUNTRY,
			record.ANNUALREPORTYEAR1,
			record.ANNUALHOUSEFLAG1,
			reportDate1,
			record.ANNUALREPORTYEAR2,
			record.ANNUALHOUSEFLAG2,
			reportDate2,
			record.ANNUALREPORTYEAR3,
			record.ANNUALHOUSEFLAG3,
			reportDate3,
			record.ANNUALRANAME,
			record.ANNUALRANAMETYPE,
			record.ANNUALRAADD1,
			record.ANNUALRACITY,
			record.ANNUALRASTATE,
			record.ANNUALRAZIP5,
			record.ANNUALRAZIP4,
			record.ANNUALPRINCTITLEn1,
			record.ANNUALPRINCNAMETYPEn1,
			record.ANNUALPRINCNAMEn1,
			record.ANNUALPRINCADD1n1,
			record.ANNUALPRINCCITYn1,
			record.ANNUALPRINCSTATEn1,
			record.ANNUALPRINCZIP5n1,
			record.ANNUALPRINCZIP4n1,
			record.ANNUALPRINCTITLEn2,
			record.ANNUALPRINCNAMETYPEn2,
			record.ANNUALPRINCNAMEn2,
			record.ANNUALPRINCADD1n2,
			record.ANNUALPRINCCITYn2,
			record.ANNUALPRINCSTATEn2,
			record.ANNUALPRINCZIP5n2,
			record.ANNUALPRINCZIP4n2,
			record.ANNUALPRINCTITLEn3,
			record.ANNUALPRINCNAMETYPEn3,
			record.ANNUALPRINCNAMEn3,
			record.ANNUALPRINCADD1n3,
			record.ANNUALPRINCCITYn3,
			record.ANNUALPRINCSTATEn3,
			record.ANNUALPRINCZIP5n3,
			record.ANNUALPRINCZIP4n3,
			record.ANNUALPRINCTITLEn4,
			record.ANNUALPRINCNAMETYPEn4,
			record.ANNUALPRINCNAMEn4,
			record.ANNUALPRINCADD1n4,
			record.ANNUALPRINCCITYn4,
			record.ANNUALPRINCSTATEn4,
			record.ANNUALPRINCZIP5n4,
			record.ANNUALPRINCZIP4n4,
			record.ANNUALPRINCTITLEn5,
			record.ANNUALPRINCNAMETYPEn5,
			record.ANNUALPRINCNAMEn5,
			record.ANNUALPRINCADD1n5,
			record.ANNUALPRINCCITYn5,
			record.ANNUALPRINCSTATEn5,
			record.ANNUALPRINCZIP5n5,
			record.ANNUALPRINCZIP4n5,
			record.ANNUALPRINCTITLEn6,
			record.ANNUALPRINCNAMETYPEn6,
			record.ANNUALPRINCNAMEn6,
			record.ANNUALPRINCADD1n6,
			record.ANNUALPRINCCITYn6,
			record.ANNUALPRINCSTATEn6,
			record.ANNUALPRINCZIP5n6,
			record.ANNUALPRINCZIP4n6,
		)
		if err != nil {
			log.Printf("couldn't prepare COPY statement: %s \n", err.Error())
			return err
		}
	}
	log.Println("********")

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

func RemoveWS(md models.ANNUALMICRODATAREC) *models.ANNUALMICRODATAREC {
	mdtype := reflect.TypeOf(md)
	numFields := mdtype.NumField()
	rg := reflect.ValueOf(&md)

	for i := 0; i < numFields; i++ {
		v := rg.Elem().Field(i)

		y := v.Interface().(string) // y will have type float64.

		rg.Elem().Field(i).SetString(models.RemoveDuplicateWhitespaces(y))
	}
	return &md
}

func GetLastProcessedFile() string {
	file, err := os.Open(*InfoFile)

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
		}
	}(file)

	if err != nil {
		log.Printf("Failed opening file: %s \n", err.Error())
		log.Println("Last file set to empty. Parse all files in folder")
		return ""
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var txtlines []string

	for scanner.Scan() {
		txtlines = append(txtlines, scanner.Text())
	}

	return txtlines[0]
}

func PutLastProcessedFile(name string) {
	file, err := os.OpenFile(*InfoFile, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}

	datawriter := bufio.NewWriter(file)
	datawriter.WriteString(name)
	datawriter.Flush()
	err = file.Close()
	if err != nil {
		log.Fatalf("%s", err)
	}
}

func ReadFileData(client *webdav.Client, file webdav.FileInfo) []byte {
	re, err := client.Open(file.Path)
	if err != nil {
		log.Fatal(file.Path + " " + err.Error())
	}

	body, err := ioutil.ReadAll(re)

	if err != nil {
		log.Fatal(file.Path + " " + err.Error())
	}

	return body
}

func TimeParse(s string) (time.Time, error) {
	// The date we're trying to parse, work with and format
	var myDate time.Time
	myDateString := s

	if len(myDateString) == 8 {
		var err error
		se := myDateString[0:2] + "-" + myDateString[2:4] + "-" + myDateString[4:8]
		myDate, err = time.Parse("01-02-2006", se)
		if err != nil {
			return myDate, nil
		}
	}
	return myDate, nil
}

func sendmail(m []string, message string) {
	for _, mail := range m {
		command := fmt.Sprintf("echo \"Server message: %s\" | mail -s \"Florida corporations parser info:\" %s", message, mail)

		out, errout, err := Shellout(command)
		if err != nil {
			log.Printf("error: %v\n", err)
		}
		fmt.Println("--- stdout ---")
		fmt.Println(out)
		fmt.Println("--- stderr ---")
		fmt.Println(errout)
	}
}

func Shellout(command string) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(ShellToUse, "-c", command)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// func (i *arrayFlags) String() string {
// 	return "my string representation"
// }

// func (i *arrayFlags) Set(value string) error {
// 	*i = append(*i, value)
// 	return nil
// }

// var myFlags arrayFlags

// func main() {
// 	flag.Var(&myFlags, "list1", "Some description for this param.")
// 	flag.Parse()
// }
