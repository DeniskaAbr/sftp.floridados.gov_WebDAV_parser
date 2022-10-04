package main

//1 читаем файлы рекурсивнов папке
//2 парсим файлы построчно
//3 записываем данные файла ы базу данных
//4 перемещаем файл в другую папку
//5 повторяем рекрсивно

import (
	"CorParser/models"
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/foundatn-io/go-pic"
	"github.com/lib/pq"
	"github.com/omeid/pgerror"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

var DB *sql.DB
var connStr *string
var tableName *string
var partSize int
var inf = new(Info)

type Info struct {
	StringParseEr int
	Ok            int
	Er            int
}

func main() {

	startTime := time.Now()
	startTimeForFilename := startTime.Format("2006-01-02")

	connStr = flag.String("connstring", "postgres://user:user@192.168.0.34:5433/dbname?sslmode=disable", "Connection string for PostgresSQL database. ")
	tableName = flag.String("tableName", "annual_microdata", "Table name. Default value is \"annual_microdata\"")
	partSize = *flag.Int("partSize", 200, "Transaction part size.")

	flag.Parse()

	var err error

	f, err := os.OpenFile(startTimeForFilename+"_parser.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	DB = PGDB()

	err = DB.Ping()

	if err != nil {
		log.Fatalln(err.Error())
	}

	files, err := run()

	if err != nil {
		log.Fatalln(err)
	}

	for _, file := range files {
		ForAllFiles(file)
	}

	fmt.Printf("ok: %d  err: %d", inf.Ok, inf.Er)

}

func run() ([]string, error) {
	searchDir := "./cor"

	fileList := make([]string, 0)
	e := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			fileList = append(fileList, path)
		}

		return err
	})

	if e != nil {
		log.Fatalln(e.Error())
	}

	return fileList, nil
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

func ForAllFiles(filepath string) {
	var datarecords []models.ANNUALMICRODATAREC

	// file, err := os.Open(filepath)

	file, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	/*
	   	re := regexp.MustCompile(`\r\n`)
	   	str := string(file)
	   	str = re.ReplaceAllString(str, "  ")
	      reader := bytes.NewReader([]byte(str))
	*/
	reader := bytes.NewReader(file)

	// ansi to utf8

	decodingReader := transform.NewReader(reader, charmap.Windows1252.NewDecoder())

	scanner := bufio.NewScanner(decodingReader)

	scanner.Split(bufio.ScanLines)
	var txtlines []string

	var i int
	for scanner.Scan() {
		scanedText := scanner.Text()
		if len(scanedText) > 1440 || len(scanedText) < 1440 {
			log.Printf("E: "+"Anomaly data length %v in file: %s string index: %v. Ignored.", len(scanner.Text()), filepath, i)
			log.Println("I: " + scanedText)
			inf.StringParseEr += 1
		} else {
			txtlines = append(txtlines, scanedText+``)
		}
		i++
	}

	// file.Close()

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
							log.Printf("Error with insert to PostgreSQL string: %v from file: %s string index: %v part: from-%v to-%v", start+i+1, filepath, i, start, end)
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

	MoveParsedFile(filepath)

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
		if corFileDate.Time.Format("01-02-2006") == "01-01-0001" {
			corFileDate.Valid = false
		}

		lastTrxDateTime, _ := TimeParse(record.ANNUALLASTTRXDATE)
		lastTrxDate := pq.NullTime{
			Time:  lastTrxDateTime,
			Valid: true,
		}
		if lastTrxDate.Time.Format("01-02-2006") == "01-01-0001" {
			lastTrxDate.Valid = false
		}

		reportDate1Time, _ := TimeParse(record.ANNUALLASTTRXDATE)
		reportDate1 := pq.NullTime{
			Time:  reportDate1Time,
			Valid: true,
		}
		if reportDate1.Time.Format("01-02-2006") == "01-01-0001" {
			reportDate1.Valid = false
		}

		reportDate2Time, _ := TimeParse(record.ANNUALLASTTRXDATE)
		reportDate2 := pq.NullTime{
			Time:  reportDate2Time,
			Valid: true,
		}
		if reportDate2.Time.Format("01-02-2006") == "01-01-0001" {
			reportDate2.Valid = false
		}

		reportDate3Time, _ := TimeParse(record.ANNUALLASTTRXDATE)
		reportDate3 := pq.NullTime{
			Time:  reportDate3Time,
			Valid: true,
		}
		if reportDate3.Time.Format("01-02-2006") == "01-01-0001" {
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
			log.Println("couldn't prepare COPY statement: %v", err)
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

func MoveParsedFile(path string) {
	newPath := "parsed" + path

	fpath := filepath.Dir(newPath)

	err := os.MkdirAll(fpath, 0750)
	if err != nil {

		log.Fatalln(err)
	}
	err = os.Rename(path, newPath)
	if err != nil {

		log.Fatalln(err)
	}
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
