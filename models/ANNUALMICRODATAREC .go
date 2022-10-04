package models

import (
	"reflect"
	"regexp"
)

type ANNUALMICRODATAREC struct {
	ANNUALCORNUMBER     string `pic:"1,12"`
	ANNUALCORNAME       string `pic:"13,204"`
	ANNUALCORSTATUS     string `pic:"205,205"`
	ANNUALCORFILINGTYPE string `pic:"206,220"`

	ANNUALCOR2NDMAILADD1n1    string `pic:"221,262"`
	ANNUALCOR2NDMAILADD2n1    string `pic:"263,304"`
	ANNUALCOR2NDMAILCITYn1    string `pic:"305,332"`
	ANNUALCOR2NDMAILSTATEn1   string `pic:"333,334"`
	ANNUALCOR2NDMAILZIPn1     string `pic:"335,344"`
	ANNUALCOR2NDMAILCOUNTRYn1 string `pic:"345,346"`

	ANNUALCOR2NDMAILADD1n2    string `pic:"347,388"`
	ANNUALCOR2NDMAILADD2n2    string `pic:"389,430"`
	ANNUALCOR2NDMAILCITYn2    string `pic:"431,458"`
	ANNUALCOR2NDMAILSTATEn2   string `pic:"459,460"`
	ANNUALCOR2NDMAILZIPn2     string `pic:"461,470"`
	ANNUALCOR2NDMAILCOUNTRYn2 string `pic:"471,472"`

	ANNUALCORFILEDATE        string `pic:"473,480"`
	ANNUALCORFEINUMBER       string `pic:"481,494"`
	ANNUALMORETHANSIXOFFFLAG string `pic:"495,495"`
	ANNUALLASTTRXDATE        string `pic:"496,503"`
	ANNUALSTATECOUNTRY       string `pic:"504,505"`
	ANNUALREPORTYEAR1        string `pic:"506,509"`
	ANNUALHOUSEFLAG1         string `pic:"510,510"`
	ANNUALREPORTDATE1        string `pic:"511,518"`
	ANNUALREPORTYEAR2        string `pic:"519,522"`
	ANNUALHOUSEFLAG2         string `pic:"523,523"`
	ANNUALREPORTDATE2        string `pic:"524,531"`
	ANNUALREPORTYEAR3        string `pic:"532,535"`
	ANNUALHOUSEFLAG3         string `pic:"536,536"`
	ANNUALREPORTDATE3        string `pic:"537,544"`
	ANNUALRANAME             string `pic:"545,586"`
	ANNUALRANAMETYPE         string `pic:"587,587"`
	ANNUALRAADD1             string `pic:"588,629"`
	ANNUALRACITY             string `pic:"630,657"`
	ANNUALRASTATE            string `pic:"658,659"`
	ANNUALRAZIP5             string `pic:"660,664"`
	ANNUALRAZIP4             string `pic:"665,668"`

	ANNUALPRINCTITLEn1    string `pic:"669,672"`
	ANNUALPRINCNAMETYPEn1 string `pic:"673,673"`
	ANNUALPRINCNAMEn1     string `pic:"674,715"`
	ANNUALPRINCADD1n1     string `pic:"716,757"`
	ANNUALPRINCCITYn1     string `pic:"758,785"`
	ANNUALPRINCSTATEn1    string `pic:"786,787"`
	ANNUALPRINCZIP5n1     string `pic:"788,792"`
	ANNUALPRINCZIP4n1     string `pic:"793,796"`

	ANNUALPRINCTITLEn2    string `pic:"797,800"`
	ANNUALPRINCNAMETYPEn2 string `pic:"801,801"`
	ANNUALPRINCNAMEn2     string `pic:"802,843"`
	ANNUALPRINCADD1n2     string `pic:"844,885"`
	ANNUALPRINCCITYn2     string `pic:"886,913"`
	ANNUALPRINCSTATEn2    string `pic:"914,915"`
	ANNUALPRINCZIP5n2     string `pic:"916,920"`
	ANNUALPRINCZIP4n2     string `pic:"921,924"`

	ANNUALPRINCTITLEn3    string `pic:"925,928"`
	ANNUALPRINCNAMETYPEn3 string `pic:"929,929"`
	ANNUALPRINCNAMEn3     string `pic:"930,971"`
	ANNUALPRINCADD1n3     string `pic:"972,1013"`
	ANNUALPRINCCITYn3     string `pic:"1014,1041"`
	ANNUALPRINCSTATEn3    string `pic:"1042,1043"`
	ANNUALPRINCZIP5n3     string `pic:"1044,1048"`
	ANNUALPRINCZIP4n3     string `pic:"1049,1052"`

	ANNUALPRINCTITLEn4    string `pic:"1053,1056"`
	ANNUALPRINCNAMETYPEn4 string `pic:"1057,1057"`
	ANNUALPRINCNAMEn4     string `pic:"1058,1099"`
	ANNUALPRINCADD1n4     string `pic:"1100,1141"`
	ANNUALPRINCCITYn4     string `pic:"1142,1169"`
	ANNUALPRINCSTATEn4    string `pic:"1170,1171"`
	ANNUALPRINCZIP5n4     string `pic:"1172,1176"`
	ANNUALPRINCZIP4n4     string `pic:"1177,1180"`

	ANNUALPRINCTITLEn5    string `pic:"1181,1184"`
	ANNUALPRINCNAMETYPEn5 string `pic:"1185,1185"`
	ANNUALPRINCNAMEn5     string `pic:"1186,1227"`
	ANNUALPRINCADD1n5     string `pic:"1228,1269"`
	ANNUALPRINCCITYn5     string `pic:"1270,1297"`
	ANNUALPRINCSTATEn5    string `pic:"1298,1299"`
	ANNUALPRINCZIP5n5     string `pic:"1300,1304"`
	ANNUALPRINCZIP4n5     string `pic:"1305,1308"`

	ANNUALPRINCTITLEn6    string `pic:"1309,1312"`
	ANNUALPRINCNAMETYPEn6 string `pic:"1313,1313"`
	ANNUALPRINCNAMEn6     string `pic:"1314,1355"`
	ANNUALPRINCADD1n6     string `pic:"1356,1397"`
	ANNUALPRINCCITYn6     string `pic:"1398,1425"`
	ANNUALPRINCSTATEn6    string `pic:"1426,1427"`
	ANNUALPRINCZIP5n6     string `pic:"1428,1432"`
	ANNUALPRINCZIP4n6     string `pic:"1433,1436"`

	FILLER string `pic:"1437,1440"`
}

func RemoveDuplicateWhitespaces(s string) string {
	pattern := regexp.MustCompile(`\s+`)
	res := pattern.ReplaceAllString(s, " ")
	return res
}

func (md ANNUALMICRODATAREC) RemoveWS() {
	mdtype := reflect.TypeOf(md)
	numFields := mdtype.NumField()
	rg := reflect.ValueOf(&md)

	for i := 0; i < numFields; i++ {
		v := rg.Elem().Field(i)

		y := v.Interface().(string) // y will have type float64.

		rg.Elem().Field(i).SetString(RemoveDuplicateWhitespaces(y))
		// ewer := rg.Elem().Field(i)
		// fmt.Println(ewer)
	}
}
