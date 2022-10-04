package main

import (
	"fmt"
	"time"
)

func main() {

	// The date we're trying to parse, work with and format
	myDateString := "2018-01-20 04:35"
	fmt.Println("My Starting Date:\t", myDateString)

	// Parse the date string into Go's time object
	// The 1st param specifies the format, 2nd is our date string
	myDate, err := time.Parse("2006-01-02", myDateString)
	// myDate, err := time.Parse("2006-01-02 15:04", myDateString)
	if err != nil {
		panic(err)
	}

	// Format uses the same formatting style as parse, or we can use a pre-made constant
	fmt.Println("My Date Reformatted:\t", myDate.Format(time.RFC822))

	// In Y-m-d
	fmt.Println("Just The Date:\t\t", myDate.Format("2006-01-02"))

}
