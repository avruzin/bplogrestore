package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"
)

const ELKTimeFormat = "Jan 02, 2006 @ 15:04:05.000" // Формат даты-времени в ELK
const BPTimeFormat = "2006/01/02 15:04:05.000"      // Формат даты-времени в логах Bright Pattern (без "микросекунд")
const tzCorrection = 3                              // Поправка времени для часового пояса Москвы (UTC+3)

type csvInputType struct {
	DebugLevel string
	Timestamp  string
	Message    string
}

var csvInputData []csvInputType

func main() {

	// Читаем параметры командной строки -- имя входного и выходного файлов
	flag.Parse()
	csvFileName := flag.Arg(0)
	logFileName := flag.Arg(1)

	if csvFileName == "" || logFileName == "" {
		fmt.Println("Использовать так: ./bplogrestore source.csv target.log")
		os.Exit(1)
	}

	//Читаем csv-файл и заполняем csvInputData
	csvInputData = readCsvInput(csvFileName, csvInputData)

	//Пишем log-файл в формате Bright Pattern
	writeOutputLog(logFileName, csvInputData)
}

func readCsvInput(csvFileName string, csvInputData []csvInputType) []csvInputType {
	// Открываем исходный файл
	csvFile, err := os.Open(csvFileName)
	if err != nil {
		panic(err)
	}

	defer csvFile.Close()

	// Читаем информацию
	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		fmt.Println("Ошибка в чтении сsv")
		panic(err)
	}

	for index, line := range csvLines {
		//Не импортировать первую строку csv
		if index == 0 {
			continue
		}

		// Парсим формат времени ELK
		myTime, err := time.Parse(ELKTimeFormat, line[0])
		if err != nil {
			panic(err)
		}

		// Делаем корректировку времени --  поправку на временную зону
		myTime = myTime.Add(time.Hour * tzCorrection)

		// Преобразуем в формат времени BP и добавляем "микросекунды"
		myTimeStr := myTime.Format(BPTimeFormat) + "." + line[13]

		// Добавляем пробелы к уровням логгирования длинной <5 символов (ERROR и DEBUG оставляем как есть)
		myDebugLevel := line[6]
		if myDebugLevel == "INFO" || myDebugLevel == "WARN" {
			myDebugLevel = myDebugLevel + " "
		}
		if myDebugLevel == "LOG" {
			myDebugLevel = myDebugLevel + "  "
		}

		csvInputLine := csvInputType{
			DebugLevel: myDebugLevel,
			Timestamp:  myTimeStr,
			Message:    line[12],
		}
		csvInputData = append(csvInputData, csvInputLine)
	}

	// Сортируем по временной метке
	sort.SliceStable(csvInputData,
		func(i, j int) bool { return csvInputData[i].Timestamp < csvInputData[j].Timestamp })

	return csvInputData
}

func writeOutputLog(logFileName string, csvInputData []csvInputType) {
	f, err := os.Create(logFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for _, csvInputLine := range csvInputData {
		logString := csvInputLine.DebugLevel + " " + csvInputLine.Timestamp + " " + csvInputLine.Message + "\n"
		_, err = f.WriteString(logString)
	}

	f.Sync()

}
