package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	inputFile  = flag.String("i", "", "input file,dump from mysql")
	outputFile = flag.String("o", "", "output file,input in mysql")
)

//get the num of lines in the given file
func getLineNum(f string) int {
	inFi, err := os.Open(f)
	if err != nil {
		log.Fatalln(err)
	}
	defer inFi.Close()

	br := bufio.NewReader(inFi)
	i := 0
	for {
		_, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}
		i++
	}
	return i
}

func main() {
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		log.Fatalln("no input or out put file")
	}

	//the amount of insert rows in each table
	tables := map[string]int{}
	//create table sqls
	creationLines := []string{}
	//constraint sqls
	foreignKeyLines := []string{}
	fulltextKeyLines := []string{}
	sequenceLines := []string{}
	//current being processed table
	currentTable := ""
	//the table which inserts sql
	lastTable := ""

	inFi, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer inFi.Close()
	outFi, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalln(err)
	}
	defer outFi.Close()

	outFi.WriteString("-- Converted by db_converter\n")
	outFi.WriteString("START TRANSACTION;\n")
	//outFi.WriteString("SET standard_conforming_strings=off;\n")
	//outFi.WriteString("SET escape_string_warning=off;\n")
	//outFi.WriteString("SET CONSTRAINTS ALL DEFERRED;\n\n")

	br := bufio.NewReader(inFi)
	lineNum := 0
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}
		lineNum++

		// replace ' with ''
		linestr := strings.TrimSpace(string(line))
		linestr = strings.Replace(linestr, `\\`, "$$$$&&&$$$", -1)
		linestr = strings.Replace(linestr, `\'`, "''", -1)
		linestr = strings.Replace(linestr, "$$$$&&&$$$", `\\`, -1)

		//ignore comment lines
		if strings.HasPrefix(linestr, "--") ||
			strings.HasPrefix(linestr, "/*") ||
			strings.HasPrefix(linestr, "LOCK TABLES") ||
			strings.HasPrefix(linestr, "DROP TABLE") ||
			strings.HasPrefix(linestr, "UNLOCK TABLES") ||
			len(linestr) == 0 {
			continue
		}

		if len(currentTable) == 0 {
			if strings.HasPrefix(linestr, "CREATE TABLE") {
				currentTable = strings.Split(linestr, "\"")[1]
				tables[currentTable] = 0
				creationLines = []string{}
			} else if strings.HasPrefix(linestr, "INSERT INTO") {
				//Solve BLOB content
				re, _ := regexp.Compile(`0x[0-9A-F]+`)
				linestr = re.ReplaceAllStringFunc(linestr,
					func(i string) string {
						return "E'" + i + "'"
					})
				// solve null time content
				linestr = strings.Replace(linestr, "'0000-00-00 00:00:00'", "NULL", -1)
				outFi.WriteString(linestr + "\n")
				tables[lastTable]++
			} else {
				fmt.Println("\n ! Unknown line in main body: ", linestr)
			}
		} else {
			//is it a column
			if strings.HasPrefix(linestr, "\"") {
				linestr = strings.Trim(linestr, ",")
				linearr := strings.SplitN(linestr, "\"", 3)
				name := linearr[1]
				definition := linearr[2]
				//ignore comment
				definition = strings.Split(definition, "COMMENT")[0]
				definition = strings.TrimSpace(definition)
				definitionarr := strings.SplitN(definition, " ", 2)
				typed := definitionarr[0]
				extra := ""
				if len(definitionarr) > 1 {
					extra := definitionarr[1]
					extra = strings.Replace(extra, "unsigned", "", -1)
					re, _ := regexp.Compile(`CHARACTER SET [\w\d]+\s*`)
					re.ReplaceAllString(extra, "")
					re, _ = regexp.Compile(`COLLATE [\w\d]+\s*`)
					re.ReplaceAllString(extra, "")
				}

				//do type convertion
				setSeq := false
				if strings.HasPrefix(typed, "tinyint(") {
					typed = "int4"
					setSeq = true
				} else if strings.HasPrefix(typed, "int(") {
					typed = "integer"
					setSeq = true
				} else if strings.HasPrefix(typed, "bigint(") {
					typed = "bigint"
					setSeq = true
				} else if typed == "longtext" {
					typed = "text"
				} else if typed == "mediumtext" {
					typed = "text"
				} else if typed == "tinytext" {
					typed = "text"
				} else if strings.HasPrefix(typed, "varchar(") {
					sizearr := strings.Split(typed, "(")
					sizei, _ := strconv.Atoi(strings.Trim(sizearr[1], ")"))
					typed = fmt.Sprintf("varchar(%d)", sizei*2)
				} else if strings.HasPrefix(typed, "double(") {
					sizearr := strings.Split(typed, "(")
					sizes := strings.Trim(sizearr[1], ")")
					typed = fmt.Sprintf("numeric(%s)", sizes)
				} else if strings.HasPrefix(typed, "smallint(") {
					typed = "int2"
					setSeq = true
				} else if typed == "datetime" {
					typed = "timestamp with time zone"
				} else if typed == "double" {
					typed = "double precision"
				} else if strings.HasSuffix(typed, "blob") {
					typed = "bytea"
				}

				//id fied need sequences[if they are integers?]
				if name == "id" && setSeq == true {
					sequenceLines = append(sequenceLines, fmt.Sprintf("CREATE SEQUENCE %s_id_seq", currentTable))
					sequenceLines = append(sequenceLines, fmt.Sprintf("SELECT setval('%s_id_seq', max(id)) FROM %s", currentTable, currentTable))
					sequenceLines = append(sequenceLines, fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"id\" SET DEFAULT nextval('%s_id_seq')", currentTable, currentTable))
				}
				//Record it
				creationLines = append(creationLines, fmt.Sprintf(`"%s" %s %s`, name, typed, extra))
			} else if strings.HasPrefix(linestr, "PRIMARY KEY") {
				//is it a constraint or something
				creationLines = append(creationLines, strings.Trim(linestr, ","))
			} else if strings.HasPrefix(linestr, "CONSTRAINT") {
				csName := strings.Split(linestr, "CONSTRAINT")[1]
				csName = strings.TrimSpace(csName)
				csName = strings.Trim(csName, ",")
				foreignKeyLines = append(foreignKeyLines, fmt.Sprintf("ALTER TABLE \"%s\" ADD CONSTRAINT %s DEFERRABLE INITIALLY DEFERRED", currentTable, csName))

				fkName := strings.Split(linestr, "FOREIGN KEY")[1]
				fkName = strings.Split(fkName, "REFERENCES")[0]
				fkName = strings.TrimSpace(fkName)
				fkName = strings.Trim(fkName, ",")
				foreignKeyLines = append(foreignKeyLines, fmt.Sprintf("CREATE INDEX ON \"%s\" %s", currentTable, fkName))
			} else if strings.HasPrefix(linestr, "UNIQUE KEY") {
				uDes := strings.Split(linestr, "(")[1]
				uDes = strings.Split(uDes, ")")[0]
				uCs := fmt.Sprintf("UNIQUE (%s)", uDes)
				creationLines = append(creationLines, uCs)
			} else if strings.HasPrefix(linestr, "FULLTEXT KEY") {
				ftKeys := strings.Split(linestr, "(")[1]
				ftKeys = strings.Split(linestr, ")")[0]
				ftKeys = strings.Replace(ftKeys, "\"", "", -1)
				ftKeysArr := strings.Split(ftKeys, ",")
				ftKeys = strings.Join(ftKeysArr, " || ' ' || ")
				ftKeys = fmt.Sprintf("CREATE INDEX ON %s USING gin(to_tsvector('english', %s))", currentTable, ftKeys)
				fulltextKeyLines = append(fulltextKeyLines, ftKeys)
			} else if strings.HasPrefix(linestr, "KEY") {
				continue
			} else if linestr == ");" {
				outFi.WriteString(fmt.Sprintf("CREATE TABLE \"%s\" (\n", currentTable))
				for k, v := range creationLines {
					tSep := ","
					if k == len(creationLines)-1 {
						tSep = ""
					}
					outFi.WriteString(fmt.Sprintf("    %s%s\n", v, tSep))
				}
				outFi.WriteString(");\n\n")
				lastTable = currentTable
				currentTable = ""
			} else {
				fmt.Println("\n ! Unknown line inside table creation: ", linestr)
			}
		}
	}

	outFi.WriteString("\n-- Post-data save --\n")
	outFi.WriteString("COMMIT;\n")
	outFi.WriteString("START TRANSACTION;\n")

	//Write FK constraints out
	outFi.WriteString("\n-- Foreign keys --\n")
	for _, v := range foreignKeyLines {
		outFi.WriteString(v + ";\n")
	}

	//Write sequences out
	outFi.WriteString("\n-- Sequences --\n")
	for _, v := range sequenceLines {
		outFi.WriteString(v + ";\n")
	}

	//Write full-text indexkeyses out
	outFi.WriteString("\n-- Full Text keys --\n")
	for _, v := range fulltextKeyLines {
		outFi.WriteString(v + ";\n")
	}

	//Finish file
	outFi.WriteString("\n")
	outFi.WriteString("COMMIT;\n")

	// print the process result
	fmt.Printf("\n")
	tableNum := 0
	insertNum := 0
	maxNmaeLen := 0
	keys := make([]string, len(tables))
	for k, v := range tables {
		keys[tableNum] = k
		tableNum++
		insertNum += v
		if len(k) > maxNmaeLen {
			maxNmaeLen = len(k)
		}
	}
	sort.Strings(keys)
	formatStr := "|create table|%" + strconv.Itoa(maxNmaeLen) + "s|insert rows|%10d|\n"
	for _, k := range keys {
		fmt.Printf(formatStr, k, tables[k])
	}
	fmt.Printf("\n #all rows %d, table num %d, insert num %d\n", lineNum, tableNum, insertNum)
}
