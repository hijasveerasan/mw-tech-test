/*
This package is required to provide functionality to process a csv file and return a sorted (by email domain) data
structure of your choice containing the email domains along with the number of customers for each domain. The customer_data.csv
file provides an example csv file to work with. Any errors should be logged (or handled) or returned to the consumer of
this package. Performance matters, the sample file may only contain 1K lines but the package may be expected to be used on
files with 10 million lines or run on a small machine.

Write this package as you normally would for any production grade code that would be deployed to a live system.

Please stick to using the standard library.
*/

package emaildomainstats

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

// DomainCount represents a single email domain and how many users use it
type DomainCount struct {
	Domain        string
	NumberOfUsers int
}

// Parser provides functionality to process csv and extract stats from it 
type Parser struct {
	config ParserConfig
}

// ParserConfig provides adjustable config options for the parser
type ParserConfig struct {
	WorkerCount int
	BufferSize int
}

// NewParser returns a parser
func NewParser(config ParserConfig) *Parser {
	return &Parser{
		config: config,
	}
}


// DomainCounts returns a sorted slice of email domains and number of users that use them. See emaildomainstats_test for examples
func (p *Parser) DomainCounts(ctx context.Context, fileName string) ([]DomainCount, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	rows := make(chan []string, p.config.BufferSize)  // @todo buffering option
	results := make(chan string, p.config.BufferSize) // @todo buffering option

	// setup worker goroutines
	var wg sync.WaitGroup
	for i := 1; i <= p.config.WorkerCount; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			worker(ctx, workerId, rows, results)
		}(i)
	}

	// read and ignore/validate the header
	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	// read the csv
	go func() {
		for {
			row, err := reader.Read()
			if err == io.EOF { //end of line
				break
			}
			if err != nil {
				//maybe return the error on an error channel and break
				log.Fatal(err)
			}

			rows <- row
		}

		// finished reading the file
		close(rows)
	}()

	// wait for workers to finish and close the results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	domainUserCount := make(map[string]int)
	for result := range results {
		domainUserCount[result]++
	}

	// sort by domain name
	domains := make([]string, 0, len(domainUserCount))
	for domain := range domainUserCount {
		domains = append(domains, domain)
	}

	sort.Strings(domains)

	domainUsage := make([]DomainCount, 0, len(domainUserCount))
	for _, domain := range domains {
		domainUsage = append(domainUsage, DomainCount{
			Domain:        domain,
			NumberOfUsers: domainUserCount[domain],
		})
	}

	return domainUsage, nil
}

func worker(ctx context.Context, id int, rows <-chan []string, results chan<- string) {
	for {
		select {
		case row, ok := <-rows:
			// has channel closed
			if !ok {
				return
			}
			if len(row) > 3 {
				email := row[2]
				parts := strings.Split(email, "@")
				results <- parts[1] // extract domain
			} else {
				fmt.Printf("invalid row, skipping %v", row)
			}
		case <-ctx.Done():
			return
		}
	}
}
