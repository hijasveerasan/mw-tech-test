package emaildomainstats

import (
	"context"
	"reflect"
	"testing"
)

func TestDomainCount(t *testing.T) {
	fileName := "testdata/test_customer_data.csv"
	
	want := []DomainCount{
		{
			Domain:        "about.com",
			NumberOfUsers: 1,
		},
		{
			Domain:        "bigcartel.com",
			NumberOfUsers: 1,
		},
		{
			Domain:        "bizjournals.com",
			NumberOfUsers: 1,
		},
		{
			Domain:        "fastcompany.com",
			NumberOfUsers: 1,
		},
		{
			Domain:        "goo.gl",
			NumberOfUsers: 1,
		},
		{
			Domain:        "google.com",
			NumberOfUsers: 2,
		},
		{
			Domain:        "i2i.jp",
			NumberOfUsers: 1,
		},
		{
			Domain:        "nasa.gov",
			NumberOfUsers: 1,
		},
		{
			Domain:        "topsy.com",
			NumberOfUsers: 1,
		},
	}

	parser := NewParser(ParserConfig{
		WorkerCount: 10,
		BufferSize:  0,
	})
	
	got, err := parser.DomainCounts(context.Background(), fileName)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func BenchmarkDomainCounts(b *testing.B) {
	fileName := "testdata/customer_data.csv" 
	ctx := context.Background()

	parser := NewParser(ParserConfig{
		WorkerCount: 10,
		BufferSize:  10,
	})

	for n := 0; n < b.N; n++ {
		parser.DomainCounts(ctx, fileName)
	}
}

func BenchmarkDomainCountsWithLargeDataset(b *testing.B) {
	fileName := "testdata/test_large_customer_data.csv" // this file has roughly 1million rows
	ctx := context.Background()

	parser := NewParser(ParserConfig{
		WorkerCount: 200,
		BufferSize:  200,
	})
	
	for n := 0; n < b.N; n++ {
		parser.DomainCounts(ctx, fileName)
	}
}
