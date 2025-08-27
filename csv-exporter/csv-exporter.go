package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	reg "github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
)

func main() {
	var (
		dynamoEndpoint = flag.String("dynamo-docapi-endpoint", "", "DynamoDB endpoint (Yandex Cloud Document API URL)")
		taskID         = flag.String("task-id", "", "Filter by TaskID (optional). If empty, export ALL task runs via Scan")
		statusesCSV    = flag.String("status", "", "Comma-separated statuses to include (Submitted,Finished,Failed,Cancelled)")
		output         = flag.String("output", "export.csv", "Output CSV path")
	)
	flag.Parse()

	if *dynamoEndpoint == "" {
		log.Fatal("--dynamo-endpoint is required (e.g., https://docapi.serverless.yandexcloud.net/...)")
	}

	r, err := reg.New(*dynamoEndpoint)
	if err != nil {
		log.Fatalf("registry init: %v", err)
	}

	var statuses []reg.TaskRunStatus
	for _, s := range split(*statusesCSV) {
		statuses = append(statuses, reg.TaskRunStatus(s))
	}

	runs, err := r.ListTaskRuns(*taskID, statuses)
	if err != nil {
		log.Fatalf("list task runs: %v", err)
	}

	// Collect headers
	metaCols := []string{"task_id", "run_uuid", "status", "creation_time"}
	paramKeys := map[string]struct{}{}
	objKeys := map[string]struct{}{}

	for _, tr := range runs {
		for k := range tr.Parameters {
			paramKeys[k] = struct{}{}
		}
		for k := range tr.Results {
			objKeys[k] = struct{}{}
		}
	}

	pCols := prefixedSorted(paramKeys, "param_")
	oCols := prefixedSorted(objKeys, "obj_")
	header := append(append(metaCols, pCols...), oCols...)

	if err := writeCSV(*output, header, runs, pCols, oCols); err != nil {
		log.Fatalf("write csv: %v", err)
	}
	fmt.Printf("Wrote %d rows to %s\n", len(runs), *output)
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func prefixedSorted(m map[string]struct{}, prefix string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, prefix+k)
	}
	sort.Strings(keys)
	return keys
}

func writeCSV(path string, header []string, runs []reg.TaskRun, pCols, oCols []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(header); err != nil {
		return err
	}

	// Stripped keys (without prefixes) to look up in the maps
	strip := func(cols []string, prefix string) []string {
		out := make([]string, len(cols))
		for i, c := range cols {
			out[i] = strings.TrimPrefix(c, prefix)
		}
		return out
	}
	pKeys := strip(pCols, "param_")
	oKeys := strip(oCols, "obj_")

	for _, tr := range runs {
		row := make([]string, 0, len(header))

		row = append(row, tr.TaskID)
		row = append(row, tr.UUID)
		row = append(row, string(tr.Status))
		row = append(row, timePtr(tr.CreationTime))

		for range pCols { // fill later
			row = append(row, "")
		}
		for range oCols { // fill later
			row = append(row, "")
		}

		// Fill parameters
		for i, key := range pKeys {
			row[4+i] = tr.Parameters[key]
		}
		// Fill objectives/results
		base := 4 + len(pCols)
		for i, key := range oKeys {
			row[base+i] = tr.Results[key]
		}

		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func timePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
