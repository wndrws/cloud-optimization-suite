package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintResultsToFile_MustRespectUserDefinedObjectivesOrder_MustPutNaNsForAbsentObjectives(t *testing.T) {
	for i := 0; i < 100; i++ {
		// given
		tmpDir := t.TempDir()
		testFile := tmpDir + "/test" + strconv.Itoa(i)
		objectives := []string{"a", "c", "b"}
		results := map[string]string{"b": "123.7", "c": "456"}
		// when
		err := printResultsToFile(testFile, objectives, results, "NaN")
		if err != nil {
			t.Errorf("Error printing results to file: %v", err)
		}
		// then
		actual, err := readTestFile(testFile)
		if err != nil {
			t.Errorf("Error reading test file: %v", err)
		}
		assert.Equal(t, []string{"NaN a", "456 c", "123.7 b"}, actual)
	}
}

func TestPrintResultsToFile_MustPutSpecifiedValueForAbsentObjectivesIfProvided(t *testing.T) {
	// given
	tmpDir := t.TempDir()
	testFile := tmpDir + "/test"
	objectives := []string{"a", "c", "b"}
	results := map[string]string{"b": "123.7", "c": "NaN"}
	// when
	err := printResultsToFile(testFile, objectives, results, "-9999")
	if err != nil {
		t.Errorf("Error printing results to file: %v", err)
	}
	// then
	actual, err := readTestFile(testFile)
	if err != nil {
		t.Errorf("Error reading test file: %v", err)
	}
	assert.Equal(t, []string{"-9999 a", "NaN c", "123.7 b"}, actual)
}

func readTestFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var res []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		res = append(res, trim(line))
	}

	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return res, nil
}
