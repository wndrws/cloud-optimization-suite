package main

import "testing"

func TestExtract7z(t *testing.T) {
	if err := Extract7z("~/projects/test/test.7z", "~/projects/test/target"); err != nil {
		t.Errorf("error extracting 7z files: %v", err)
	}
}
