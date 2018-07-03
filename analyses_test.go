package main

import (
	"encoding/json"
	"testing"
)

func TestAnalyses(t *testing.T) {
	teststring := `{"id":"73376a8e-7ee9-11e8-be08-f64e9b87c109","app_id":"5db1d48c-b484-44de-a6d7-31b1039989cb","user_id":"6c04cd3e-854a-11e4-8fa0-1fbef07e6168","username":"ipcdev@iplantcollaborative.org","status":"Canceled","description":"","name":"jupyter-lab_analysis1","result_folder":"/iplant/home/ipcdev/analyses/jupyter-lab_analysis1-2018-07-03-17-49-35.4","start_date":1530614975539,"planned_end_date":null,"system_id":"de"}`

	j := &Job{}

	if err := json.Unmarshal([]byte(teststring), j); err != nil {
		t.Error(err)
	}
}
