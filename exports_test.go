package goisilon

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func init() {
	testClient()
}

func assertNoError(t *testing.T, err error) {
	if !assert.NoError(t, err) {
		t.Error(err)
		t.FailNow()
	}
}

func assertNil(t *testing.T, i interface{}) {
	if !assert.Nil(t, i) {
		t.FailNow()
	}
}

func assertNotNil(t *testing.T, i interface{}) {
	if !assert.NotNil(t, i) {
		t.FailNow()
	}
}

func TestExportsList(t *testing.T) {
	volumeName1 := "test_get_exports1"
	volumeName2 := "test_get_exports2"
	volumeName3 := "test_get_exports3"

	// Identify all exports currently on the cluster
	exportMap := make(map[int]string)
	exports, err := client.GetExports()
	assertNoError(t, err)

	for _, export := range exports {
		exportMap[export.ID] = (*export.Paths)[0]
	}
	initialExportCount := len(exports)

	var (
		vol      Volume
		exportID int
	)

	// Add the test exports
	vol, err = client.CreateVolume(volumeName1)
	assertNoError(t, err)
	assertNotNil(t, vol)
	volumeName1 = vol.Name
	volumePath1 := client.Path(volumeName1)
	t.Logf("created volume: %s", volumeName1)

	vol, err = client.CreateVolume(volumeName2)
	assertNoError(t, err)
	assertNotNil(t, vol)
	volumeName2 = vol.Name
	volumePath2 := client.Path(volumeName2)
	t.Logf("created volume: %s", volumeName2)

	vol, err = client.CreateVolume(volumeName3)
	assertNoError(t, err)
	assertNotNil(t, vol)
	volumeName3 = vol.Name
	volumePath3 := client.Path(volumeName3)
	t.Logf("created volume: %s", volumeName3)

	exportID, err = client.Export(volumeName1)
	assertNoError(t, err)
	t.Logf("created export: %d", exportID)

	exportID, err = client.Export(volumeName2)
	assertNoError(t, err)
	t.Logf("created export: %d", exportID)

	exportID, err = client.Export(volumeName3)
	assertNoError(t, err)
	t.Logf("created export: %d", exportID)

	// make sure we clean up when we're done
	defer client.Unexport(volumeName1)
	defer client.Unexport(volumeName2)
	defer client.Unexport(volumeName3)
	defer client.DeleteVolume(volumeName1)
	defer client.DeleteVolume(volumeName2)
	defer client.DeleteVolume(volumeName3)

	// Get the updated export list
	exports, err = client.GetExports()
	assertNoError(t, err)

	// Verify that the new exports are there as well as all the old exports.
	if !assert.Equal(t, initialExportCount+3, len(exports)) {
		t.FailNow()
	}

	// Remove the original exports and add the new ones.  In the end, we should only have the
	// exports we just created and nothing more.
	for _, export := range exports {
		if _, found := exportMap[export.ID]; found == true {
			// this export was exported prior to the test start
			delete(exportMap, export.ID)
		} else {
			// this export is new
			exportMap[export.ID] = (*export.Paths)[0]
		}
	}

	if !assert.Len(t, exportMap, 3) {
		t.FailNow()
	}

	volumeBitmap := 0
	for _, path := range exportMap {
		if path == volumePath1 {
			volumeBitmap += 1
		} else if path == volumePath2 {
			volumeBitmap += 2
		} else if path == volumePath3 {
			volumeBitmap += 4
		}
	}

	assert.Equal(t, 7, volumeBitmap)
}

func TestExportCreate(t *testing.T) {
	volumeName := "test_create_export"
	volumePath := client.Path(volumeName)

	// setup the test
	_, err := client.CreateVolume(volumeName)
	assertNoError(t, err)

	// make sure we clean up when we're done
	defer client.Unexport(volumeName)
	defer client.DeleteVolume(volumeName)

	// verify the volume isn't already exported
	export, err := client.GetExportByName(volumeName)
	assertNoError(t, err)
	assertNil(t, export)

	// export the volume
	_, err = client.Export(volumeName)
	assertNoError(t, err)

	// verify the volume has been exported
	export, err = client.GetExportByName(volumeName)
	assertNoError(t, err)
	assertNotNil(t, export)

	found := false
	for _, path := range *export.Paths {
		if path == volumePath {
			found = true
			break
		}
	}
	assert.True(t, found)
}

func TestExportDelete(t *testing.T) {
	volumeName := "test_unexport_volume"

	// initialize the export
	_, err := client.CreateVolume(volumeName)
	assertNoError(t, err)

	_, err = client.Export(volumeName)
	assertNoError(t, err)

	// make sure we clean up when we're done
	defer client.DeleteVolume(volumeName)

	// verify the volume is exported
	export, err := client.GetExportByName(volumeName)
	assertNoError(t, err)
	assertNotNil(t, export)

	// Unexport the volume
	err = client.Unexport(volumeName)
	assertNoError(t, err)

	// verify the volume is no longer exported
	export, err = client.GetExportByName(volumeName)
	assertNoError(t, err)
	assertNil(t, export)
}

func TestExportRootMapping(t *testing.T) {

	var (
		err        error
		exportID   int
		rootMap    UserMapping
		volumeName = "test_export_root_mapping"
	)

	// initialize the export
	_, err = client.CreateVolume(volumeName)
	assertNoError(t, err)

	exportID, err = client.Export(volumeName)
	assertNoError(t, err)

	// make sure we clean up when we're done
	defer client.UnexportByID(exportID)
	defer client.DeleteVolume(volumeName)

	// verify the existing root mapping is mapped to nobody
	rootMap, err = client.GetRootMappingByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, rootMap)
	assertNotNil(t, rootMap.User)
	assertNotNil(t, rootMap.User.Name)
	assert.Equal(t, "nobody", *rootMap.User.Name)

	// update the root mapping to root
	err = client.EnableRootMappingByID(exportID, "root")
	assertNoError(t, err)

	// verify the root mapping is mapped to root
	rootMap, err = client.GetRootMappingByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, rootMap)
	assertNotNil(t, rootMap.User)
	assertNotNil(t, rootMap.User.Name)
	assert.Equal(t, "root", *rootMap.User.Name)

	// disable the root mapping
	err = client.DisableRootMappingByID(exportID)
	assertNoError(t, err)

	// verify the root mapping is disabled
	rootMap, err = client.GetRootMappingByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, rootMap.Enabled)
	assert.False(t, *rootMap.Enabled)
}

var (
	getClients     = func(e Export) []string { return *e.Clients }
	getRootClients = func(e Export) []string { return *e.RootClients }
)

func TestExportClientsGet(t *testing.T) {
	testExportClientsGet(
		t,
		"test_get_export_clients",
		client.GetExportClientsByID,
		client.SetExportClientsByID)
}

func TestExportClientsSet(t *testing.T) {
	testExportClientsSet(
		t,
		"test_set_export_clients",
		getClients,
		client.SetExportClientsByID)
}

func TestExportClientsAdd(t *testing.T) {
	testExportClientsAdd(
		t,
		"test_add_export_clients",
		getClients,
		client.SetExportClientsByID,
		client.AddExportClientsByID)
}

func TestExportClientsClear(t *testing.T) {
	testExportClientsClear(
		t,
		"test_clear_export_clients",
		getClients,
		client.SetExportClientsByID,
		client.ClearExportClientsByID)
}

func TestExportRootClientsGet(t *testing.T) {
	testExportClientsGet(
		t,
		"test_get_export_root_clients",
		client.GetExportRootClientsByID,
		client.SetExportRootClientsByID)
}

func TestExportRootClientsSet(t *testing.T) {
	testExportClientsSet(
		t,
		"test_set_export_root_clients",
		getRootClients,
		client.SetExportRootClientsByID)
}

func TestExportRootClientsAdd(t *testing.T) {
	testExportClientsAdd(
		t,
		"test_add_export_root_clients",
		getRootClients,
		client.SetExportRootClientsByID,
		client.AddExportRootClientsByID)
}

func TestExportRootClientsClear(t *testing.T) {
	testExportClientsClear(
		t,
		"test_clear_export_root_clients",
		getRootClients,
		client.SetExportRootClientsByID,
		client.ClearExportRootClientsByID)
}

func testExportClientsGet(
	t *testing.T,
	volumeName string,
	getClients func(id int) ([]string, error),
	setClients func(id int, clients ...string) error) {

	var (
		err            error
		exportID       int
		clientList     = []string{"1.2.3.4", "1.2.3.5"}
		currentClients []string
	)

	// initialize the export
	_, err = client.CreateVolume(volumeName)
	assertNoError(t, err)

	exportID, err = client.Export(volumeName)
	assertNoError(t, err)

	// make sure we clean up when we're done
	defer client.UnexportByID(exportID)
	defer client.DeleteVolume(volumeName)

	// set the export client
	err = setClients(exportID, clientList...)
	assertNoError(t, err)

	// test getting the client list
	currentClients, err = getClients(exportID)
	assertNoError(t, err)

	// verify we received the correct clients
	assert.Equal(t, len(clientList), len(currentClients))

	sort.Strings(currentClients)
	sort.Strings(clientList)

	for i := range currentClients {
		assert.Equal(t, currentClients[i], clientList[i])
	}
}

func testExportClientsSet(
	t *testing.T,
	volumeName string,
	getClients func(e Export) []string,
	setClients func(id int, clients ...string) error) {

	var (
		err            error
		export         Export
		exportID       int
		currentClients []string
		clientList     = []string{"1.2.3.4", "1.2.3.5"}
	)

	sort.Strings(clientList)

	// initialize the export
	_, err = client.CreateVolume(volumeName)
	assertNoError(t, err)

	exportID, err = client.Export(volumeName)
	assertNoError(t, err)

	// make sure we clean up when we're done
	defer client.UnexportByID(exportID)
	defer client.DeleteVolume(volumeName)

	// verify we aren't already exporting the volume to any of the clients
	export, err = client.GetExportByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, export)

	for _, currentClient := range getClients(export) {
		for _, newClient := range clientList {
			assert.NotEqual(t, currentClient, newClient)
		}
	}

	// test setting the export client
	err = setClients(exportID, clientList...)
	assertNoError(t, err)

	// verify the export client was set
	export, err = client.GetExportByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, export)

	currentClients = getClients(export)
	assert.Equal(t, len(clientList), len(currentClients))

	sort.Strings(currentClients)
	for i := range currentClients {
		assert.Equal(t, currentClients[i], clientList[i])
	}
}

func testExportClientsAdd(
	t *testing.T,
	volumeName string,
	getClients func(e Export) []string,
	setClients func(id int, clients ...string) error,
	addClients func(id int, clients ...string) error) {

	var (
		err            error
		export         Export
		exportID       int
		currentClients []string
		clientList     = []string{"1.2.3.4", "1.2.3.5"}
		addedClients   = []string{"1.2.3.6", "1.2.3.7"}
		allClients     = append(clientList, addedClients...)
	)

	sort.Strings(clientList)
	sort.Strings(allClients)

	// initialize the export
	_, err = client.CreateVolume(volumeName)
	assertNoError(t, err)

	exportID, err = client.Export(volumeName)
	assertNoError(t, err)

	// make sure we clean up when we're done
	defer client.UnexportByID(exportID)
	defer client.DeleteVolume(volumeName)

	// verify we aren't already exporting the volume to any of the clients
	export, err = client.GetExportByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, export)

	for _, currentClient := range getClients(export) {
		for _, newClient := range clientList {
			assert.NotEqual(t, currentClient, newClient)
		}
	}

	// test setting the export client
	err = setClients(exportID, clientList...)
	assertNoError(t, err)

	export, err = client.GetExportByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, export)

	currentClients = getClients(export)
	assert.Equal(t, len(clientList), len(currentClients))

	sort.Strings(currentClients)
	for i := range currentClients {
		assert.Equal(t, currentClients[i], clientList[i])
	}

	// verify that added clients are added to the list
	err = addClients(exportID, addedClients...)
	assertNoError(t, err)

	export, err = client.GetExportByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, export)

	currentClients = getClients(export)
	assert.Equal(t, len(allClients), len(currentClients))

	sort.Strings(currentClients)
	for i := range currentClients {
		assert.Equal(t, currentClients[i], allClients[i])
	}
}

func testExportClientsClear(
	t *testing.T,
	volumeName string,
	getClients func(e Export) []string,
	setClients func(id int, clients ...string) error,
	nilClients func(id int) error) {

	var (
		err            error
		export         Export
		exportID       int
		currentClients []string
		clientList     = []string{"1.2.3.4", "1.2.3.5"}
	)

	sort.Strings(clientList)

	// initialize the export
	_, err = client.CreateVolume(volumeName)
	assertNoError(t, err)

	exportID, err = client.Export(volumeName)
	assertNoError(t, err)

	// make sure we clean up when we're done
	defer client.UnexportByID(exportID)
	defer client.DeleteVolume(volumeName)

	// verify we are exporting the volume
	err = setClients(exportID, clientList...)
	assertNoError(t, err)

	export, err = client.GetExportByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, export)

	currentClients = getClients(export)
	assert.Equal(t, len(clientList), len(currentClients))

	sort.Strings(currentClients)
	for i := range currentClients {
		assert.Equal(t, currentClients[i], clientList[i])
	}

	// test clearing the export client
	err = nilClients(exportID)
	assertNoError(t, err)

	// verify the export client was cleared
	export, err = client.GetExportByID(exportID)
	assertNoError(t, err)
	assertNotNil(t, export)

	assert.Len(t, getClients(export), 0)
}
