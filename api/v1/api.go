package apiv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var (
	NamespacePath       = "namespace"
	VolumesPath         = "/ifs/volumes"
	ExportsPath         = "platform/1/protocols/nfs/exports"
	QuotaPath           = "platform/1/quota/quotas"
	SnapshotsPath       = "platform/1/snapshot/snapshots"
	VolumeSnapshotsPath = "/ifs/.snapshot"

	Enabled  = true
	Disabled = false

	contentTypeJSONHeader = map[string]string{
		"Content-Type": "application/json",
	}

	debug, _ = strconv.ParseBool(os.Getenv("GOISILON_DEBUG"))

	colonBytes = []byte{byte(':')}
)

type IsiVolume struct {
	Name         string `json:"name"`
	AttributeMap []struct {
		Name  string      `json:"name"`
		Value interface{} `json:"value"`
	} `json:"attrs"`
}

// Isi PAPI volume JSON structs
type VolumeName struct {
	Name string `json:"name"`
}

type getIsiVolumesResp struct {
	Children []*VolumeName `json:"children"`
}

// Isi PAPI Volume ACL JSON structs
type Ownership struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AclRequest struct {
	Authoritative string     `json:"authoritative"`
	Action        string     `json:"action"`
	Owner         *Ownership `json:"owner"`
	Group         *Ownership `json:"group,omitempty"`
}

// Isi PAPI volume attributes JSON struct
type getIsiVolumeAttributesResp struct {
	AttributeMap []struct {
		Name  string      `json:"name"`
		Value interface{} `json:"value"`
	} `json:"attrs"`
}

// Isi PAPI export path JSON struct
type ExportPathList struct {
	Paths  []string `json:"paths"`
	MapAll struct {
		User   string   `json:"user"`
		Groups []string `json:"groups,omitempty"`
	} `json:"map_all"`
}

// Isi PAPI export clients JSON struct
type ExportClientList struct {
	Clients []string `json:"clients"`
}

// Isi PAPI export Id JSON struct
type postIsiExportResp struct {
	Id int `json:"id"`
}

// Export is an Isilon Export.
type Export struct {
	ID          int          `json:"-"`
	Paths       *[]string    `json:"paths,omitempty"`
	Clients     *[]string    `json:"clients,omitempty"`
	RootClients *[]string    `json:"root_clients,omitempty"`
	MapAll      *UserMapping `json:"map_all,omitempty"`
	MapNonRoot  *UserMapping `json:"map_non_root,omitempty"`
	MapRoot     *UserMapping `json:"map_root,omitempty"`
}

type export struct {
	ID          *int         `json:"id,omitempty"`
	Paths       *[]string    `json:"paths,omitempty"`
	Clients     *[]string    `json:"clients,omitempty"`
	RootClients *[]string    `json:"root_clients,omitempty"`
	MapAll      *UserMapping `json:"map_all,omitempty"`
	MapNonRoot  *UserMapping `json:"map_non_root,omitempty"`
	MapRoot     *UserMapping `json:"map_root,omitempty"`
}

func isNilUserMapping(um *UserMapping) bool {
	return um == nil || (um.Enabled == nil && um.PrimaryGroup == nil &&
		um.SecondaryGroup == nil && um.User == nil)
}

// UnmarshalJSON unmarshals a Export from JSON.
func (e *Export) UnmarshalJSON(data []byte) error {

	if isEmptyJSON(&data) {
		return nil
	}

	var pe export
	if err := json.Unmarshal(data, &pe); err != nil {
		return nil
	}

	if pe.ID != nil {
		e.ID = *pe.ID
	}
	e.Paths = pe.Paths
	e.Clients = pe.Clients
	e.RootClients = pe.RootClients

	if !isNilUserMapping(pe.MapAll) {
		e.MapAll = pe.MapAll
	}
	if !isNilUserMapping(pe.MapNonRoot) {
		e.MapNonRoot = pe.MapNonRoot
	}
	if !isNilUserMapping(pe.MapRoot) {
		e.MapRoot = pe.MapRoot
	}

	return nil
}

// ExportList is a list of Isilon Exports.
type ExportList []*Export

// MarshalJSON marshals an ExportList to JSON.
func (l ExportList) MarshalJSON() ([]byte, error) {
	exports := struct {
		Exports []*Export `json:"exports,omitempty"`
	}{l}
	return json.Marshal(exports)
}

// UnmarshalJSON unmarshals an ExportList from JSON.
func (l *ExportList) UnmarshalJSON(text []byte) error {
	exports := struct {
		Exports []*Export `json:"exports,omitempty"`
	}{}
	if err := json.Unmarshal(text, &exports); err != nil {
		return err
	}
	*l = exports.Exports
	return nil
}

// Isi PAPI export attributes JSON structs
type IsiExport struct {
	Id      int      `json:"id"`
	Paths   []string `json:"paths"`
	Clients []string `json:"clients"`
}

type getIsiExportsResp struct {
	ExportList []*IsiExport `json:"exports"`
}

// Isi PAPI snapshot path JSON struct
type SnapshotPath struct {
	Path string `json:"path"`
	Name string `json:"name,omitempty"`
}

// Isi PAPI snapshot JSON struct
type IsiSnapshot struct {
	Created       int64   `json:"created"`
	Expires       int64   `json:"expires"`
	HasLocks      bool    `json:"has_locks"`
	Id            int64   `json:"id"`
	Name          string  `json:"name"`
	Path          string  `json:"path"`
	PctFilesystem float64 `json:"pct_filesystem"`
	PctReserve    float64 `json:"pct_reserve"`
	Schedule      string  `json:"schedule"`
	ShadowBytes   int64   `json:"shadow_bytes"`
	Size          int64   `json:"size"`
	State         string  `json:"state"`
	TargetId      int64   `json:"target_it"`
	TargetName    string  `json:"target_name"`
}

type getIsiSnapshotsResp struct {
	SnapshotList []*IsiSnapshot `json:"snapshots"`
	Total        int64          `json:"total"`
	Resume       string         `json:"resume"`
}

type isiThresholds struct {
	Advisory             int64       `json:"advisory"`
	AdvisoryExceeded     bool        `json:"advisory_exceeded"`
	AdvisoryLastExceeded interface{} `json:"advisory_last_exceeded"`
	Hard                 int64       `json:"hard"`
	HardExceeded         bool        `json:"hard_exceeded"`
	HardLastExceeded     interface{} `json:"hard_last_exceeded"`
	Soft                 int64       `json:"soft"`
	SoftExceeded         bool        `json:"soft_exceeded"`
	SoftLastExceeded     interface{} `json:"soft_last_exceeded"`
}

type IsiQuota struct {
	Container                 bool          `json:"container"`
	Enforced                  bool          `json:"enforced"`
	Id                        string        `json:"id"`
	IncludeSnapshots          bool          `json:"include_snapshots"`
	Linked                    interface{}   `json:"linked"`
	Notifications             string        `json:"notifications"`
	Path                      string        `json:"path"`
	Persona                   interface{}   `json:"persona"`
	Ready                     bool          `json:"ready"`
	Thresholds                isiThresholds `json:"thresholds"`
	ThresholdsIncludeOverhead bool          `json:"thresholds_include_overhead"`
	Type                      string        `json:"type"`
	Usage                     struct {
		Inodes   int64 `json:"inodes"`
		Logical  int64 `json:"logical"`
		Physical int64 `json:"physical"`
	} `json:"usage"`
}

type isiThresholdsReq struct {
	Advisory interface{} `json:"advisory"`
	Hard     interface{} `json:"hard"`
	Soft     interface{} `json:"soft"`
}

type IsiQuotaReq struct {
	Enforced                  bool             `json:"enforced"`
	IncludeSnapshots          bool             `json:"include_snapshots"`
	Path                      string           `json:"path"`
	Thresholds                isiThresholdsReq `json:"thresholds"`
	ThresholdsIncludeOverhead bool             `json:"thresholds_include_overhead"`
	Type                      string           `json:"type"`
}

type IsiUpdateQuotaReq struct {
	Enforced                  bool             `json:"enforced"`
	Thresholds                isiThresholdsReq `json:"thresholds"`
	ThresholdsIncludeOverhead bool             `json:"thresholds_include_overhead"`
}

type isiQuotaListResp struct {
	Quotas []IsiQuota `json:"quotas"`
}

// UserMapping maps to the ISI <user-mapping> type.
type UserMapping struct {
	Enabled        *bool      `json:"enabled,omitempty"`
	User           *Persona   `json:"user,omitempty"`
	PrimaryGroup   *Persona   `json:"primary_group,omitempty"`
	SecondaryGroup []*Persona `json:"secondary_group,omitempty"`
}

type userMapping struct {
	Enabled        *bool      `json:"enabled,omitempty"`
	User           *Persona   `json:"user,omitempty"`
	PrimaryGroup   *Persona   `json:"primary_group,omitempty"`
	SecondaryGroup []*Persona `json:"secondary_group,omitempty"`
}

func isNilPersona(p *Persona) bool {
	return p == nil || (p.ID == nil && p.Name == nil && p.Type == nil)
}

// UnmarshalJSON unmarshals a UserMapping from JSON.
func (um *UserMapping) UnmarshalJSON(data []byte) error {

	if isEmptyJSON(&data) {
		return nil
	}

	var pum userMapping
	if err := json.Unmarshal(data, &pum); err != nil {
		return nil
	}

	if pum.Enabled != nil {
		um.Enabled = pum.Enabled
	}
	if !isNilPersona(pum.User) {
		um.User = pum.User
	}
	if !isNilPersona(pum.PrimaryGroup) {
		um.PrimaryGroup = pum.PrimaryGroup
	}
	if len(pum.SecondaryGroup) > 0 {
		um.SecondaryGroup = pum.SecondaryGroup
	}

	return nil
}

func isEmptyJSON(data *[]byte) bool {
	d := *data
	return len(d) == 2 && d[0] == '{' && d[1] == '}'
}

// Persona maps to the ISI <persona> type.
type Persona struct {
	ID   *PersonaID   `json:"id,omitempty"`
	Type *PersonaType `json:"type,omitempty"`
	Name *string      `json:"name,omitempty"`
}

type personaWithID struct {
	ID *PersonaID `json:"id,omitempty"`
}

// MarshalJSON marshals a Persona to JSON.
func (p *Persona) MarshalJSON() ([]byte, error) {
	if p.ID != nil {
		return json.Marshal(personaWithID{p.ID})
	} else if p.Type != nil && p.Name != nil {
		return json.Marshal(fmt.Sprintf("%s:%s", *p.Type, *p.Name))
	} else if p.Name != nil {
		return json.Marshal(*p.Name)
	}
	return nil, fmt.Errorf("persona cannot be marshaled to json: %+v", p)
}

// UnmarshalJSON unmarshals a Persona from JSON.
func (p *Persona) UnmarshalJSON(data []byte) error {

	if isEmptyJSON(&data) {
		return nil
	}

	var pid personaWithID
	if err := json.Unmarshal(data, &pid); err == nil {
		if pid.ID != nil {
			p.ID = pid.ID
			return nil
		}
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parts := strings.SplitN(s, ":", 2)
	if len(parts) == 1 {
		p.Name = &parts[0]
		return nil
	}

	pt := ParsePersonaType(parts[0])
	p.Type = &pt
	p.Name = &parts[1]
	return nil
}

// PersonaID maps to the ISI <persona-id> type.
type PersonaID struct {
	ID   string
	Type PersonaIDType
}

// MarshalJSON marshals a PersonaID to JSON.
func (p *PersonaID) MarshalJSON() ([]byte, error) {
	if p.Type == PersonaIDTypeUnknown {
		return json.Marshal(p.ID)
	}
	return json.Marshal(fmt.Sprintf("%s:%s", p.Type, p.ID))
}

// UnmarshalJSON unmarshals a PersonaID from JSON.
func (p *PersonaID) UnmarshalJSON(data []byte) error {

	if isEmptyJSON(&data) {
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parts := strings.SplitN(s, ":", 2)
	if len(parts) == 1 {
		p.ID = parts[0]
		return nil
	}

	p.Type = ParsePersonaIDType(parts[0])
	p.ID = parts[1]
	return nil
}

// PersonaIDType is a valid Persona ID type.
type PersonaIDType uint8

const (
	// PersonaIDTypeUnknown is an unknown PersonaID type.
	PersonaIDTypeUnknown PersonaIDType = iota

	// PersonaIDTypeUser is a PersonaID user type.
	PersonaIDTypeUser

	// PersonaIDTypeGroup is a PersonaID group type.
	PersonaIDTypeGroup

	// PersonaIDTypeSID is a PersonaID SID type.
	PersonaIDTypeSID

	// PersonaIDTypeUID is a PersonaID UID type.
	PersonaIDTypeUID

	// PersonaIDTypeGID is a PersonaID GID type.
	PersonaIDTypeGID

	personaIDTypeCount
)

const (
	personaIDTypeUnknownStr = "unknown"
	personaIDTypeUserStr    = "user"
	personaIDTypeGroupStr   = "group"
	personaIDTypeSIDStr     = "SID"
	personaIDTypeUIDStr     = "UID"
	personaIDTypeGIDStr     = "GID"
)

var personaIDTypesToStrs = [personaIDTypeCount]string{
	personaIDTypeUnknownStr,
	personaIDTypeUserStr,
	personaIDTypeGroupStr,
	personaIDTypeSIDStr,
	personaIDTypeUIDStr,
	personaIDTypeGIDStr,
}

// ParsePersonaIDType parses a PersonaIDType from a string.
func ParsePersonaIDType(text string) PersonaIDType {
	switch {
	case strings.EqualFold(text, personaIDTypeUserStr):
		return PersonaIDTypeUser
	case strings.EqualFold(text, personaIDTypeGroupStr):
		return PersonaIDTypeGroup
	case strings.EqualFold(text, personaIDTypeSIDStr):
		return PersonaIDTypeSID
	case strings.EqualFold(text, personaIDTypeUIDStr):
		return PersonaIDTypeUID
	case strings.EqualFold(text, personaIDTypeGIDStr):
		return PersonaIDTypeGID
	}
	return PersonaIDTypeUnknown
}

// String returns the string representation of a PersonaIDType value.
func (p PersonaIDType) String() string {
	if p < (PersonaIDTypeUnknown+1) || p >= personaIDTypeCount {
		return personaIDTypesToStrs[PersonaIDTypeUnknown]
	}
	return personaIDTypesToStrs[p]
}

// MarshalJSON marshals a PersonaIDType value to JSON.
func (p PersonaIDType) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

// UnmarshalJSON unmarshals a PersonaIDType value from JSON.
func (p *PersonaIDType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*p = ParsePersonaIDType(s)
	return nil
}

// PersonaType is a valid Persona type.
type PersonaType uint8

const (
	// PersonaTypeUnknown is an unknown Persona type.
	PersonaTypeUnknown PersonaType = iota

	// PersonaIDTypeUser is a Persona user type.
	PersonaTypeUser

	// PersonaTypeGroup is a Persona group type.
	PersonaTypeGroup

	// PersonaTypeWellKnown is a Persona wellknown type.
	PersonaTypeWellKnown

	personaTypeCount
)

var (
	// PPersonaIDTypeUnknown is used to get adddress of the constant.
	PPersonaTypeUnknown = PersonaTypeUnknown

	// PPersonaTypeUser is used to get adddress of the constant.
	PPersonaTypeUser = PersonaTypeUser

	// PPersonaTypeGroup is used to get adddress of the constant.
	PPersonaTypeGroup = PersonaTypeGroup

	// PPersonaTypeWellKnown is used to get adddress of the constant.
	PPersonaTypeWellKnown = PersonaTypeWellKnown
)

const (
	personaTypeUnknownStr   = "unknown"
	personaTypeUserStr      = "user"
	personaTypeGroupStr     = "group"
	personaTypeWellKnownStr = "wellknown"
)

var personaTypesToStrs = [personaTypeCount]string{
	personaTypeUnknownStr,
	personaTypeUserStr,
	personaTypeGroupStr,
	personaTypeWellKnownStr,
}

// ParsePersonaType parses a PersonaType from a string.
func ParsePersonaType(text string) PersonaType {
	switch {
	case strings.EqualFold(text, personaTypeUserStr):
		return PersonaTypeUser
	case strings.EqualFold(text, personaTypeGroupStr):
		return PersonaTypeGroup
	case strings.EqualFold(text, personaTypeWellKnownStr):
		return PersonaTypeWellKnown
	}
	return PersonaTypeUnknown
}

// String returns the string representation of a PersonaType value.
func (p PersonaType) String() string {
	if p < (PersonaTypeUnknown+1) || p >= personaTypeCount {
		return personaTypesToStrs[PersonaTypeUnknown]
	}
	return personaTypesToStrs[p]
}

// MarshalJSON marshals a PersonaType value to JSON.
func (p PersonaType) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

// UnmarshalJSON marshals a PersonaType value from JSON.
func (p *PersonaType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*p = ParsePersonaType(s)
	return nil
}

// GetIsiQuota queries the quota for a directory
func (papi *PapiConnection) GetIsiQuota(path string) (quota *IsiQuota, err error) {
	// PAPI call: GET https://1.2.3.4:8080/platform/1/quota/quotas
	// This will list out all quotas on the cluster

	var quotaResp isiQuotaListResp
	err = papi.query("GET", QuotaPath, "", nil, nil, &quotaResp)
	if err != nil {
		return nil, err
	}

	// find the specific quota we are looking for
	for _, quota := range quotaResp.Quotas {
		if quota.Path == path {
			return &quota, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("Quota not found: %s", path))
}

// TODO: Add a means to set/update more than just the hard threshold

// SetIsiQuotaHardThreshold sets the hard threshold of a quota for a directory
func (papi *PapiConnection) SetIsiQuotaHardThreshold(path string, size int64) (err error) {
	// PAPI call: POST https://1.2.3.4:8080/platform/1/quota/quotas
	//             { "enforced" : true,
	//               "include_snapshots" : false,
	//               "path" : "/ifs/volumes/volume_name",
	//               "thresholds_include_overhead" : false,
	//               "type" : "directory",
	//               "thresholds" : { "advisory" : null,
	//                                "hard" : 1234567890,
	//                                "soft" : null
	//                              }
	//             }
	var data = &IsiQuotaReq{
		Enforced:         true,
		IncludeSnapshots: false,
		Path:             path,
		ThresholdsIncludeOverhead: false,
		Type:       "directory",
		Thresholds: isiThresholdsReq{Advisory: nil, Hard: size, Soft: nil},
	}

	var quotaResp IsiQuota
	err = papi.query("POST", QuotaPath, "", nil, data, &quotaResp)
	return err
}

// UpdateIsiQuotaHardThreshold modifies the hard threshold of a quota for a directory
func (papi *PapiConnection) UpdateIsiQuotaHardThreshold(path string, size int64) (err error) {
	// PAPI call: PUT https://1.2.3.4:8080/platform/1/quota/quotas/Id
	//             { "enforced" : true,
	//               "thresholds_include_overhead" : false,
	//               "thresholds" : { "advisory" : null,
	//                                "hard" : 1234567890,
	//                                "soft" : null
	//                              }
	//             }
	var data = &IsiUpdateQuotaReq{
		Enforced:                  true,
		ThresholdsIncludeOverhead: false,
		Thresholds:                isiThresholdsReq{Advisory: nil, Hard: size, Soft: nil},
	}

	quota, err := papi.GetIsiQuota(path)
	if err != nil {
		return err
	}

	var quotaResp IsiQuota
	err = papi.query("PUT", QuotaPath, quota.Id, nil, data, &quotaResp)
	return err
}

// DeleteIsiQuota removes the quota for a directory
func (papi *PapiConnection) DeleteIsiQuota(path string) (err error) {
	// PAPI call: DELETE https://1.2.3.4:8080/platform/1/quota/quotas?path=/path/to/volume
	// This will remove a the quota on a volume

	var quotaResp isiQuotaListResp
	err = papi.query("DELETE", QuotaPath, "", map[string]string{"path": path}, nil, &quotaResp)

	return err
}

// GetIsiVolumes queries a list of all volumes on the cluster
func (papi *PapiConnection) GetIsiVolumes() (resp *getIsiVolumesResp, err error) {
	// PAPI call: GET https://1.2.3.4:8080/namespace/path/to/volumes/
	err = papi.query("GET", papi.nameSpacePath(), "", nil, nil, &resp)
	return resp, err
}

// CreateIsiVolume makes a new volume on the cluster
func (papi *PapiConnection) CreateIsiVolume(name string) (resp *getIsiVolumesResp, err error) {
	// PAPI calls: PUT https://1.2.3.4:8080/namespace/path/to/volumes/volume_name
	//             x-isi-ifs-target-type: container
	//             x-isi-ifs-access-control: public_read_write
	//
	//             PUT https://1.2.3.4:8080/namespace/path/to/volumes/volume_name?acl
	//             {authoritative: "acl",
	//              action: "update",
	//              owner: {name: "username", type: "user"},
	//              group: {name: "groupname", type: "group"}
	//             }

	headers := map[string]string{"x-isi-ifs-target-type": "container", "x-isi-ifs-access-control": "public_read_write"}
	var data = &AclRequest{
		"acl",
		"update",
		&Ownership{papi.username, "user"},
		nil,
	}
	if papi.group != "" {
		data.Group = &Ownership{papi.group, "group"}
	}

	// create the volume
	err = papi.queryWithHeaders("PUT", papi.nameSpacePath(), name, nil, headers, nil, &resp)
	if err != nil {
		return resp, err
	}

	// set the ownership of the volume
	err = papi.query("PUT", papi.nameSpacePath(), name, map[string]string{"acl": ""}, data, &resp)

	return resp, err
}

// GetIsiVolume queries the attributes of a volume on the cluster
func (papi *PapiConnection) GetIsiVolume(name string) (resp *getIsiVolumeAttributesResp, err error) {
	// PAPI call: GET https://1.2.3.4:8080/namespace/path/to/volume/?metadata
	err = papi.query("GET", papi.nameSpacePath(), name, map[string]string{"metadata": ""}, nil, &resp)
	return resp, err
}

// DeleteIsiVolume removes a volume from the cluster
func (papi *PapiConnection) DeleteIsiVolume(name string) (resp *getIsiVolumesResp, err error) {
	// PAPI call: DELETE https://1.2.3.4:8080/namespace/path/to/volumes/volume_name?recursive=true

	err = papi.queryWithHeaders("DELETE", papi.nameSpacePath(), name, map[string]string{"recursive": "true"}, nil, nil, &resp)
	return resp, err
}

// CopyIsiVolume creates a new volume on the cluster based on an existing volume
func (papi *PapiConnection) CopyIsiVolume(sourceName, destinationName string) (resp *getIsiVolumesResp, err error) {
	// PAPI calls: PUT https://1.2.3.4:8080/namespace/path/to/volumes/destination_volume_name
	//             x-isi-ifs-copy-source: /path/to/volumes/source_volume_name

	headers := map[string]string{"x-isi-ifs-copy-source": fmt.Sprintf("/%s/%s", papi.nameSpacePath(), sourceName)}

	// copy the volume
	err = papi.queryWithHeaders("PUT", papi.nameSpacePath(), destinationName, nil, headers, nil, &resp)
	return resp, err
}

// Export creates an NFS export for a volume with the provided name.
func (papi *PapiConnection) Export(path string) (int, error) {
	return papi.ExportCreate(&Export{Paths: &([]string{path})})
}

// ExportList GETs all exports.
func (papi *PapiConnection) ExportsList() ([]*Export, error) {
	// GET https://1.2.3.4:8080/platform/1/protocols/nfs/exports

	var resp ExportList

	if err := papi.queryWithHeaders(
		"GET",
		ExportsPath,
		"",
		nil,
		contentTypeJSONHeader,
		nil,
		&resp); err != nil {

		return nil, err
	}

	return resp, nil
}

// ExportInspect GETs an export.
func (papi *PapiConnection) ExportInspect(id int) (*Export, error) {
	// GET https://1.2.3.4:8080/platform/1/protocols/nfs/exports/{id}

	var resp ExportList

	if err := papi.queryWithHeaders(
		"GET",
		ExportsPath,
		strconv.Itoa(id),
		nil,
		contentTypeJSONHeader,
		nil,
		&resp); err != nil {

		return nil, err
	}

	if len(resp) == 0 {
		return nil, nil
	}

	return resp[0], nil
}

// ExportCreate POSTs an Export object to the Isilon server.
func (papi *PapiConnection) ExportCreate(export *Export) (int, error) {
	// POST https://1.2.3.4:8080/platform/1/protocols/nfs/exports
	// Content-Type: application/json
	// json.Marshal(export)

	if export.Paths != nil && len(*export.Paths) == 0 {
		return 0, errors.New("no path set")
	}

	var resp Export

	if err := papi.queryWithHeaders(
		"POST",
		ExportsPath,
		"",
		nil,
		contentTypeJSONHeader,
		export,
		&resp); err != nil {

		return 0, err
	}

	return resp.ID, nil
}

// ExportUpdate PUTs an Export object to the Isilon server.
func (papi *PapiConnection) ExportUpdate(export *Export) error {
	// PUT https://1.2.3.4:8080/platform/1/protocols/nfs/exports/{id}
	// Content-Type: application/json
	// json.Marshal(export)
	return papi.queryWithHeaders(
		"PUT",
		ExportsPath,
		strconv.Itoa(export.ID),
		nil,
		contentTypeJSONHeader,
		export,
		nil)
}

// ExportDELETE DELETEs an Export object on the Isilon server.
func (papi *PapiConnection) ExportDelete(id int) error {
	// DELETE https://1.2.3.4:8080/platform/1/protocols/nfs/exports/{id}
	return papi.queryWithHeaders(
		"DELETE",
		ExportsPath,
		strconv.Itoa(id),
		nil,
		nil,
		nil,
		nil)
}

// SetExportClients sets an Export's clients property.
func (papi *PapiConnection) SetExportClients(
	id int, addrs ...string) error {

	return papi.ExportUpdate(&Export{ID: id, Clients: &addrs})
}

// SetExportRootClients sets an Export's root_clients property.
func (papi *PapiConnection) SetExportRootClients(
	id int, addrs ...string) error {

	return papi.ExportUpdate(&Export{ID: id, RootClients: &addrs})
}

// Unexport deletes the NFS export.
func (papi *PapiConnection) Unexport(id int) error {
	return papi.ExportDelete(id)
}

func (papi *PapiConnection) nameSpacePath() string {
	return fmt.Sprintf("%s%s", NamespacePath, papi.VolumePath)
}

func (papi *PapiConnection) exportsPath() string {
	return fmt.Sprintf("%s%s", ExportsPath, papi.VolumePath)
}

func (papi *PapiConnection) volumeSnapshotPath(name string) string {
	// snapshots of /ifs are stored in /ifs/.snapshots/snapshot_name
	path_tokens := strings.SplitN(papi.nameSpacePath(), "/ifs/", 2)
	return fmt.Sprintf("%s/ifs/.snapshot/%s/%s", path_tokens[0], name, path_tokens[1])
}

// GetIsiExports queries a list of all exports on the cluster
func (papi *PapiConnection) GetIsiExports() (resp *getIsiExportsResp, err error) {
	// PAPI call: GET https://1.2.3.4:8080/platform/1/protocols/nfs/exports
	err = papi.query("GET", ExportsPath, "", nil, nil, &resp)

	return resp, err
}

// GetIsiSnapshots queries a list of all snapshots on the cluster
func (papi *PapiConnection) GetIsiSnapshots() (resp *getIsiSnapshotsResp, err error) {
	// PAPI call: GET https://1.2.3.4:8080/platform/1/snapshot/snapshots
	err = papi.query("GET", SnapshotsPath, "", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetIsiSnapshot queries an individual snapshot on the cluster
func (papi *PapiConnection) GetIsiSnapshot(id int64) (*IsiSnapshot, error) {
	// PAPI call: GET https://1.2.3.4:8080/platform/1/snapshot/snapshots/123
	snapshotUrl := fmt.Sprintf("%s/%d", SnapshotsPath, id)
	var resp *getIsiSnapshotsResp
	err := papi.query("GET", snapshotUrl, "", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	// PAPI returns the snapshot data in a JSON list with the same structure as
	// when querying all snapshots.  Since this is for a single Id, we just
	// want the first (and should be only) entry in the list.
	return resp.SnapshotList[0], nil
}

// CreateIsiSnapshot makes a new snapshot on the cluster
func (papi *PapiConnection) CreateIsiSnapshot(path, name string) (resp *IsiSnapshot, err error) {
	// PAPI call: POST https://1.2.3.4:8080/platform/1/snapshot/snapshots
	//            Content-Type: application/json
	//            {path: "/path/to/volume"
	//             name: "snapshot_name"  <--- optional
	//            }
	if path == "" {
		return nil, errors.New("no path set")
	}

	data := &SnapshotPath{Path: path}
	if name != "" {
		data.Name = name
	}
	headers := map[string]string{"Content-Type": "application/json"}

	err = papi.queryWithHeaders("POST", SnapshotsPath, "", nil, headers, data, &resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CopyIsiSnaphost copies all files/directories in a snapshot to a new directory
func (papi *PapiConnection) CopyIsiSnapshot(sourceSnapshotName, sourceVolume, destinationName string) (resp *IsiVolume, err error) {
	// PAPI calls: PUT https://1.2.3.4:8080/namespace/path/to/volumes/destination_volume_name
	//             x-isi-ifs-copy-source: /path/to/snapshot/volumes/source_volume_name

	headers := map[string]string{"x-isi-ifs-copy-source": fmt.Sprintf("/%s/%s/", papi.volumeSnapshotPath(sourceSnapshotName), sourceVolume)}

	// copy the volume
	err = papi.queryWithHeaders("PUT", papi.nameSpacePath(), destinationName, nil, headers, nil, &resp)

	return resp, err
}

// RemoveIsiSnapshot deletes a snapshot from the cluster
func (papi *PapiConnection) RemoveIsiSnapshot(id int64) error {
	// PAPI call: DELETE https://1.2.3.4:8080/platform/1/snapshot/snapshots/123
	snapshotUrl := fmt.Sprintf("%s/%d", SnapshotsPath, id)
	err := papi.query("DELETE", snapshotUrl, "", nil, nil, nil)

	return err
}
