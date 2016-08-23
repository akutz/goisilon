// Note: The bulk of this was taken from the goextremio package.
package apiv1

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type PapiConnection struct {
	endpoint   string
	insecure   bool
	username   string
	group      string
	password   string
	httpClient *http.Client
	VolumePath string
}

// Isi PAPI error JSON structs
type PapiError struct {
	Code    string `json:"code"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

type Error struct {
	StatusCode int
	Err        []PapiError `json:"errors"`
}

// Create a new HTTP connection
func New(endpoint string, insecure bool, username, group, password, volumePath string) (*PapiConnection, error) {
	if endpoint == "" || username == "" || password == "" {
		return nil, errors.New("Missing endpoint, username, or password")
	}

	if volumePath == "" {
		volumePath = VolumesPath
	} else if volumePath[0] == '/' {
		volumePath = fmt.Sprintf("%s%s", VolumesPath, volumePath)
	} else {
		volumePath = fmt.Sprintf("%s/%s", VolumesPath, volumePath)
	}

	var client *http.Client
	if insecure {
		client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: insecure,
				},
			},
		}
	} else {
		client = &http.Client{}
	}

	return &PapiConnection{endpoint, insecure, username, group, password, client, volumePath}, nil
}

func multimap(p map[string]string) url.Values {
	q := make(url.Values, len(p))
	for k, v := range p {
		q[k] = []string{v}
	}
	return q
}

// Extract the error string from a received error message
func (err *Error) Error() string {
	// I've only seen PAPI return a single error, but, technically, it can be a list
	return err.Err[0].Message
}

// Parse a PAPI error message sent by the cluster
func buildError(r *http.Response) error {
	jsonError := Error{}
	json.NewDecoder(r.Body).Decode(&jsonError)

	jsonError.StatusCode = r.StatusCode
	// I've only seen PAPI return a single error, but, technically, it can be a list
	if jsonError.Err[0].Message == "" {
		jsonError.Err[0].Message = r.Status
	}

	return &jsonError
}

// Send an HTTP query to the cluster
func (xms *PapiConnection) query(method string, path string, id string,
	params map[string]string, body interface{}, resp interface{}) error {

	return xms.queryWithHeaders(method, path, id, params, nil, body, resp)
}

// Send an HTTP query that includes headers to the cluster
func (xms *PapiConnection) queryWithHeaders(
	method, path, id string,
	params map[string]string,
	headers map[string]string,
	body interface{},
	resp interface{}) error {

	var (
		err error
		url string
		req *http.Request
		res *http.Response
	)

	// build the URI
	if id == "" {
		url = fmt.Sprintf("%s/%s", xms.endpoint, path)
	} else {
		url = fmt.Sprintf("%s/%s/%s", xms.endpoint, path, id)
	}

	// add parameters to the URI
	encodedParams := multimap(params).Encode()
	if encodedParams != "" {
		url = fmt.Sprintf("%s?%s", url, encodedParams)
	}

	// marshal the message body (assumes json format)
	if body != nil {
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		if err = enc.Encode(body); err != nil {
			return err
		}
		req, err = http.NewRequest(method, url, buf)
	} else {
		req, err = http.NewRequest(method, url, nil)
	}

	if err != nil {
		return err
	}

	// add headers to the request
	if len(headers) > 0 {
		for header, value := range headers {
			req.Header.Add(header, value)
		}
	}

	// set the username and password
	req.SetBasicAuth(xms.username, xms.password)

	logRequest(req)

	// send the request
	if res, err = xms.httpClient.Do(req); err != nil {
		return err
	}
	defer res.Body.Close()

	logResponse(res)

	// parse the response
	switch {
	case res == nil:
		return nil
	case res.StatusCode >= 200 && res.StatusCode <= 299:
		dec := json.NewDecoder(res.Body)
		if err = dec.Decode(resp); err != nil && err != io.EOF {
			return err
		}
	default:
		return buildError(res)
	}

	return nil
}
