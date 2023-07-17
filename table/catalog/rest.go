// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package catalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/zeroshade/icegopher"
	"github.com/zeroshade/icegopher/io"
	"github.com/zeroshade/icegopher/table"

	"golang.org/x/exp/maps"
)

const (
	authorizationHeader    = "Authorization"
	bearerPrefix           = "Bearer"
	icebergRestSpecVersion = "0.14.1"

	namespaceSeparator = "\x1F"
	prefixKey          = "prefix"
)

var (
	ErrBadRequest           = fmt.Errorf("%w: bad request", ErrRESTError)
	ErrForbidden            = fmt.Errorf("%w: forbidden", ErrRESTError)
	ErrUnauthorized         = fmt.Errorf("%w: unauthorized", ErrRESTError)
	ErrAuthorizationExpired = fmt.Errorf("%w: authorization expired", ErrRESTError)
	ErrServiceUnavailble    = fmt.Errorf("%w: service unavailable", ErrRESTError)
	ErrServerError          = fmt.Errorf("%w: server error", ErrRESTError)
	ErrCommitFailed         = fmt.Errorf("%w: commit failed, refresh and try again", ErrRESTError)
	ErrCommitStateUnknown   = fmt.Errorf("%w: commit failed due to unknown reason", ErrRESTError)
	ErrOAuthError           = fmt.Errorf("%w: oauth error", ErrRESTError)
)

type errorResponse struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`

	wrapping error
}

func (e errorResponse) Unwrap() error { return e.wrapping }

func (e errorResponse) Error() string {
	return e.Type + ": " + e.Message
}

type oauthTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	RefreshToken string `json:"refresh_token"`
}

type oauthErrorResponse struct {
	Err     string `json:"error"`
	ErrDesc string `json:"error_description"`
	ErrURI  string `json:"error_uri"`
}

func (o oauthErrorResponse) Unwrap() error { return ErrOAuthError }
func (o oauthErrorResponse) Error() string {
	msg := o.Err
	if o.ErrDesc != "" {
		msg += ": " + o.ErrDesc
	}

	if o.ErrURI != "" {
		msg += " (" + o.ErrURI + ")"
	}
	return msg
}

type configResponse struct {
	Defaults  icegopher.Properties `json:"defaults"`
	Overrides icegopher.Properties `json:"overrides"`
}

type sessionTransport struct {
	http.RoundTripper

	defaultHeaders http.Header
}

func (s *sessionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	for k, v := range s.defaultHeaders {
		for _, hdr := range v {
			r.Header.Add(k, hdr)
		}
	}

	return s.RoundTripper.RoundTrip(r)
}

func doGet[T any](baseURI *url.URL, path []string, cl *http.Client, override map[int]error) (ret T, err error) {
	var rsp *http.Response

	uri := baseURI.JoinPath(path...).String()
	rsp, err = cl.Get(uri)
	if err != nil {
		return
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override)
	}

	defer rsp.Body.Close()
	if err = json.NewDecoder(rsp.Body).Decode(&ret); err != nil {
		return ret, fmt.Errorf("%w: error decoding json payload: `%s`", ErrRESTError, err.Error())
	}

	return
}

func doPost[Payload, Result any](baseURI *url.URL, path []string, payload Payload, cl *http.Client, override map[int]error) (ret Result, err error) {
	var (
		rsp  *http.Response
		data []byte
	)

	uri := baseURI.JoinPath(path...).String()
	data, err = json.Marshal(payload)
	if err != nil {
		return
	}

	rsp, err = cl.Post(uri, "application/json", bytes.NewReader(data))
	if err != nil {
		return
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override)
	}

	if rsp.ContentLength == 0 {
		return
	}

	defer rsp.Body.Close()
	if err = json.NewDecoder(rsp.Body).Decode(&ret); err != nil {
		return ret, fmt.Errorf("%w: error decoding json payload: `%s`", ErrRESTError, err.Error())
	}

	return
}

type rest struct {
	catalog

	baseURI *url.URL
	cl      http.Client
}

func NewRestCatalog(name, uri string, props icegopher.Properties) (table.Catalog, error) {
	baseuri, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	r := &rest{
		catalog: catalog{name: name, props: props},
		baseURI: baseuri.JoinPath("v1"),
	}

	if err = r.fetchConfig(); err != nil {
		return nil, err
	}

	if err = r.createSession(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *rest) fetchAccessToken(creds string) (string, error) {
	clientID, clientSecret, hasID := strings.Cut(creds, ":")
	if !hasID {
		clientID, clientSecret = "", clientID
	}

	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"scope":         {"catalog"},
	}

	rsp, err := r.cl.PostForm(r.baseURI.JoinPath("oauth/tokens").String(), data)
	if err != nil {
		return "", err
	}

	if rsp.StatusCode == http.StatusOK {
		defer rsp.Body.Close()
		dec := json.NewDecoder(rsp.Body)
		var tok oauthTokenResponse
		if err := dec.Decode(&tok); err != nil {
			return "", err
		}

		return tok.AccessToken, nil
	}

	switch rsp.StatusCode {
	case http.StatusUnauthorized, http.StatusBadRequest:
		defer rsp.Body.Close()
		dec := json.NewDecoder(rsp.Body)
		var oauthErr oauthErrorResponse
		if err := dec.Decode(&oauthErr); err != nil {
			return "", err
		}
		return "", oauthErr
	default:
		return "", handleNon200(rsp, nil)
	}
}

func (r *rest) createSession() error {
	session := &sessionTransport{
		RoundTripper:   http.DefaultTransport,
		defaultHeaders: http.Header{},
	}
	r.cl.Transport = session

	var err error
	// TODO: handle SSL config in props

	creds, hasCreds := r.props[KeyCredential]
	if _, hasToken := r.props[KeyToken]; !hasToken && hasCreds {
		if r.props[KeyToken], err = r.fetchAccessToken(creds); err != nil {
			return err
		}
	}

	if token, ok := r.props[KeyToken]; ok {
		session.defaultHeaders.Set(authorizationHeader, bearerPrefix+" "+token)
	}

	session.defaultHeaders.Set("X-Client-Version", icebergRestSpecVersion)
	session.defaultHeaders.Set("User-Agent", "github.com/zeroshade/icegopher/"+icegopher.Version)

	// TODO: add sig4 request signing
	return nil
}

func (r *rest) fetchConfig() error {
	params := url.Values{}
	if warehouse := r.props[KeyWarehouseLocation]; warehouse != "" {
		params.Set(KeyWarehouseLocation, warehouse)
	}

	route := r.baseURI.JoinPath("config")
	route.RawQuery = params.Encode()
	resp, err := r.cl.Get(route.String())
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return handleNon200(resp, nil)
	}

	defer resp.Body.Close()
	var cfg configResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&cfg); err != nil {
		return err
	}

	props := cfg.Defaults
	maps.Copy(props, r.props)
	maps.Copy(props, cfg.Overrides)
	r.props = props

	if uri, ok := props["uri"]; ok {
		r.baseURI, err = url.Parse(uri)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkValidNamespaceId(id ...string) (table.Identifier, error) {
	ident := ToIdentifier(id...)
	if len(ident) < 1 {
		return nil, fmt.Errorf("%w: empty namespace identifier '%v'", ErrNoSuchNamespace, id)
	}

	return ident, nil
}

func handleNon200(rsp *http.Response, override map[int]error) error {
	var e errorResponse

	dec := json.NewDecoder(rsp.Body)
	dec.Decode(&struct {
		Error *errorResponse `json:"error"`
	}{Error: &e})

	if override != nil {
		if err, ok := override[rsp.StatusCode]; ok {
			e.wrapping = err
			return e
		}
	}

	switch rsp.StatusCode {
	case http.StatusBadRequest:
		e.wrapping = ErrBadRequest
	case http.StatusUnauthorized:
		e.wrapping = ErrUnauthorized
	case http.StatusForbidden:
		e.wrapping = ErrForbidden
	case http.StatusUnprocessableEntity:
		e.wrapping = ErrRESTError
	case 419:
		e.wrapping = ErrAuthorizationExpired
	case http.StatusNotImplemented:
		e.wrapping = icegopher.ErrNotImplemented
	case http.StatusServiceUnavailable:
		e.wrapping = ErrServiceUnavailble
	default:
		if 500 <= rsp.StatusCode && rsp.StatusCode < 600 {
			e.wrapping = ErrServerError
		} else {
			e.wrapping = ErrRESTError
		}
	}

	return e
}

func splitIdentForPath(ident ...string) (string, string, error) {
	id := ToIdentifier(ident...)
	if len(ident) < 1 {
		return "", "", fmt.Errorf("%w: missing namespace or invalid identifier %v",
			ErrNoSuchTable, strings.Join(ident, "."))
	}

	return strings.Join(NamespaceFromIdent(id), namespaceSeparator), TableNameFromIdent(id), nil
}

type tblResponse struct {
	MetadataLoc string               `json:"metadata-location"`
	RawMetadata json.RawMessage      `json:"metadata"`
	Config      icegopher.Properties `json:"config"`
	Metadata    table.Metadata       `json:"-"`
}

func (t *tblResponse) UnmarshalJSON(b []byte) (err error) {
	type Alias tblResponse
	if err = json.Unmarshal(b, (*Alias)(t)); err != nil {
		return err
	}

	t.Metadata, err = table.ParseMetadataBytes(t.RawMetadata)
	return
}

func (r *rest) LoadTable(pathToTable ...string) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(pathToTable...)
	if err != nil {
		return nil, err
	}

	ret, err := doGet[tblResponse](r.baseURI, []string{r.props[prefixKey], "namespaces", ns, "tables", tbl},
		&r.cl, map[int]error{http.StatusNotFound: ErrNoSuchTable})
	if err != nil {
		return nil, err
	}

	id := ToIdentifier(pathToTable...)
	if r.name != "" {
		id = append([]string{r.name}, id...)
	}

	props := maps.Clone(r.props)
	maps.Copy(props, ret.Metadata.Properties())
	for k, v := range ret.Config {
		props[k] = v
	}

	iofs, err := io.LoadFS(props, ret.MetadataLoc)
	if err != nil {
		return nil, err
	}
	return table.New(id, ret.Metadata, ret.MetadataLoc, iofs, r), nil
}

func (r *rest) DropTable(_ ...string) error {
	panic("not implemented") // TODO: Implement
}

func (r *rest) RenameTable(from table.Identifier, to table.Identifier) (*table.Table, error) {
	panic("not implemented") // TODO: Implement
}

func (r *rest) CreateNamespace(props icegopher.Properties, namespace ...string) error {
	ident, err := checkValidNamespaceId(namespace...)
	if err != nil {
		return err
	}

	_, err = doPost[map[string]any, struct{}](r.baseURI, []string{r.props[prefixKey], "namespaces"},
		map[string]any{"namespace": ident, "properties": props}, &r.cl, map[int]error{
			http.StatusNotFound: ErrNoSuchNamespace, http.StatusConflict: ErrNamespaceAlreadyExists,
		})

	return err
}

func (r *rest) DropNamespace(namespace ...string) error {
	ident, err := checkValidNamespaceId(namespace...)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodDelete, r.baseURI.JoinPath("namespaces", strings.Join(ident, namespaceSeparator)).String(), nil)
	if err != nil {
		return err
	}

	rsp, err := r.cl.Do(req)
	if err != nil {
		return err
	}

	if rsp.StatusCode != http.StatusOK {
		return handleNon200(rsp, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})
	}

	return nil
}

func (r *rest) ListTables(namespace ...string) ([]table.Identifier, error) {
	ident, err := checkValidNamespaceId(namespace...)
	if err != nil {
		return nil, err
	}

	ns := strings.Join(ident, namespaceSeparator)
	path := make([]string, 0, 4)
	if p, ok := r.props[prefixKey]; ok {
		path = append(path, p)
	}
	path = append(path, "namespaces", ns, "tables")

	type resp struct {
		Identifiers []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
	}

	response, err := doGet[resp](r.baseURI, path, &r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})
	if err != nil {
		return nil, err
	}

	out := make([]table.Identifier, len(response.Identifiers))
	for i, r := range response.Identifiers {
		out[i] = append(r.Namespace, r.Name)
	}
	return out, nil
}

func (r *rest) ListNamespaces(namespace ...string) ([]table.Identifier, error) {
	ident := ToIdentifier(namespace...)

	prefix := r.props[prefixKey]
	uri := r.baseURI.JoinPath(prefix, "namespaces")
	if len(ident) != 0 {
		v := url.Values{}
		v.Set("parent", strings.Join(ident, namespaceSeparator))
		uri.RawQuery = v.Encode()
	}

	type rsptype struct {
		Namespaces []table.Identifier `json:"namespaces"`
	}

	response, err := doGet[rsptype](uri, []string{}, &r.cl, nil)
	if err != nil {
		return nil, err
	}

	out := make([]table.Identifier, len(response.Namespaces))
	for i, ns := range response.Namespaces {
		out[i] = append(ident, ns...)
	}
	return out, nil
}

func (r *rest) LoadNamespaceProperties(namespace ...string) (icegopher.Properties, error) {
	ident, err := checkValidNamespaceId(namespace...)
	if err != nil {
		return nil, err
	}

	type nsresponse struct {
		Namespace table.Identifier     `json:"namespace"`
		Props     icegopher.Properties `json:"properties"`
	}

	response, err := doGet[nsresponse](r.baseURI, []string{r.props[prefixKey], "namespaces", strings.Join(ident, namespaceSeparator)},
		&r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})

	if err != nil {
		return nil, err
	}

	return response.Props, nil
}

func (r *rest) UpdateNamespaceProperties(namespace table.Identifier, removals []string, updates icegopher.Properties) (table.PropertiesUpdateSummary, error) {
	ident, err := checkValidNamespaceId(namespace...)
	if err != nil {
		return table.PropertiesUpdateSummary{}, err
	}

	type payload struct {
		Remove  []string             `json:"removals"`
		Updates icegopher.Properties `json:"updates"`
	}

	ns := strings.Join(ident, namespaceSeparator)
	return doPost[payload, table.PropertiesUpdateSummary](r.baseURI, []string{r.props[prefixKey], "namespaces", ns, "properties"},
		payload{Remove: removals, Updates: updates}, &r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})
}
