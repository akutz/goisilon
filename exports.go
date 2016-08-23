package goisilon

import papi "github.com/emccode/goisilon/api/v1"

type ExportList []*papi.Export
type Export *papi.Export
type UserMapping *papi.UserMapping

// GetExports returns a list of all exports on the cluster
func (c *Client) GetExports() (ExportList, error) {
	return c.API.ExportsList()
}

// GetExportByID returns an export with the provided ID.
func (c *Client) GetExportByID(id int) (Export, error) {
	return c.API.ExportInspect(id)
}

// GetExportByName returns the first export with a path for the provided
// volume name.
func (c *Client) GetExportByName(name string) (Export, error) {
	exports, err := c.API.ExportsList()
	if err != nil {
		return nil, err
	}
	path := c.Path(name)
	for _, ex := range exports {
		for _, p := range *ex.Paths {
			if p == path {
				return ex, nil
			}
		}
	}
	return nil, nil
}

// Export the volume with a given name on the cluster
func (c *Client) Export(name string) (int, error) {
	ok, id, err := c.IsExported(name)
	if err != nil {
		return 0, err
	}
	if ok {
		return id, nil
	}
	return c.API.Export(c.Path(name))
}

// GetRootMapping returns the root mapping for an Export.
func (c *Client) GetRootMapping(name string) (UserMapping, error) {
	ex, err := c.GetExportByName(name)
	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, nil
	}
	return ex.MapRoot, nil
}

// GetRootMappingByID returns the root mapping for an Export.
func (c *Client) GetRootMappingByID(id int) (UserMapping, error) {
	ex, err := c.GetExportByID(id)
	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, nil
	}
	return ex.MapRoot, nil
}

// EnableRootMapping enables the root mapping for an Export.
func (c *Client) EnableRootMapping(name, user string) error {
	ex, err := c.GetExportByName(name)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}

	nex := &papi.Export{ID: ex.ID, MapRoot: ex.MapRoot}

	setUserMapping(
		nex,
		user,
		true,
		func(e Export) UserMapping { return e.MapRoot },
		func(e Export, m UserMapping) { e.MapRoot = m })

	return c.API.ExportUpdate(nex)
}

// EnableRootMappingByID enables the root mapping for an Export.
func (c *Client) EnableRootMappingByID(id int, user string) error {
	ex, err := c.GetExportByID(id)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}

	nex := &papi.Export{ID: ex.ID, MapRoot: ex.MapRoot}

	setUserMapping(
		nex,
		user,
		true,
		func(e Export) UserMapping { return e.MapRoot },
		func(e Export, m UserMapping) { e.MapRoot = m })

	return c.API.ExportUpdate(nex)
}

// DisableRootMapping disables the root mapping for an Export.
func (c *Client) DisableRootMapping(name string) error {
	ex, err := c.GetExportByName(name)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}

	nex := &papi.Export{ID: ex.ID, MapRoot: ex.MapRoot}

	setUserMapping(
		nex,
		"nobody",
		false,
		func(e Export) UserMapping { return e.MapRoot },
		func(e Export, m UserMapping) { e.MapRoot = m })

	return c.API.ExportUpdate(nex)
}

// DisableRootMappingbyID disables the root mapping for an Export.
func (c *Client) DisableRootMappingByID(id int) error {
	ex, err := c.GetExportByID(id)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}

	nex := &papi.Export{ID: ex.ID, MapRoot: ex.MapRoot}

	setUserMapping(
		nex,
		"nobody",
		false,
		func(e Export) UserMapping { return e.MapRoot },
		func(e Export, m UserMapping) { e.MapRoot = m })

	return c.API.ExportUpdate(nex)
}

func setUserMapping(
	ex Export,
	user string,
	enabled bool,
	getMapping func(Export) UserMapping,
	setMapping func(Export, UserMapping)) {

	m := getMapping(ex)
	if m == nil || m.User == nil {
		m = &papi.UserMapping{
			User: &papi.Persona{
				Name: &user,
			},
		}
		setMapping(ex, m)
		return
	}

	m.Enabled = &enabled
	m.User = &papi.Persona{
		ID: &papi.PersonaID{
			ID:   user,
			Type: papi.PersonaIDTypeUser,
		},
	}
}

// GetExportClients returns an Export's clients property.
func (c *Client) GetExportClients(name string) ([]string, error) {
	ex, err := c.GetExportByName(name)
	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, nil
	}
	if ex.Clients == nil {
		return nil, nil
	}
	return *ex.Clients, nil
}

// GetExportClientsByID returns an Export's clients property.
func (c *Client) GetExportClientsByID(id int) ([]string, error) {
	ex, err := c.GetExportByID(id)
	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, nil
	}
	if ex.Clients == nil {
		return nil, nil
	}
	return *ex.Clients, nil
}

// AddExportClients adds to the Export's clients property.
func (c *Client) AddExportClients(name string, clients ...string) error {
	ex, err := c.GetExportByName(name)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}
	addClients := ex.Clients
	if addClients == nil {
		addClients = &clients
	} else {
		*addClients = append(*addClients, clients...)
	}
	return c.API.ExportUpdate(&papi.Export{ID: ex.ID, Clients: addClients})
}

// AddExportClientsByID adds to the Export's clients property.
func (c *Client) AddExportClientsByID(id int, clients ...string) error {
	ex, err := c.GetExportByID(id)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}
	addClients := ex.Clients
	if addClients == nil {
		addClients = &clients
	} else {
		*addClients = append(*addClients, clients...)
	}
	return c.API.ExportUpdate(&papi.Export{ID: ex.ID, Clients: addClients})
}

// SetExportClients sets the Export's clients property.
func (c *Client) SetExportClients(name string, clients ...string) error {
	ok, id, err := c.IsExported(name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return c.API.ExportUpdate(&papi.Export{ID: id, Clients: &clients})
}

// SetExportClientsByID sets the Export's clients property.
func (c *Client) SetExportClientsByID(id int, clients ...string) error {
	return c.API.ExportUpdate(&papi.Export{ID: id, Clients: &clients})
}

// ClearExportClients sets the Export's clients property to nil.
func (c *Client) ClearExportClients(name string) error {
	return c.SetExportClients(name, []string{}...)
}

// ClearExportClientsByID sets the Export's clients property to nil.
func (c *Client) ClearExportClientsByID(id int) error {
	return c.SetExportClientsByID(id, []string{}...)
}

// GetExportRootClients returns an Export's root_clients property.
func (c *Client) GetExportRootClients(name string) ([]string, error) {
	ex, err := c.GetExportByName(name)
	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, nil
	}
	if ex.RootClients == nil {
		return nil, nil
	}
	return *ex.RootClients, nil
}

// GetExportRootClientsByID returns an Export's clients property.
func (c *Client) GetExportRootClientsByID(id int) ([]string, error) {
	ex, err := c.GetExportByID(id)
	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, nil
	}
	if ex.RootClients == nil {
		return nil, nil
	}
	return *ex.RootClients, nil
}

// AddExportRootClients adds to the Export's root_clients property.
func (c *Client) AddExportRootClients(name string, clients ...string) error {
	ex, err := c.GetExportByName(name)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}
	addClients := ex.RootClients
	if addClients == nil {
		addClients = &clients
	} else {
		*addClients = append(*addClients, clients...)
	}
	return c.API.ExportUpdate(&papi.Export{ID: ex.ID, RootClients: addClients})
}

// AddExportRootClientsByID adds to the Export's root_clients property.
func (c *Client) AddExportRootClientsByID(id int, clients ...string) error {
	ex, err := c.GetExportByID(id)
	if err != nil {
		return err
	}
	if ex == nil {
		return nil
	}
	addClients := ex.RootClients
	if addClients == nil {
		addClients = &clients
	} else {
		*addClients = append(*addClients, clients...)
	}
	return c.API.ExportUpdate(&papi.Export{ID: ex.ID, RootClients: addClients})
}

// SetExportRootClients sets the Export's root_clients property.
func (c *Client) SetExportRootClients(name string, clients ...string) error {
	ok, id, err := c.IsExported(name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return c.API.ExportUpdate(&papi.Export{ID: id, RootClients: &clients})
}

// SetExportRootClientsByID sets the Export's clients property.
func (c *Client) SetExportRootClientsByID(id int, clients ...string) error {
	return c.API.ExportUpdate(&papi.Export{ID: id, RootClients: &clients})
}

// ClearExportRootClients sets the Export's root_clients property to nil.
func (c *Client) ClearExportRootClients(name string) error {
	return c.SetExportRootClients(name, []string{}...)
}

// ClearExportRootClientsByID sets the Export's clients property to nil.
func (c *Client) ClearExportRootClientsByID(id int) error {
	return c.SetExportRootClientsByID(id, []string{}...)
}

// Stop exporting a given volume from the cluster
func (c *Client) Unexport(name string) error {
	ok, id, err := c.IsExported(name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return c.API.Unexport(id)
}

// UnexportByID unexports an Export by its ID.
func (c *Client) UnexportByID(id int) error {
	return c.API.Unexport(id)
}

// IsExported returns a flag and export ID if the provided volume name is
// already exported.
func (c *Client) IsExported(name string) (bool, int, error) {
	export, err := c.GetExportByName(name)
	if err != nil {
		return false, 0, err
	}
	if export == nil {
		return false, 0, nil
	}
	return true, export.ID, nil
}
