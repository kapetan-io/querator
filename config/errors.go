package config

import "fmt"

type ErrYAMLParse struct {
	Msg string
}

func (e ErrYAMLParse) Error() string {
	return fmt.Sprintf("yaml parse error: %s", e.Msg)
}

type ErrFileNotExist struct {
	Msg string
}

func (e ErrFileNotExist) Error() string {
	return fmt.Sprintf("file does not exist: %s", e.Msg)
}

type ErrUnsupportedBackendDriverType struct {
	driverType string
}

func (e ErrUnsupportedBackendDriverType) Error() string {
	return fmt.Sprintf("unsupported backend driver type: %s", e.driverType)
}
