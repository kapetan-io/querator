/*
Copyright 2024 Derrick J. Wippler

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package transport

type HealthStatus string

const (
	HealthStatusPass HealthStatus = "pass"
	HealthStatusWarn HealthStatus = "warn"
	HealthStatusFail HealthStatus = "fail"
)

type HealthResponse struct {
	Status      HealthStatus       `json:"status"`
	Version     string             `json:"version,omitempty"`
	ReleaseID   string             `json:"releaseId,omitempty"`
	Notes       []string           `json:"notes,omitempty"`
	Output      string             `json:"output,omitempty"`
	Checks      map[string][]Check `json:"checks,omitempty"`
	Links       map[string]string  `json:"links,omitempty"`
	ServiceID   string             `json:"serviceId,omitempty"`
	Description string             `json:"description,omitempty"`
}

type Check struct {
	ComponentID   string       `json:"componentId,omitempty"`
	ComponentType string       `json:"componentType,omitempty"`
	Status        HealthStatus `json:"status"`
	Time          string       `json:"time,omitempty"`
	Output        string       `json:"output,omitempty"`
}
